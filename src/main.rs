use std::process::Stdio;

use prqlc::{compile, sql::Dialect, Options, Target};

use anyhow::{anyhow, bail, Context, Result};

use axum::{
    extract::Query,
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};

use serde::Deserialize;

use arrow::array::Array;
use duckdb::Connection;
use tempfile::TempDir;
use tokio::{fs, io, process::Command};

mod ansi;
mod arrow_utils;
mod escape;

use ansi::strip_ansi;
use arrow_utils::value_string;
use escape::{escape_html, escape_query};

const SCHEMA_HELP: &'static str = r#"
<details style="margin-left: 8%; margin-top: 10px">
<summary>Schema</summary>
<p>These are the most useful columns.</p>
<dl>
    <dt>account</dt>
    <dd>e.g. "aspall/formal"</dd>
    <dt>command</dt>
    <dd>e.g. "bash"</dd>
    <dt>eligible_time</dt>
    <dd>e.g. 1707412088</dd>
    <dt>end_time</dt>
    <dd>e.g. 1738948088</dd>
    <dt>exit_code</dt>
    <dd>e.g. 0</dd>
    <dt>job_id</dt>
    <dd>e.g. 10700559</dd>
    <dt>job_state</dt>
    <dd>e.g. "RUNNING"</dd>
    <dt>licenses</dt>
    <dd>e.g. ""</dd>
    <dt>max_cpus</dt>
    <dd>e.g. 0</dd>
    <dt>max_nodes</dt>
    <dd>e.g. 0</dd>
    <dt>name</dt>
    <dd>e.g. "qfm_aspall"</dd>
    <dt>nodes</dt>
    <dd>e.g. "slurm3.example.com"</dd>
    <dt>tasks_per_node</dt>
    <dd>e.g. 0</dd>
    <dt>cpus</dt>
    <dd>e.g. 1</dd>
    <dt>node_count</dt>
    <dd>e.g. 1</dd>
    <dt>tasks</dt>
    <dd>e.g. 1</dd>
    <dt>partition</dt>
    <dd>e.g. "all"</dd>
    <dt>memory_per_node</dt>
    <dd>In MB, e.g. 4096</dd>
    <dt>qos</dt>
    <dd>Quality of Service, e.g. "normal"</dd>
    <dt>start_time</dt>
    <dd>e.g. 1707412088</dd>
    <dt>standard_error</dt>
    <dd>Path to file containing stderr, e.g. ""</dd>
    <dt>standard_input</dt>
    <dd>Path to file containing stdin, e.g. ""</dd>
    <dt>standard_output</dt>
    <dd>Path to file containing stdout, e.g. ""</dd>
    <dt>submit_time</dt>
    <dd>e.g. 1707412087</dd>
    <dt>time_limit</dt>
    <dd>e.g. null</dd>
    <dt>user_id</dt>
    <dd>e.g. 529202496</dd>
    <dt>user_name</dt>
    <dd>e.g. "tim.hutt"</dd>
    <dt>current_working_directory</dt>
    <dd>e.g. "/opt/work/foo</dd>
</dl>
</details>
"#;

const EXAMPLES: &[(&'static str, &'static str)] = &[
    ("All data", r#"
from queue
"#),
    ("Aggregate CPU and memory use by user and job state", r#"
from queue
derive {
  mem_gb = node_count * memory_per_node / 1024,
}
group {user_name, job_state} (
  aggregate {
    total_cpus = sum cpus,
    total_mem_gb = sum mem_gb,
  }
)
sort { -total_mem_gb }
"#),
    ("Number of jobs by user", r#"
from queue
group user_name (
  aggregate {
    num_jobs = count user_name,
  }
)
sort (-num_jobs)
"#),
    ("Number of jobs by user/account", r#"
from queue
group {user_name, account} (
  aggregate {
    num_jobs = count 1,
  }
)
sort (-num_jobs)
"#),
    ("Number of jobs by user with no account", r#"
from queue
filter account == "none"
group user_name (
  aggregate {
    num_jobs = count 1,
  }
)
sort (-num_jobs)
"#),
];

/// Run `squeue --json` and write it to a file called 'squeue.json' in a
/// temporary directory which is returend. We have to use a temporary directory
/// so it is possible to close the file without deleting it. That's necessary
/// on Windows otherwise DuckDB won't read it due to file locking.
async fn squeue_json() -> Result<TempDir> {
    let mut child = Command::new("squeue")
        .arg("--json")
        .stdout(Stdio::piped())
        .spawn()?;

    let stdout = child
        .stdout
        .take()
        .expect("stdout must exist because we said it should be piped");

    let dir = TempDir::new()?;

    let mut file = fs::File::create(dir.path().join("squeue.json")).await?;

    io::copy(&mut io::BufReader::new(stdout), &mut file).await?;

    // Wait for the command to finish executing
    let status = child.wait().await?;
    if !status.success() {
        bail!("command failed");
    }

    Ok(dir)
}

async fn query(prql: &str) -> Result<String> {
    // Compile PRQL to SQL
    let opts = &Options {
        format: true,
        target: Target::Sql(Some(Dialect::DuckDb)),
        signature_comment: false,
        // This does nothing, it actually always returns ANSI colours.
        color: false,
    };
    let sql = compile(&prql, opts).map_err(|e| anyhow!("{}", strip_ansi(&e.to_string())))?;

    dbg!(&sql);

    let json_dir = squeue_json().await?;
    let json_path = json_dir.path().join("squeue.json");

    // Import JSON into DuckDB
    let conn = Connection::open_in_memory()?;

    conn.execute(
        "CREATE TABLE queue AS SELECT * FROM read_json_auto(?)",
        [json_path.to_string_lossy()],
    )
    .with_context(|| anyhow!("Reading JSON"))?;

    // Security, hopefully.
    conn.execute(
        "SET disabled_filesystems='LocalFileSystem,HTTPFileSystem'",
        [],
    )?;
    conn.execute("SET lock_configuration=true", [])?;

    // Run Query
    let mut stmt = conn.prepare(&sql)?;

    stmt.execute([])?;

    let mut table_html = "<table id=\"results\">".to_string();

    let mut header_printed = false;
    while let Some(batch) = stmt.step() {
        if !header_printed {
            header_printed = true;
            table_html += "<thead><tr>";
            for col in batch.column_names() {
                table_html += "<th>";
                table_html += &escape_html(&col);
                table_html += "</th>";
            }
            table_html += "</tr></thead><tbody>";
        }

        for row in 0..batch.len() {
            table_html += "<tr>";
            for col in batch.columns() {
                table_html += "<td>";
                table_html += &escape_html(&value_string(col, row));
                table_html += "</td>";
            }
            table_html += "</tr>";
        }
    }

    table_html += "</tbody></table>";

    Ok(table_html)
}

#[tokio::main]
async fn main() {
    let app = Router::new().route("/", get(index));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    println!("listening on http://{}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Params {
    prql: Option<String>,
}

async fn index(Query(params): Query<Params>) -> std::result::Result<Html<String>, AppError> {
    let escaped_prql = match &params.prql {
        Some(q) => escape_html(q),
        None => "".to_string(),
    };

    let result_html = match &params.prql {
        Some(q) => query(q).await?,
        None => {
            let mut examples = "<ul style=\"margin-left: 5%\">".to_string();
            for (name, example) in EXAMPLES {
                examples += &format!(
                    "<li><a href=\"/?prql={}\">{}</a></li>",
                    escape_query(example),
                    escape_html(name),
                );
            }
            examples += "</ul>";
            examples
        }
    };

    Ok(Html(format!(
        r#"
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">

<title>SLURM Query</title>

<link href="https://cdn.jsdelivr.net/npm/simple-datatables@latest/dist/style.css" rel="stylesheet" type="text/css">
<script src="https://cdn.jsdelivr.net/npm/simple-datatables@latest" type="text/javascript"></script>

</head>
<body>

    <form id="query_form" "action="/" method="GET" style="text-align: center">
        <textarea id="query_text" name="prql" rows="15" spellcheck="false" placeholder="from queue" style="width: 80%; vertical-align: bottom; height: 195px;">{escaped_prql}</textarea>
        <input style="height: 200px;" type="submit" value="Submit">
    </form>

    {SCHEMA_HELP}

    {result_html}

</div>

<script>
document.getElementById("query_text").addEventListener("keydown", event => {{
  if (event.ctrlKey && event.keyCode === 13) {{
    // Prevent adding a newline to the form.
    event.preventDefault();
    document.getElementById("query_form").submit()
  }}
}});
</script>

<script>
const dataTable = new simpleDatatables.DataTable('#results', {{  perPageSelect: false, searchable: false }});
</script>

</body>
</html>
"#
    )))
}

// Make our own error that wraps `anyhow::Error`.
struct AppError(anyhow::Error);

// Tell axum how to convert `AppError` into a response.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", self.0)).into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
