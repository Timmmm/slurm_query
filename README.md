# Slurm Query

Simple web interface to query the status of a Slurm cluster. It works by running `squeue --json`, then loading the result into a temporary DuckDB database, and then executing the given [PRQL](https://prql-lang.org/) query on the database.

Some examples are included.
