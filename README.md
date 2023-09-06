# Python Data Engineer Tasks

This repo contains some everyday tasks in a Python Data Engineer's day job.

The idea is to demonstrate how everyday tasks are performed by the different levels of data engineering and a little bit about the tasks expected from each level.

Below is a summary of each task:

- [Task 1: Ingest a JSON file into a Database](./Task%201/) (Junior)
- Task 2: Ingest a CSV file and Create Visualizations (Mid-level)
- Task 3: Data pipeline with Airflow and Spark (Sênior)

## Prerequisites

Ensure you have a PostgreSQL instance running locally. If you don't have it installed, you can use Docker to quickly set up a PostgreSQL container with the following commands:

```bash
$ docker run -d --name postgres-db -e POSTGRES_DB=postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -p 15432:5432 postgres:15
```

The project notebooks utilize some Python libraries, such as `pandas`, `psycopg2`, and `matplotlib`. You can install these libraries using pip, for example:

```bash
$ pip install pandas psycopg2 matplotlib
```

If you prefer a clean environment, consider creating a Python Virtual Environment to isolate the project dependencies:

```bash
$ python -m venv myenv
$ source myenv/bin/activate # Linux/Mac
$ .\myenv\Scripts\activate #Windows
```

With these prerequisites in place, you should good to go in replicating the notebooks.