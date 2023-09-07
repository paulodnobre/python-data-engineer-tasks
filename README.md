# Python Data Engineer Tasks

This repo contains some everyday tasks in a Python Data Engineer's day job.

## Task Descriptions

### [Task 1: Ingest Purchase Order data](./Task%201/) (Basic): 
In this task, you will learn how to ingest Purchase Order data from a JSON file into a database table. You will practice data extraction, transformation, and loading (ETL) by ensuring that the data is formatted correctly, following the pattern: **PartNumber:Qty:Price | PartNumber2:Qty:Price** if more than one. This task is designed to build your foundational skills in data engineering.
### [Task 2: Hiring Data Ingestion and Analysis](./Task%202/) (Intermediate): 
In this task, you will work with CSV data to ingest and analyze hiring information. You will save the data into a database table and create chart visualizations from the database. This task provides an opportunity to practice more advanced ETL techniques and data visualization.
### Task 3: NYC Yellow Taxy Trip Data Ingestion and Analysis (Advanced): 
This advanced task involves building a robust data pipeline using Apache Spark and Apache Airflow. You will ingest data from multiple parquet files, orchestrated by an Airflow DAG. Additionally, you will create chart visualizations to analyze the NYC Yellow Taxi trip data. This task is ideal for those looking to expand their skills in big data processing and workflow orchestration.

## Additional Resources

If you're new to any of the concepts or tools used in these tasks, you may find these resources helpful:
  - [Spark Documentation](https://spark.apache.org/docs/latest/)
  - [Airflow Documentation](https://airflow.apache.org/docs/stable/index.html)

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

## Contributing

We welcome contributions! If you have additional tasks or improvements to existing ones, please submit a pull request.

## License

This repository is licensed under the [MIT License](LICENSE).