# Big Data Analytics with Apache Spark

This repository contains a project designed to explore Spark RDDs, Spark SQL, and Spark DataFrames as part of an Intro to Big Data Analytics coursework. The project demonstrates distributed data processing using Docker and Apache Spark on a dataset downloaded from Kaggle.

## Table of Contents
- [Project Overview](#project-overview)
- [Requirements](#requirements)
- [Setup Instructions](#setup-instructions)
  - [Using Docker](#using-docker)
  - [Downloading the Dataset](#downloading-the-dataset)
- [Running the Project](#running-the-project)
  - [Executing Tasks](#executing-tasks)
  - [Benchmarking Performance](#benchmarking-performance)
- [Project Structure](#project-structure)
- [Acknowledgments](#acknowledgments)

## Project Overview
The project consists of three tasks:
1. **Spark RDDs**: Perform map, filter, reduce, count, and groupBy operations on a dataset.
2. **Spark SQL**: Use SQL queries for aggregations, filtering, and sorting.
3. **Spark DataFrames**: Perform select, groupBy, and orderBy operations using DataFrames.

Benchmarking is included to compare Spark's performance with other methods like Pandas.

## Requirements
- Docker
- Kaggle API credentials (`kaggle.json`)
- Python dependencies specified in `requirements.txt`

## Setup Instructions

### Using Docker
1. Build the Docker container:
   ```bash
   docker build -t spark-docker .
   ```
2. Run the Docker container:
   ```bash
   docker run -it --name spark-container -v $(pwd):/app spark-docker
   ```

### Downloading the Dataset
1. Place your Kaggle API credentials (`kaggle.json`) in the root of the project.
2. Run the dataset download script:
   ```bash
   sh download_dataset.sh
   ```

## Running the Project

### Executing Tasks
1. Copy the `tasks` folder to the container:
   ```bash
   docker cp tasks spark-container:/app/tasks
   ```
2. Execute the tasks:
   ```bash
   docker exec -it spark-container python tasks/task1_rdds.py
   docker exec -it spark-container python tasks/task2_sql.py
   docker exec -it spark-container python tasks/task3_dataframes.py
   ```

### Benchmarking Performance
1. Copy the `benchmarking` folder to the container:
   ```bash
   docker cp benchmarking spark-container:/app/benchmarking
   ```
2. Execute benchmarking scripts:
   ```bash
   docker exec -it spark-container python benchmarking/rdds_vs_pandas.py
   docker exec -it spark-container python benchmarking/sql_vs_pandas.py
   ```

## Project Structure
```plaintext
.
├── download_dataset.sh   # Script to download the dataset
├── tasks/                # Contains scripts for Spark RDDs, SQL, and DataFrames
├── benchmarking/         # Contains benchmarking scripts
├── requirements.txt      # Python dependencies
├── Dockerfile            # Docker configuration
└── README.md             # This README file
```

## Acknowledgments
This project was completed as part of the Intro to Big Data Analytics coursework. Special thanks to the open-source contributors of Apache Spark and the Kaggle community.