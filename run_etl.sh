#!/bin/bash

# install libraries
pip install -r requirements.txt

# Run Python script 1
python3 ./global_x_task.py

# Navigate to the dbt project directory
cd ./dbt_project

# Run dbt
dbt run

# Run Python script 2
python3 ../extra_metrics_viz.py