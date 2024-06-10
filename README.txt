# Author: Octavio E. Lima
# Date: 06-Jun-2024

---

# What the Code Does:
— RUN PYTHON SCRIPT 
        — 1. API call (Polygon.io)
        — 2. API call (FRED)
        — 3. Clean data
        — 4. Merge
        — 5. AWS-S3 Connection + Upload
        — 6. AWS-Athena Connection 
        — 7. Create Athena DB
— RUN DBT MODEL
    — 8. Create view of original DB
    — 9. Normalize the data
    — 10. Calculate additional metrics
    — 11. Group-by Statements
    — 12. Sanity Checks / Viz

---

# How to Run:
1- `cd ./globalX/`
2- `sh run_etl.sh`, which will run `global_x_task.py`, followed by the `dbt` models

---

# Structure:
.
├── README.md
├── config.ini
├── dbt_project
│   ├── analyses
│   ├── dbt_packages
│   ├── dbt_project.yml
│   ├── logs
│   │   └── dbt.log
│   ├── macros
│   │   └── drop_view.sql
│   ├── models
│   │   ├── etl_integrated_etf_data.sql
│   │   ├── extra_metrics.sql
│   │   ├── fred_unemp.sql
│   │   ├── group_by_date.sql
│   │   ├── group_by_stock.sql
│   │   ├── polygon.sql
│   │   └── source.yml
│   ├── seeds
│   ├── snapshots
│   ├── target
│   │   ├── compiled
│   │   │   └── global_x_task
│   │   │       └── models
│   │   │           ├── etl_integrated_etf_data.sql
│   │   │           ├── extra_metrics.sql
│   │   │           ├── fred_unemp.sql
│   │   │           ├── group_by_date.sql
│   │   │           ├── group_by_stock.sql
│   │   │           ├── integrated_etf_data.sql
│   │   │           └── polygon.sql
│   │   ├── graph.gpickle
│   │   ├── manifest.json
│   │   ├── partial_parse.msgpack
│   │   ├── run
│   │   │   └── global_x_task
│   │   │       └── models
│   │   │           ├── etl_integrated_etf_data.sql
│   │   │           ├── extra_metrics.sql
│   │   │           ├── fred_unemp.sql
│   │   │           ├── group_by_date.sql
│   │   │           ├── group_by_stock.sql
│   │   │           ├── integrated_etf_data.sql
│   │   │           └── polygon.sql
│   │   └── run_results.json
│   └── tests
├── extra_metrics_viz.py
├── global_x_task.py
├── requirements.txt
└── run_etl.sh