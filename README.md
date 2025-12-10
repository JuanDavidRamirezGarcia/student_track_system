# student_track_system

This project aims to create an ETL (Extract, Transform, Load) workflow to track student attendance and prevent absences. 
I utilize the Medallion Architecture (Bronze, Silver, and Gold layers) to process raw attendance data from a Google spreadsheet to a visualization dashboard on Databricks.

## Architecture

The automated workflow is orchestrated for a job on Databricks that acts as a container for the following tasks:

1. Set up the infrastructure: `00_setup_infrastructure`. Create the Catalog `student_system` and the schema `attendance` to set the path to create the tables that will store the data.
2. Ingest the data: `01_ingest_attendance_bronze`. Ingested the raw data from the spreadsheet source into the table `raw_attendance.`
3. Clean data: `02_clean_attendance_silver`. Performs data cleaning and transformation, data type inconsistencies, and missing data.
4. Keymetrics: `03_keymetrics_gold`. Aggregate data to calculate key metrics such as attendance rate per student, and label students at risk of absences.
5. Visualization: `attendance_dashboard.` Visualize and track attendance per student to identify the students who are at risk of dropping off.

<img width="524" height="251" alt="job" src="https://github.com/user-attachments/assets/8d9fc8f7-2ac7-4df5-9526-a07e8c15e7b5" />
<img width="416" height="289" alt="image" src="https://github.com/user-attachments/assets/ba150199-410c-4f0d-881a-18c3c8771692" />

## Repository Structure

** Source Code (`/src`)**
* `00_setup_infrastructure.py` - Environment setup
* `01_ingest_attendance_bronze.py` - Raw data ingestion
* `02_clean_attendance_silver.py` - Data quality & transformation
* `03_keymetrics_gold.py` - Business logic aggregation
* `04_query_metric_attendance.py` - Reporting queries

** Configuration (`/config`)**
* `job_settings.yaml` - Job orchestration settings

** Documentation (`/docs`)**
* `job_flow.png` - Pipeline visualization
* `dashboard_preview.pdf` - Dashboard export

## Tools

* Platform: Databricks
* Language: Python (PySpark) / SQL
* Storage: Delta Lake
* Orchestration: Databricks Jobs

## Dashboard

<img width="394" height="353" alt="image" src="https://github.com/user-attachments/assets/7103c1c4-84b1-4604-be67-68a36cf2b1d7" />
