# Age of Empires 2 Project

## 1. Overview

This is my 2nd version of the Age of Empires 2 (aoe2) project (see first version HERE).
This version looks to use a different technology stack (Databricks on Azure) and I have updated the dashboard to be built and hosted on Streamlit Cloud (link HERE to view).
The dashboard provides a comprehensive summary view of player and match statistics from the video game 'Age of Empires 2 DE'.  Data is automatically pulled and refreshed weekly in a Streamlit, enabling in-depth analysis and data slicing/dicing.  The dashboard aims to answer questions such as:

* Who are the top performing players currently?
* What civilization should I counter-pick to maximize my chances of beating my opponent?
* How has my favorite civilization performed over time?

## 2. Features

* Automated weekly data extraction, loading, and transformation.
* Pipeline incorporates numerous tests to ensure high data quality.
* User-friendly dashboards for data and insights visualization that anyone can view.

## 3. Project Structure

### High level view

<img src="./README_resources/aoe2project_data_pipeline.PNG" alt="Data Pipeline" width="1200"/>

### Pipeline DAG - project all

<img src="./README_resources/project_all_dag.PNG" alt="Airflow project DAG" width="1200"/>

### Pipeline DAG - dbt models

<img src="./README_resources/dbt_dag_airflow.PNG" alt="Airflow dbt DAG" width="1200"/>

### Dimensional Model

<img src="./README_resources/db_diagram_aoe_model.PNG" alt="Dimensional Model" width="1200"/>

## 4. Dashboard Example

### Leaderboard

<img src="./README_resources/aoe_leaderboard.PNG" alt="Player Leaderboard" width="1200"/>

### Counter-Civ picker

<img src="./README_resources/aoe_counter_pick.PNG" alt="Civ Counter Picker" width="1200"/>

### Civ Performance over time

<img src="./README_resources/aoe_civ_performance.PNG" alt="Civ Performance" width="1200"/>

## 5. Tools

* **a) Python (Data Extract & Load)**
    * Custom-built modules (API data extraction)
    * Pydantic (schema validation)
    * Pytest (unit testing)
    * Logging & API retries (error handling)
* **b) Azure Databricks with Unity Catalog (Data Warehouse)**
* **c) Databricks Jobs (Data Orchestration)**
    * Jobs triggered via Databricks SDK (DAGs, orchestration)
* **d) Databricks Pyspark (Data Transformation)**
    * Transformaiton defined in .py files (pyspark)
    * Custom-built DQ rules applied (data quality)
* **e) Git/Github Actions (Version Control)**
    * CI/CD pipeline (linting, testing, deployment)
    * Dev & Prod environments (software development)
* **f) Streamlit (Data Visualization)**
* **g) Other**
    * ADLS2 Containers (data storage)
    * Native coding environment (Local IDE compatable)
    * Medallion architecture (logical data modeling)
    * Star Schema (dimensional data modeling)
    * `.env` & `.yaml` files (Infrastructure/Configuration as Code)
    * `README.md` files & Docstrings (documentation)
    * `requirements.txt` & `setup.py` (package management)

## 6. Project Methodology & Technical Details

### Data Extraction and Load

The data pipeline uses the ELT framework, extracting and loading data "as-is*" from APIs into an ADLS2 Container.  Data is sourced from two APIs:

1. **Aoestats.io API (`https://aoestats.io`)**
   This API provides historical player and match data.  Two endpoints are used: one for dataset metadata (JSON) and another for direct download of match and player data (Parquet).  Custom Python functions generate API endpoint strings, query the API, validate schemas using Pydantic, and load data into ADLS.

2. **Relic-link API (now WorldsEdge)**
   This unofficial community API provides the latest leaderboard data (JSON).  Due to a 100-row request limit, data is retrieved in chunks.  Each chunk is validated and loaded as a separate JSON file into ADLS2.

Each API endpoint has dedicated Python scripts following a consistent template:

* a. Import functions from helper modules (under `common` directory).
* b. Ingest parameters from the configuration file.
* c. Establish an ADLS2 connection.
* d. Submit GET requests to retrieve data.
* e. Validate data against the expected schema (Pydantic).
* f. Load data into the ADLS2 container.

Unit tests using `pytest` ensure function correctness. Databricks jobs orchestrate script execution. An `run_all.py` script runs all individual Tasks, running reach phase of the medaillion pipeline seperately.

### Data Transformation

Data transformation occurs in Databricks with Unity Catalog, using the Medallion architecture (bronze -> silver -> gold). The gold layer uses a star schema optimized for querying. Three seperate `vw_` files were created as consumption views to store the aggregated data in ADLS2 (simply to reduce costs on reading data in Streamlit directly from Databricks). A deployment script triggers the Databricks jobs which are defined in the `pipelines` directory. These jobs contain a DAG of tasks, which in turn run the .py medallion scripts in Pyspark on Databricks compute. 

### Workflow Environment

Development and production environments are separated using distinct ADLS2 containers (`dev`, `prod`) and Unity catalogues (`aoe_dev`, `aoe_prod`). Environment is set based on class objects defined with `env_setting.py` which is applied to all scripts.

### Github Workflows

CI workflows (`ci.yaml`) on pull requests run linting (Ruff) and test with `pytest`. CD workflows (`cd.yaml`) on merge to main trigger the `run_all` pipeline set to the prod environment.

## 7. Future Direction

* Migrate from custom-built data quality checks to pre-made solutions (i.e. Soda).
* Data quality dashboards for Databrick job runs.
* Add a page on Streamlit for displaying the weekly file availbility (not all weeks are available from source).
* Incorporating additional AOE data (civilization strengths/weaknesses, logos).
* Infrastructure as Code via Databricks Asset Bundles.
* Partition source files in ADLS2 directories (year/month/day).
* Enchancing Streamlit dashboard design.

## 8. Miscellaneous

### Project Structure

```bash
├── Dockerfile
├── .env
├── README.md
├── requirements.txt
├── .github
│   └── workflows
│       ├── ci.yaml
│       └── cd.yaml
├── dags
│   ├── all_project_dag.py
│   ├── dbt_dag.py
│   ├── elt_metadata_dag.py
│   ├── elt_relic_api_dag.py
│   ├── elt_stat_matches_dag.py
│   ├── elt_stat_players_dag.py
│   └── set_load_master_dag.py
├── src
│   ├── __init__.py
│   ├── config.yaml
│   ├── elt_metadata.py
│   ├── elt_relic_api.py
│   ├── elt_stat_matches.py
│   ├── elt_stat_players.py
│   ├── project_tests.py
│   ├── set_load_master.py
│   ├── utils.py
│   ├── extract
│   │   ├── __init__.py
│   │   ├── filter.py
│   │   └── models.py
│   ├── load
│   │   ├── __init__.py
│   │   └── loader.py
│   └── transform
│       └── dbt_aoe
│           ├── dbt_project.yml
│           ├── package-lock.yml
│           ├── packages.yml
│           ├── profiles.yml.template
│           ├── analyses
│           ├── dbt_packages
│           │   └── dbt_external_tables
│           ├── macros
│           │   ├── deduplicate_by_key.sql
│           │   ├── filter_load.sql
│           │   └── generate_schema_name.sql
│           ├── models
│           │   ├── Medallion_README.md
│           │   ├── bronze
│           │   │   ├── bronze_schema.yml
│           │   │   ├── dim_date_br.py
│           │   │   ├── ext_table_schema.yml
│           │   │   ├── leaderboards_br.sql
│           │   │   ├── matches_br.sql
│           │   │   ├── players_br.sql
│           │   │   ├── statgroup_br.sql
│           │   │   ├── v_matches_raw.sql
│           │   │   ├── v_players_raw.sql
│           │   │   └── v_relic_raw.sql
│           │   ├── gold
│           │   │   ├── dim_civ.sql
│           │   │   ├── dim_date.sql
│           │   │   ├── dim_match.sql
│           │   │   ├── dim_player.sql
│           │   │   ├── fact_player_matches.sql
│           │   │   └── gold_schema.yml
│           │   └── silver
│           │       ├── matches_sr.sql
│           │       ├── player_leaderboard_stats_sr.sql
│           │       ├── player_match_sr.sql
│           │       └── silver_schema.yml
│           ├── seeds
│           │   ├── country_list.csv
│           │   └── seeds.yml
│           ├── snapshots
│           └── tests
│               ├── assert_countrys_mapped.sql
│               └── generic
│                   ├── test_recent_ldts.sql
│                   └── test_within_threshold.sql
```