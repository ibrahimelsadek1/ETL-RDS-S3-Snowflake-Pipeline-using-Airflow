# ETL-RDS-S3-Snowflake-Pipeline-using-Airflow

This project utilizes Apache Airflow to automate the extraction, transformation, and loading (ETL) process of employee data from different sources into a Snowflake data warehouse.

**Project Structure**
The project consists of the following components:

**pipline.py**: The main Airflow DAG file that defines the workflow and tasks.

**trinsformation.py:** A Python script containing a function (join_and_detect_new_or_changed_rows) used within the DAG to join and detect new or changed rows.

**queries.py:** A module containing SQL queries used in the project.

**includes/:** A directory that includes any additional Python modules or scripts required for the project.


**Task Flow**

- Extract employee salary data and store it in an S3 bucket (sql_to_s3_task_emp_sal).
- Extract employee details data and store it in an S3 bucket (sql_to_s3_task_emp_details).
- Join and detect new or changed rows from the extracted data (join_and_detect_new_or_changed_rows).
- Branch the workflow based on the presence of IDs to update (branch_task).
- If there are IDs to update, execute the Snowflake update query (snowflake_update).
- If there are new rows to insert, execute the Snowflake insert query (insert).
- If there are no IDs to update, execute a dummy task (dummy_task).
