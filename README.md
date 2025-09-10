# airflow-hdfs-pipeline

Airflow DAG to validate CSVs in HDFS, archive them, and copy them to a destination; includes a Spark validation job.

##  Project Structure


airflow-orchestration/
├─ dags/
│ └─ copy_transactions_task2_min.py # DAG definition
├─ jobs/
│ └─ validate_csvs.py 
├─ assets/
│ └─ airflow_dag_ui.png 
├─ requirements.txt
└─ .gitignore 

