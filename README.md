# airflow-hdfs-pipeline

This project implements an **Apache Airflow DAG** that validates CSV files stored in HDFS, archives them into a backup folder, copies them to a destination directory, and finally sends a success notification email.  
The validation step is powered by **PySpark**, ensuring schema consistency before files are moved.

---

## Features
- **CSV Validation**: Uses PySpark to verify schema and columns in incoming CSVs.  
- **Archiving**: Automatically moves validated CSVs to an archive folder with a timestamped partition.  
- **Data Copying**: Copies archived CSVs to a destination HDFS path for downstream processing.  
- **Email Notifications**: Sends an email upon successful completion of the pipeline.  

---

## Project Structure
```bash
airflow-orchestration/
├── dags/
│   └── copy_transactions_task2_min.py       
├── jobs/
│   └── validate_csvs.py                     
├── assets/
│   └── airflow_dag_ui.png
├── requirements.txt                        
└── .gitignore
