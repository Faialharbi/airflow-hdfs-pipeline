from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText


SENDER_EMAIL = "<YOUR_EMAIL>"

RECEIVER_EMAIL = "<RECEIVER_EMAIL>"

APP_PASSWORD_DISPLAYED = "<APP_PASSWORD>"  

def _send_success_email(**context):
    
    app_password = APP_PASSWORD_DISPLAYED.replace(" ", "")  

    subject = "[Task2] CSVs validated & copied successfully"
    body = (
        "All CSVs under /user/bank/ were validated (schema OK),\n"
        f"backed up to /user/bank/archive/DT={context['ts_nodash']},\n"
        f"and copied to /user/hadoop/destination_csv/DT={context['ts_nodash']}.\n"
    )

    msg = MIMEText(body, _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL

    with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(SENDER_EMAIL, app_password)
        server.sendmail(SENDER_EMAIL, [RECEIVER_EMAIL], msg.as_string())

default_args = {
    "owner": "fai",
    "start_date": datetime(2025, 6, 27),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="copy_transactions_task2_min",
    default_args=default_args,
    schedule=None,   
    catchup=False,
    tags=["spark", "hdfs", "csv", "task2"],
) as dag:

    check_source_folder = BashOperator(
        task_id="check_source_folder",
        bash_command='hdfs dfs -test -d /user/bank || (echo "Missing /user/bank" && exit 1)'
    )

    validate_csvs = BashOperator(
        task_id="validate_csvs",
        bash_command="/usr/local/spark/bin/spark-submit --master local[*] "
                     "<PATH_TO_YOUR_PROJECT>/jobs/validate_csvs.py"

    )

    backup_to_archive = BashOperator(
        task_id="backup_to_archive",
        bash_command=(
            "hdfs dfs -mkdir -p /user/bank/archive/DT={{ ts_nodash }} && "
            "hdfs dfs -cp /user/bank/*.csv /user/bank/archive/DT={{ ts_nodash }}/"
        )
    )

    copy_to_destination = BashOperator(
        task_id="copy_to_destination",
        bash_command=(
            "hdfs dfs -mkdir -p /user/hadoop/destination_csv/DT={{ ts_nodash }} && "
            "hdfs dfs -cp /user/bank/*.csv /user/hadoop/destination_csv/DT={{ ts_nodash }}/"
        )
    )

    send_success_email = PythonOperator(
        task_id="send_success_email",
        python_callable=_send_success_email,
        provide_context=True,
    )

    check_source_folder >> validate_csvs >> backup_to_archive >> copy_to_destination >> send_success_email
