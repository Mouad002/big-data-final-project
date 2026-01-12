from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.bash import BashSensor
from datetime import datetime

with DAG(
    dag_id="traffic_spark_processing",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    wait_for_hdfs_data = BashSensor(
        task_id="wait_for_raw_data",
        bash_command="""
        docker exec namenode hdfs dfs -test -e /data/raw/traffic
        """,
        poke_interval=30,
        timeout=600
    )

    run_spark_job = BashOperator(
        task_id="run_spark_processing",
        bash_command="""
        docker exec spark-processor spark-submit \
        --master local[*] \
        /opt/spark-apps/traffic_processor.py
        """,

    )

    wait_for_hdfs_data >> run_spark_job
