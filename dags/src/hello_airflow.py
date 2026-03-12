from airflow import DAG
# airflow.operators.python
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

def hello():
    print("Hello Juntima!")

# เป็น workflow
with DAG(
    dag_id="hello_juntima", # กำหนด id ของ DAG แสดงใน Airflow
    start_date=pendulum.datetime(2025,3,1, tz="Asia/Bangkok"),
    schedule="@daily", # กำหนดรอบการทำงาน ของ task
    catchup=False,
) as dag:
    # งานนึงใน workflow
    task = PythonOperator(
        task_id="print_hello", # กำหนด id ของ task
        python_callable=hello # เรียกใช้งาน python ที่กำหนดไว้
    )