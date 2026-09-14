from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

# Định nghĩa hàm Python sẽ được gọi bởi task
def print_hello():
    print("Hello, Airflow! DAG đang chạy thành công.")

# Thiết lập thông tin DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_airflow',
    default_args=default_args,
    description='DAG đơn giản in Hello Airflow',
    schedule=timedelta(days=1),  # Chạy mỗi ngày một lần
    catchup=True,
)

# Định nghĩa Task
task_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Thiết lập thứ tự chạy task
task_hello
