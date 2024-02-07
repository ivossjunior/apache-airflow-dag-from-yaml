from airflow.operators.python_operator import PythonOperator
import custom_lib as cl 
from airflow import DAG
from datetime import datetime

dag_id = "dag_example"
default_args = {'owner': 'Ivo', 'start_date': '2024-01-01'}
schedule_interval = "@daily"
catchup = False

dag = DAG(
    dag_id= dag_id,
    default_args= default_args,
    schedule_interval= schedule_interval,
    catchup= catchup,
)
extract = PythonOperator(
    task_id = "extract",
    python_callable = cl.custom_print,
    provide_context = True,
    dag = dag
)

transform = PythonOperator(
    task_id = "transform",
    python_callable = cl.custom_print,
    provide_context = True,
    dag = dag
)

step_01 = PythonOperator(
    task_id = "step_01",
    python_callable = cl.custom_print,
    provide_context = True,
    dag = dag
)

step_02 = PythonOperator(
    task_id = "step_02",
    python_callable = cl.custom_print,
    provide_context = True,
    dag = dag
)

step_03 = PythonOperator(
    task_id = "step_03",
    python_callable = cl.custom_print,
    provide_context = True,
    dag = dag
)

step_04 = PythonOperator(
    task_id = "step_04",
    python_callable = cl.custom_print,
    provide_context = True,
    dag = dag
)

extract >> [transform, step_01]
step_01 >> [step_02, step_04]
step_02 >> [step_03]
