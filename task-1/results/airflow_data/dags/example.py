from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta
import pandas as pd

def read_csv(**context):
    try:
        path = '/opt/airflow/data/sample_data.csv'
        df = pd.read_csv(path)
        context['ti'].xcom_push(key='orders_csv', value=df.to_dict(orient='records'))
        return len(df)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise

def read_postgres(**context):
    try:
        hook = PostgresHook(postgres_conn_id='test_db')
        df = hook.get_pandas_df('select id,userId,created::text,status from orders')
        context['ti'].xcom_push(key='orders_pg', value=df.to_dict(orient='records'))
        return len(df)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise

def calculate_failure_rate(**context):
    try:
        df_pg = pd.DataFrame(context['ti'].xcom_pull(task_ids="read_postgres", key="orders_pg"))
        df_csv = pd.DataFrame(context['ti'].xcom_pull(task_ids="read_csv", key="orders_csv"))

        df_all = pd.concat([df_pg, df_csv], ignore_index=True)
        success_count = (df_all['status'] == 'success').sum()
        fail_count = (df_all['status'] == 'fail').sum()
        total = success_count + fail_count
        failure_rate = fail_count / total if total > 0 else 0
        context['ti'].xcom_push(key="failure_rate", value=failure_rate)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise

def branch(**context):
    failure_rate = context['ti'].xcom_pull(task_ids="calculate_failure_rate", key="failure_rate")
    if failure_rate > 0.1:
        return "send_email_high_failure_rate"
    return None

default_args = {
    "email": ["admin@example.com"],
    "email_on_failure": True,
    "email_on_success": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
}

with DAG(
    dag_id='my_example_dag',
    default_args=default_args,
    catchup=False
) as dag:
    read_postgres_task = PythonOperator(
        task_id="read_postgres",
        python_callable=read_postgres,
    )

    read_csv_task = PythonOperator(
        task_id="read_csv",
        python_callable=read_csv,
    )

    calculate_failure_rate_task = PythonOperator(
        task_id="calculate_failure_rate",
        python_callable=calculate_failure_rate,
    )

    branch_task = BranchPythonOperator(
        task_id="check_failure_rate",
        python_callable=branch,
    )

    no_action = PythonOperator(
        task_id="no_action",
        python_callable=lambda: print("Failure rate is OK"),
    )

    notify_task = EmailOperator(
        task_id="send_email_high_failure_rate",
        to="admin@example.com",
        subject="Высокий процент ошибок при заказе",
        html_content="Высокий процент ошибок при заказе",
        trigger_rule="all_done"
    )

    [read_postgres_task, read_csv_task] >> calculate_failure_rate_task >> branch_task >> [notify_task, no_action]