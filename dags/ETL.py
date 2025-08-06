from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests

# DAG definition
with DAG(
    dag_id="nasa_postgres",
    start_date=datetime.now() - timedelta(days=1),
    schedule="@daily",
    catchup=False,
    tags=["example"]
) as dag:

    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        postgres_hook.run(create_table_query)

    @task
    def extract_apod():
        # Use Airflow connection to get the API key
        from airflow.hooks.base import BaseHook
        nasa_conn = BaseHook.get_connection("nasa_api")
        api_key = nasa_conn.extra_dejson.get("api_key")

        url = f"{nasa_conn.host}/planetary/apod"
        params = {"api_key": api_key}
        response = requests.get(url, params=params)

        if response.status_code != 200:
            raise Exception(f"Failed to fetch data: {response.text}")
        return response.json()

    @task
    def transform_apod_data(response):
        apod_data = {
            "title": response.get("title", ""),
            "explanation": response.get("explanation", ""),
            "url": response.get("url", ""),
            "date": response.get("date", ""),
            "media_type": response.get("media_type", "")
        }
        return apod_data

    @task
    def load_data_to_postgres(apod_data):
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        postgres_hook.run(insert_query, parameters=(
            apod_data["title"],
            apod_data["explanation"],
            apod_data["url"],
            apod_data["date"],
            apod_data["media_type"]
        ))

    # Task dependency
    create_table() >> load_data_to_postgres(transform_apod_data(extract_apod()))
