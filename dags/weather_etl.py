from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests


CITIES = {
    "Moscow": {"lat": 55.75, "lon": 37.62},
    "Aachen": {"lat": 50.78, "lon": 6.08},
}


def extract_weather(**context):
    result = []
    for city, coords in CITIES.items():
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        result.append({
            "city": city,
            "temperature": data["current"]["temperature_2m"],
            "humidity": data["current"]["relative_humidity_2m"],
            "wind_speed": data["current"]["wind_speed_10m"],
        })

    context["ti"].xcom_push(key="raw_weather", value=result)


def transform_weather(**context):
    raw_data = context["ti"].xcom_pull(key="raw_weather", task_ids="extract")

    transformed = []
    for item in raw_data:
        transformed.append({
            "city": item["city"],
            "temperature": round(item["temperature"], 1),
            "humidity": round(item["humidity"], 1),
            "wind_speed": round(item["wind_speed"], 1),
        })

    context["ti"].xcom_push(key="clean_weather", value=transformed)


def load_to_postgres(**context):
    data = context["ti"].xcom_pull(key="clean_weather", task_ids="transform")

    hook = PostgresHook(postgres_conn_id="postgres_default")
    insert_query = """
        INSERT INTO weather_data (city, fetched_at, temperature, humidity, wind_speed)
        VALUES (%s, NOW(), %s, %s, %s);
    """

    for row in data:
        hook.run(insert_query, parameters=(
            row["city"], row["temperature"], row["humidity"], row["wind_speed"]
        ))


default_args = {
    "owner": "airflow",
    "retries": 1,
}


with DAG(
    dag_id="weather_etl",
    default_args=default_args,
    description="ETL пайплайн для погодных данных",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_weather,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_weather,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_to_postgres,
    )

    extract_task >> transform_task >> load_task