#from airflow import DAG
from airflow.decorators import task,dag
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
import json

@dag(
    dag_id="nasa_apod_etl",
    schedule="@daily",
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False
)

def nasa_apod_etl():
    #step 1 create table if it doesnt exist
    @task
    def create_table():
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')


        create_table_sql = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        pg_hook.run(create_table_sql)
    
    #step 2 extract the nasa api data apod 
    #https://api.nasa.gov/planetary/apod?api_key=OcqkaMDf6tT9g2QgcE2vCfQkPrlQWw3IDetbwFu1

    extract_apod = HttpOperator(
        task_id='extract_nasa_apod',
        http_conn_id='nasa_api',
        endpoint='planetary/apod',
        method='GET',
        data = {"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"},
        response_filter=lambda response: response.json(),
    )

    #step 3 transform the data
    @task
    def transform_data(apod_data):
        transformed_data = {
            "title": apod_data.get("title",""),
            "explanation": apod_data.get("explanation",""),
            "url": apod_data.get("url",""),
            "date": apod_data.get("date",""),
            "media_type": apod_data.get("media_type","")
        }
        return transformed_data


    #step 4 load the data into postgres
    @task
    def load_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        insert_sql = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        pg_hook.run(insert_sql, parameters=(
            transformed_data['title'],
            transformed_data['explanation'],
            transformed_data['url'],
            transformed_data['date'],
            transformed_data['media_type']
        ))
    #step 5 verify the data in dbviewer
    #step 6 dependencies
    create_table() >> extract_apod
    api_response = extract_apod.output
    transformed_data = transform_data(api_response)
    load_data(transformed_data)


nasa_apod_etl()
    