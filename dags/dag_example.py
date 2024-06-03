import os.path
import random

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
import psycopg2 as psycopg2
import uuid
import json
from datetime import date, datetime, time
from decimal import Decimal






default_args = {"owner": "Yamin", "start_date": datetime(2023, 9, 29), "retries": 1}

dag = DAG("stored_procedures", default_args=default_args, schedule_interval=None)


conn_args = dict(
        host="host.docker.internal",
        user="postgres",
        password="postgres",
        dbname="postgres",
        port=5432,
    )
conn = psycopg2.connect(**conn_args)




def generate_uuid():
    dag_uuid = str(uuid.uuid4())
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO logging.batch_table (id, execution_date)
            VALUES (%s, NOW())
        """, (str(dag_uuid),))
        conn.commit()
        print("UUID inserted successfully!")
    except Exception as e:
        print(f"Error inserting UUID: {e}")
        conn.rollback()

    return dag_uuid




def load_drivers_data(**context):
    dag_uuid = context["task_instance"].xcom_pull(task_ids="insert_uuid")
    cur = conn.cursor()
    cur.execute("CALL load_drivers_data(%s);", (dag_uuid,))
    conn.commit()

def load_constructors_data(**context):
    dag_uuid = context["task_instance"].xcom_pull(task_ids="insert_uuid")
    cur = conn.cursor()
    cur.execute("CALL load_constructors_data(%s);", (dag_uuid,))
    conn.commit()

def load_races_data(**context):
    dag_uuid = context["task_instance"].xcom_pull(task_ids="insert_uuid")
    cur = conn.cursor()
    cur.execute("CALL load_races_data(%s);" , (dag_uuid,))
    conn.commit()

def load_results_data(**context):
    dag_uuid = context["task_instance"].xcom_pull(task_ids="insert_uuid")
    cur = conn.cursor()
    cur.execute("CALL load_results_data(%s);", (dag_uuid,))
    conn.commit()

def load_reference_table_data(**context):
    dag_uuid = context["task_instance"].xcom_pull(task_ids="insert_uuid")
    cur = conn.cursor()
    cur.execute("CALL load_reference_table_data(%s);", (dag_uuid,))
    conn.commit()



def insert_into_processed_constructors(**context):
    dag_uuid = context["task_instance"].xcom_pull(task_ids="insert_uuid")
    cur = conn.cursor()
    cur.execute("CALL insert_into_processed_constructors(%s);", (dag_uuid,))
    conn.commit()


def insert_into_processed_drivers(**context):
    dag_uuid = context["task_instance"].xcom_pull(task_ids="insert_uuid")
    cur = conn.cursor()
    cur.execute("CALL insert_into_processed_drivers(%s);", (dag_uuid,))
    conn.commit()

def insert_into_processed_results(**context):
    dag_uuid = context["task_instance"].xcom_pull(task_ids="insert_uuid")
    cur = conn.cursor()
    cur.execute("CALL insert_into_processed_results(%s);", (dag_uuid,))
    conn.commit()

def insert_into_processed_races(**context):
    dag_uuid = context["task_instance"].xcom_pull(task_ids="insert_uuid")
    cur = conn.cursor()
    cur.execute("CALL insert_into_processed_races(%s);", (dag_uuid,))
    conn.commit()

def update_drivers_nationalities():
    cur = conn.cursor()
    cur.execute("CALL update_drivers_nationality();")
    conn.commit()





def convert_to_serializable(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    elif isinstance(value, time):
        return value.strftime('%H:%M:%S')
    elif isinstance(value, Decimal):
        return float(value)
    elif hasattr(value, 'microseconds') and hasattr(value, 'seconds') and hasattr(value, 'days'):
        # Assuming it's an interval type
        return str(value)
    return value


def create_json():
    cur = conn.cursor()

    # Query to join drivers, results, and constructors
    query = """
    SELECT d.unique_driver_id, d.driver_id, d.driver_ref, d.number, d.code, d.forename, d.surname,
           d.nationality AS driver_nationality,
           r.result_id, r.race_id, r.grid, r.position, r.points, r.laps, r.fastest_lap, r.status_id,
           c.constructor_id AS c_id, c.constructor_ref, c.name AS constructor_name, c.nationality AS constructor_nationality
    FROM raw.drivers d
    LEFT JOIN raw.results r ON d.driver_id = r.driver_id
    LEFT JOIN raw.constructors c ON r.constructor_id = c.constructor_id;
    """

    cur.execute(query)
    rows = cur.fetchall()

    # Convert rows to a list of dictionaries
    data_list = []
    for row in rows:
        data_dict = {
            'unique_driver_id': row[0],
            'driver_id': row[1],
            'driver_ref': row[2],
            'number': row[3],
            'code': row[4],
            'forename': row[5],
            'surname': row[6],
            'nationality': row[7],
            'result': {
                'result_id': row[8],
                'race_id': row[9],
                'grid': row[10],
                'position': row[11],
                'points': convert_to_serializable(row[12]),
                'laps': row[13],
                'fastest_lap': row[14],
                'status_id': row[15],
            },
            'constructor': {
                'constructor_id': row[16],
                'constructor_ref': row[17],
                'name': row[18],
                'nationality': row[19],
            }
        }
        data_list.append(data_dict)

    # Export data to JSON file
    output_file = r'./dags/output.json'
    with open(output_file, 'w') as json_file:
        json.dump(data_list, json_file, indent=2)

    # Close the database connection
    conn.close()

    print(f"Data exported to {output_file}")



bash_task_start = BashOperator(
    task_id="bash_task_start",
    bash_command='echo "Here is your bash message at the start pf your DAG, your workflow has started !"',
    dag=dag,
)

bash_task_end = BashOperator(
    task_id="bash_task_end",
    bash_command='echo "Here is your bash message at the end of your DAG"',
    dag=dag,
)

insert_uuid = PythonOperator(
    task_id="insert_uuid",
    python_callable=generate_uuid,
    dag=dag,
)

load_drivers = PythonOperator(
    task_id="load_drivers",
    python_callable=load_drivers_data,
    provide_context=True,
    dag=dag,
)

load_constructors = PythonOperator(
    task_id="load_constructors",
    python_callable=load_constructors_data,
    provide_context=True,
    dag=dag,
)

load_races = PythonOperator(
    task_id="load_races",
    python_callable=load_races_data,
    provide_context=True,
    dag=dag,
)

load_results = PythonOperator(
    task_id="load_results",
    python_callable=load_results_data,
    provide_context=True,
    dag=dag,
)

load_reference_table = PythonOperator(
    task_id="load_reference_table",
    python_callable=load_reference_table_data,
    provide_context=True,
    dag=dag,
)




r2p_constructors = PythonOperator(
    task_id="r2p_constructors",
    python_callable=insert_into_processed_constructors,
    provide_context=True,
    dag=dag,
)

r2p_drivers = PythonOperator(
    task_id="r2p_drivers",
    python_callable=insert_into_processed_drivers,
    provide_context=True,
    dag=dag,
)

r2p_races = PythonOperator(
    task_id="r2p_races",
    python_callable=insert_into_processed_races,
    provide_context=True,
    dag=dag,
)

r2p_results = PythonOperator(
    task_id="r2p_results",
    python_callable=insert_into_processed_results,
    provide_context=True,
    dag=dag,
)

update_driver_nationality = PythonOperator(
    task_id= 'update_driver_nationality',
    python_callable = update_drivers_nationalities,
    dag=dag,
)


export_json = PythonOperator(
    task_id = 'export_json',
    python_callable= create_json,
    dag=dag,
)





"""""""""
file_sensor_task = FileSensor(
    task_id="file_sensor_task",
    filepath="driver_standings.csv",
    poke_interval=20,
    mode="poke",
    fs_conn_id="fs_default",
    dag=dag,
)
"""""



(
        bash_task_start
        >> insert_uuid
        >> load_drivers
        >> load_constructors
        >> load_races
        >> load_results
        >> load_reference_table
        >> r2p_drivers
        >> r2p_constructors
        >> r2p_races
        >> r2p_results
        >> update_driver_nationality
        >> export_json
        >> bash_task_end
)
