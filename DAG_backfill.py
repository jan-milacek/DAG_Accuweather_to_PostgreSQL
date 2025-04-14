import os
import json
import requests
import pandas as pd
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file at DAG definition time
load_dotenv()

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.utils.email import send_email_smtp
from airflow.exceptions import AirflowFailException

# Email configuration function
def get_email_config():
    return {
        'email': os.environ.get('NOTIFICATION_EMAIL', 'your_email@example.com'),
        'email_on_failure': True,
        'email_on_retry': True,
        'email_on_success': False,  # Set to True if you want notifications on success too
    }

# Enhanced default arguments with email configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    **get_email_config()  # Unpack email configuration
}

# Custom error handler function 
def task_failure_callback(context):
    """
    Function to be called when a task instance fails.
    This sends a detailed email notification.
    """
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    exception = context.get('exception')
    
    # Get the task logs
    log_url = context.get('task_instance').log_url
    
    # Create an email with detailed error information
    subject = f"Airflow Alert: Task {task_id} in DAG {dag_id} Failed"
    
    html_content = f"""
    <h3>Task Failure Alert</h3>
    <p><strong>DAG</strong>: {dag_id}</p>
    <p><strong>Task</strong>: {task_id}</p>
    <p><strong>Execution Date</strong>: {execution_date}</p>
    <p><strong>Status</strong>: Failed</p>
    
    <h4>Error Details</h4>
    <pre>{exception}</pre>
    
    <p>Please check the <a href="{log_url}">task logs</a> for more details.</p>
    """
    
    # Use the Airflow email sending function
    send_email_smtp(
        to=default_args['email'], 
        subject=subject, 
        html_content=html_content,
        files=None,
        cc=None,
        bcc=None,
        mime_charset='utf-8',
        mime_subtype='mixed'
    )
    
    return True

# Update both DAGs to include the custom failure callback
# For the backfill DAG
backfill_dag = DAG(
    'accuweather_backfill',
    default_args={**default_args, 'on_failure_callback': task_failure_callback},
    description='Backfill missing AccuWeather data for specific date range',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'start_date': '2025-04-01', 
        'end_date': '2025-04-03',
    },
)

# For the regular DAG
regular_dag = DAG(
    'accuweather_daily',
    default_args={**default_args, 'on_failure_callback': task_failure_callback},
    description='Fetch daily AccuWeather data',
    schedule_interval='0 2 * * *',  # Run at 2 AM every day
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Add a function to send a detailed error email for API failures
def send_api_error_email(context, error_message, details=None):
    """Send a detailed error email about API failures"""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    
    subject = f"AccuWeather API Error in {dag_id} - {task_id}"
    
    html_content = f"""
    <h3>AccuWeather API Error Alert</h3>
    <p><strong>DAG</strong>: {dag_id}</p>
    <p><strong>Task</strong>: {task_id}</p>
    <p><strong>Execution Date</strong>: {execution_date}</p>
    <p><strong>Error Message</strong>: {error_message}</p>
    """
    
    if details:
        html_content += f"""
        <h4>Error Details</h4>
        <pre>{details}</pre>
        """
    
    send_email_smtp(
        to=default_args['email'], 
        subject=subject, 
        html_content=html_content
    )

# Example of how to modify the extract function to use the enhanced error handling
def extract_weather_data(**context):
    # Load environment variables
    load_dotenv()
    
    # Get yesterday's date (since we run in the morning, we want yesterday's complete data)
    execution_date = context['execution_date']
    target_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"Fetching weather data for: {target_date}")
    
    # Get API key 
    api_key = os.environ.get('ACCUWEATHER_API_KEY')
    if not api_key:
        error_msg = "ERROR: No API key found in environment variables"
        print(error_msg)
        send_api_error_email(context, "API Key Missing", "AccuWeather API key not found in environment variables")
        raise ValueError("AccuWeather API key not found in environment variables")
    
    print(f"Using API key (first 4 chars): {api_key[:4]}...")
    
    location_key = '125594'  # Prague
    
    # AccuWeather historical API endpoint
    url = f"http://dataservice.accuweather.com/currentconditions/v1/{location_key}/historical/24?apikey={api_key}"
    
    print(f"Using URL: {url}")
    
    try:
        response = requests.get(url)
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            daily_data = response.json()
            print(f"Received {len(daily_data)} records")
            
            # Add date to track which day this is for
            for record in daily_data:
                record['fetch_date'] = target_date
            
            # Store the collected data
            context['ti'].xcom_push(key='raw_weather_data', value=json.dumps(daily_data))
            return f"Data extraction completed for {target_date} with {len(daily_data)} records"
        else:
            error_msg = f"Failed to fetch data: {response.status_code}"
            details = f"Response text: {response.text}\nURL: {url}"
            print(error_msg)
            print(details)
            
            # Send detailed error email
            send_api_error_email(context, error_msg, details)
            
            raise AirflowFailException(f"API request failed with status code {response.status_code}")
    except requests.exceptions.RequestException as e:
        error_msg = f"Network error while fetching data: {str(e)}"
        print(error_msg)
        
        # Send detailed error email
        send_api_error_email(context, error_msg, str(e))
        
        raise AirflowFailException(error_msg)
    except Exception as e:
        error_msg = f"Exception while fetching data: {str(e)}"
        print(error_msg)
        
        # Send detailed error email
        send_api_error_email(context, error_msg, str(e))
        
        raise

# Add a function to handle database connection errors with enhanced notification
def handle_db_error(context, error):
    """Handle database errors with detailed notification"""
    error_msg = f"Database error: {str(error)}"
    details = f"Exception type: {type(error).__name__}\nFull exception: {str(error)}"
    
    print(error_msg)
    
    # Send detailed error email
    send_api_error_email(context, error_msg, details)
    
    raise AirflowFailException(error_msg)

# Example of how to modify the database loading function
def load_to_postgres(**context):
    # Load environment variables again for the task context
    load_dotenv()
    
    # Get PostgreSQL connection details from environment variables
    pg_host = os.environ.get('POSTGRES_HOST', 'localhost')
    pg_port = os.environ.get('POSTGRES_PORT', '5432')
    pg_db = os.environ.get('POSTGRES_DB', 'weather_db')
    pg_user = os.environ.get('POSTGRES_USER', 'postgres')
    pg_password = os.environ.get('POSTGRES_PASSWORD', 'postgres')
    pg_schema = 'weather_db_schema'
    
    print(f"Connecting to DB: {pg_user}@{pg_host}:{pg_port}/{pg_db} (schema: {pg_schema})")
    
    # Read CSV file
    csv_path = '/tmp/weather_data.csv'
    
    try:
        # Read the CSV with pandas
        df = pd.read_csv(csv_path)
        print(f"Successfully read CSV with {len(df)} rows and columns: {df.columns.tolist()}")
        
        # Connect to PostgreSQL
        try:
            conn = psycopg2.connect(
                host=pg_host,
                port=pg_port,
                database=pg_db,
                user=pg_user,
                password=pg_password
            )
        except psycopg2.OperationalError as e:
            handle_db_error(context, e)
        
        # Set autocommit to True for individual transactions
        conn.autocommit = True
        
        # Insert data into the database
        try:
            with conn.cursor() as cursor:
                records_inserted = 0
                for index, row in df.iterrows():
                    cursor.execute(
                        f"""
                        INSERT INTO {pg_schema}.historical_weather_data
                        (observation_date, temperature, condition, fetch_date)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            row['observation_date'],
                            row['temperature'],
                            row['condition'],
                            row['fetch_date']
                        )
                    )
                    records_inserted += 1
        except psycopg2.Error as e:
            handle_db_error(context, e)
        finally:
            conn.close()
            
        return f"Data loaded to PostgreSQL successfully - {records_inserted} records inserted"
    
    except Exception as e:
        handle_db_error(context, e)

# Example of how to add a task that explicitly sends a success notification email
success_email_task = EmailOperator(
    task_id='send_success_email',
    to=default_args['email'],
    subject='AccuWeather Pipeline Success',
    html_content="""
    <h3>Data Pipeline Completed Successfully</h3>
    <p>The AccuWeather data pipeline has completed all tasks successfully.</p>
    <p>You can view the data in your PostgreSQL database.</p>
    """,
    dag=regular_dag,
)

# Add this task to the end of your DAG if you want success notifications
# load_task >> success_email_task