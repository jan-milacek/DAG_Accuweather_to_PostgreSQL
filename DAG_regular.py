import os
import json
import requests
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Load environment variables from .env file at DAG definition time
load_dotenv()

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
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

# Function to handle database connection errors with enhanced notification
def handle_db_error(context, error):
    """Handle database errors with detailed notification"""
    error_msg = f"Database error: {str(error)}"
    details = f"Exception type: {type(error).__name__}\nFull exception: {str(error)}"
    
    print(error_msg)
    
    # Send detailed error email
    send_api_error_email(context, error_msg, details)
    
    raise AirflowFailException(error_msg)

# Enhanced default arguments with email configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    **get_email_config(),  # Unpack email configuration
    'on_failure_callback': task_failure_callback
}

# Create regular DAG that runs daily at 2 AM
regular_dag = DAG(
    'accuweather_daily',
    default_args=default_args,
    description='Fetch daily AccuWeather data',
    schedule_interval='0 2 * * *',  # Run at 2 AM every day
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Function to extract today's weather data
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

# Function to transform weather data
def transform_weather_data(**context):
    ti = context['ti']
    
    try:
        raw_data = json.loads(ti.xcom_pull(key='raw_weather_data'))
    except Exception as e:
        error_msg = f"Error retrieving raw data from XCom: {str(e)}"
        print(error_msg)
        send_api_error_email(context, error_msg, str(e))
        raise AirflowFailException(error_msg)
    
    print(f"Transforming {len(raw_data)} raw records")
    
    # Initialize list for transformed data
    transformed_data = []
    
    try:
        # Process each record
        for record in raw_data:
            # Extract relevant fields
            date = record.get('LocalObservationDateTime')
            temp = record.get('Temperature', {}).get('Metric', {}).get('Value')
            weather_text = record.get('WeatherText')
            fetch_date = record.get('fetch_date')
            
            # Create a record
            transformed_record = {
                'observation_date': date,
                'temperature': temp,
                'condition': weather_text,
                'fetch_date': fetch_date
            }
            transformed_data.append(transformed_record)
        
        # Convert to DataFrame
        df = pd.DataFrame(transformed_data)
        print(f"Transformed data into DataFrame with {len(df)} rows and columns: {df.columns.tolist()}")
        
        # Show data sample
        if not df.empty:
            print("Sample of transformed data:")
            print(df.head(2).to_string())
        else:
            error_msg = "Transformation resulted in empty DataFrame"
            print(error_msg)
            send_api_error_email(context, error_msg, "No data after transformation")
            raise AirflowFailException(error_msg)
        
        # Save as CSV for database loading
        csv_data = df.to_csv(index=False)
        ti.xcom_push(key='processed_weather_data', value=csv_data)
        return f"Weather data transformation successful - {len(df)} records processed"
        
    except Exception as e:
        error_msg = f"Error during data transformation: {str(e)}"
        print(error_msg)
        send_api_error_email(context, error_msg, str(e))
        raise AirflowFailException(error_msg)

# Function to prepare data for loading
def prepare_load_data(**context):
    ti = context['ti']
    
    try:
        csv_data = ti.xcom_pull(key='processed_weather_data')
        
        # Save CSV temporarily
        with open('/tmp/weather_data.csv', 'w') as f:
            f.write(csv_data)
        
        # Verify the file was written correctly
        file_size = os.path.getsize('/tmp/weather_data.csv')
        print(f"CSV file created with size: {file_size} bytes")
        
        if file_size <= 0:
            error_msg = "Created CSV file is empty"
            print(error_msg)
            send_api_error_email(context, error_msg, "File size is 0 bytes")
            raise AirflowFailException(error_msg)
        
        # Read back the first few lines to verify content
        with open('/tmp/weather_data.csv', 'r') as f:
            header = f.readline().strip()
            print(f"CSV header: {header}")
            data_sample = f.readline().strip() if file_size > len(header) + 10 else "NO DATA ROWS"
            print(f"CSV data sample: {data_sample}")
        
        return "Weather data prepared for loading"
        
    except Exception as e:
        error_msg = f"Error preparing data for loading: {str(e)}"
        print(error_msg)
        send_api_error_email(context, error_msg, str(e))
        raise AirflowFailException(error_msg)

# Function to load data to PostgreSQL
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
        # Check if file exists
        if not os.path.exists(csv_path):
            error_msg = f"CSV file not found at path: {csv_path}"
            print(error_msg)
            handle_db_error(context, Exception(error_msg))
        
        # Read the CSV with pandas
        df = pd.read_csv(csv_path)
        print(f"Successfully read CSV with {len(df)} rows and columns: {df.columns.tolist()}")
        
        if df.empty:
            error_msg = "CSV file contains no data"
            print(error_msg)
            handle_db_error(context, Exception(error_msg))
        
        # Connect to PostgreSQL
        try:
            conn = psycopg2.connect(
                host=pg_host,
                port=pg_port,
                database=pg_db,
                user=pg_user,
                password=pg_password,
                connect_timeout=10  # 10 second timeout
            )
        except psycopg2.OperationalError as e:
            error_msg = f"Could not connect to database: {str(e)}"
            handle_db_error(context, e)
        
        # Set autocommit to True for individual transactions
        conn.autocommit = True
        
        # Insert data into the database
        records_inserted = 0
        try:
            with conn.cursor() as cursor:
                for index, row in df.iterrows():
                    try:
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
                        error_msg = f"Error inserting row {index}: {str(e)}"
                        print(error_msg)
                        # Continue with other rows instead of failing completely
                        continue
        except psycopg2.Error as e:
            handle_db_error(context, e)
        finally:
            conn.close()
        
        if records_inserted == 0:
            error_msg = "No records were inserted into the database"
            print(error_msg)
            handle_db_error(context, Exception(error_msg))
            
        return f"Data loaded to PostgreSQL successfully - {records_inserted} records inserted"
    
    except Exception as e:
        handle_db_error(context, e)

# Create a success notification task
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

# Define tasks
extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    provide_context=True,
    dag=regular_dag,
)

transform_task = PythonOperator(
    task_id='transform_weather_data',
    python_callable=transform_weather_data,
    provide_context=True,
    dag=regular_dag,
)

prepare_load_task = PythonOperator(
    task_id='prepare_load_data',
    python_callable=prepare_load_data,
    provide_context=True,
    dag=regular_dag,
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=regular_dag,
)

# Set task dependencies
extract_task >> transform_task >> prepare_load_task >> load_task >> success_email_task