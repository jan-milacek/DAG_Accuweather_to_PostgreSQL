# AccuWeather Data Pipeline

## What is this?

A robust data pipeline that fetches weather data from AccuWeather and stores it in a PostgreSQL database using Apache Airflow. This project includes comprehensive error handling and notification features to ensure reliability and maintainability.

## How it works

Every day at 2 AM, this pipeline automatically:
1. Extracts yesterday's weather data from AccuWeather's API
2. Transforms the raw data into a structured format
3. Loads the processed data into a PostgreSQL database
4. Sends status notifications via email

Additionally, a backfill DAG allows you to retrieve historical weather data for any date range.

## Features

- **Automated Daily Data Collection**: Scheduled to run daily at 2 AM
- **Historical Data Backfill**: Retrieve weather data for any past date range
- **Comprehensive Error Handling**: Robust error detection and recovery
- **Email Notifications**: Get alerts for failures and optional success confirmations
- **Data Validation**: Ensures data quality at each step of the pipeline
- **Database Integration**: Seamlessly stores data in PostgreSQL

## Files in this project

- `accuweather_daily.py`: Main DAG that runs daily to collect weather data
- `accuweather_backfill.py`: Utility DAG for retrieving historical weather data
- `database_setup.sql`: SQL commands to set up the PostgreSQL database
- `.env`: Configuration file for API keys, database credentials, and email settings
- `README.md`: Documentation for the project (this file)

## Setup Guide

### Step 1: Get an AccuWeather API key
1. Visit [AccuWeather's developer portal](https://developer.accuweather.com/)
2. Create a developer account
3. Register a new application to receive an API key

### Step 2: Set up PostgreSQL
1. Install PostgreSQL if not already installed
2. Run the database setup script:
```bash
psql -U postgres -f database_setup.sql
```

### Step 3: Configure your environment
1. Copy `.env.example` to `.env`
2. Add your AccuWeather API key
3. Configure PostgreSQL connection details
4. Set up email notification settings:
   - SMTP server information
   - Email credentials
   - Notification recipient address

### Step 4: Install and configure Airflow
1. Install Apache Airflow:
```bash
pip install apache-airflow[postgres,email]
```

2. Set environment variables for Airflow:
```bash
export AIRFLOW_HOME=~/airflow
```

3. Initialize the Airflow database:
```bash
airflow db init
```

4. Create an admin user:
```bash
airflow users create \
    --username admin \
    --firstname FirstName \
    --lastname LastName \
    --role Admin \
    --email your_email@example.com \
    --password your_password
```

5. Copy the DAG files to your Airflow dags folder:
```bash
cp accuweather_daily.py accuweather_backfill.py $AIRFLOW_HOME/dags/
```

### Step 5: Start Airflow
1. Start the Airflow webserver:
```bash
airflow webserver --port 8080
```

2. In a new terminal, start the Airflow scheduler:
```bash
airflow scheduler
```

3. Access the Airflow web interface at http://localhost:8080

### Step 6: Activate the DAG
1. Log in to the Airflow web interface
2. Navigate to the DAGs page
3. Enable the `accuweather_daily` DAG
4. The DAG will now run automatically according to its schedule

## Running a backfill

To retrieve historical weather data:

1. Go to the Airflow web interface
2. Locate the `accuweather_backfill` DAG
3. Click "Trigger DAG w/ config"
4. Enter your desired date range in JSON format:
```json
{
  "start_date": "2023-01-01",
  "end_date": "2023-01-31"
}
```
5. Click "Trigger" to start the backfill process

## Email Notifications

This pipeline includes comprehensive email notifications:

- **Failure Alerts**: Detailed notifications when any task fails
- **API Error Reports**: Specific information about API-related issues
- **Database Error Alerts**: Details about database connection or query problems
- **Success Confirmations**: Optional notifications when the pipeline completes successfully

To configure email notifications:

1. Ensure your `.env` file contains the correct SMTP settings
2. For Gmail users, you'll need to generate an App Password:
   - Go to your Google Account → Security
   - Enable 2-Step Verification if not already enabled
   - Go to App Passwords and generate a password for the application

## Troubleshooting

### API Issues
- **Error**: "API key not found" or "Invalid API key"
  - **Solution**: Verify your AccuWeather API key in the `.env` file
  
- **Error**: "API request failed with status code 429"
  - **Solution**: You've exceeded AccuWeather's API limits. Wait or upgrade your plan

### Database Issues
- **Error**: "Could not connect to database"
  - **Solution**: Check PostgreSQL is running and credentials in `.env` are correct

- **Error**: "Relation does not exist"
  - **Solution**: Ensure you've run the database setup script

### Airflow Issues
- **Error**: "DAG not showing in Airflow UI"
  - **Solution**: Check file permissions and verify Python syntax

- **Error**: "Task instance failed"
  - **Solution**: Check Airflow logs and email notifications for details

## Extending the Project

Ways to enhance this project:

- Add data visualization dashboards
- Include additional weather data sources
- Implement data quality checks
- Create alerts for extreme weather conditions
- Add geographic expansion to track multiple locations

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.