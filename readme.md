# AccuWeather to PostgreSQL Data Pipeline

## Business Use Case
Automated weather data collection and storage for analytics, forecasting, and business intelligence. Common applications include retail planning, logistics optimization, energy demand forecasting, and operational decision-making based on weather patterns.

## Technical Implementation

### Architecture
- **Data Source**: AccuWeather API (historical weather data)
- **Orchestration**: Apache Airflow (scheduled daily execution)
- **Storage**: PostgreSQL with dedicated schema
- **Processing**: Python with pandas for data transformation
- **Monitoring**: Email notifications and comprehensive error handling

### Tech Stack Rationale
- **PostgreSQL**: Reliable, fast queries, excellent time-series support, widely known
- **Airflow**: Production-proven orchestration, easy monitoring, robust retry mechanisms
- **Python**: Rich ecosystem for API integration and data processing
- **Pandas**: Efficient data transformation and validation

## Features

### Production-Ready Pipeline
- **Daily automated execution** at 2 AM
- **Comprehensive error handling** with detailed email notifications
- **Retry logic** for transient failures
- **Data validation** at each pipeline stage
- **Backfill capability** for historical data gaps

### Data Quality & Monitoring
- **Schema validation** before database insertion
- **Temperature range checks** and data consistency validation
- **Real-time error notifications** with detailed context
- **Success confirmations** with pipeline metrics

### Enterprise Considerations
- **Environment-based configuration** using .env files
- **Database role separation** with appropriate permissions
- **Secure credential management** 
- **Audit trail** with timestamps and data lineage

## Database Schema

```sql
-- Main data table
weather_db_schema.historical_weather_data
├── id (SERIAL PRIMARY KEY)
├── observation_date (TIMESTAMP WITH TIME ZONE)
├── temperature (NUMERIC(5,2))
├── condition (VARCHAR(100))
├── fetch_date (DATE)
└── created_at (TIMESTAMP WITH TIME ZONE)

-- Analytics view
weather_db_schema.daily_temperature_view
├── date
├── min_temp, max_temp, avg_temp
└── observation_count
```

## Setup Instructions

### Prerequisites
- Apache Airflow environment
- PostgreSQL database
- AccuWeather API key
- Python packages: `requests`, `pandas`, `psycopg2`, `python-dotenv`

### Database Setup
```bash
# Run the database setup script as PostgreSQL superuser
psql -U postgres -f database_setup.sql
```

### Environment Configuration
Create `.env` file with:
```env
# AccuWeather API
ACCUWEATHER_API_KEY=your_api_key_here

# PostgreSQL Connection
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=weather_db
POSTGRES_USER=airflow_weather_user
POSTGRES_PASSWORD=secure_password_here

# Notifications
NOTIFICATION_EMAIL=your_email@company.com
```

### Airflow DAG Deployment
1. Copy `DAG_regular.py` to your Airflow DAGs folder
2. Copy `DAG_backfill.py` for historical data processing
3. Ensure `.env` file is accessible to Airflow workers
4. Configure Airflow email settings for notifications

## DAG Operations

### Regular Daily Pipeline (`accuweather_daily`)
- **Schedule**: Daily at 2:00 AM
- **Data Range**: Previous day's complete weather data
- **Tasks**: Extract → Transform → Load → Notify

### Backfill Pipeline (`accuweather_backfill`)
- **Schedule**: Manual trigger only
- **Purpose**: Fill historical data gaps
- **Configuration**: Date range parameters

## Why This Approach Works

### Reliability Over Complexity
- **Proven tools** that operate consistently across years
- **Simple architecture** that teams can understand and maintain
- **Comprehensive monitoring** to catch issues before they impact business

### Business Value Focus
- **Clean, analytics-ready data** in PostgreSQL
- **Consistent daily execution** for reliable reporting
- **Historical data preservation** for trend analysis
- **Easy integration** with BI tools and analytics platforms

### Production Lessons Learned
- **API rate limiting** handled gracefully with retries
- **Network timeouts** managed with appropriate error handling
- **Data validation** prevents bad data from corrupting analytics
- **Email notifications** ensure operations team awareness

## Scaling Considerations

### Current Capacity
- Handles daily weather data for single location
- Processes ~24 hourly observations per day
- Minimal infrastructure requirements

### Enterprise Scaling
- **Multiple locations**: Parameterize location keys
- **Higher frequency**: Adjust to hourly collection
- **Data retention**: Add archival and partitioning strategies
- **Monitoring**: Integrate with enterprise monitoring systems

## Integration Examples

### BI Dashboard Integration
```sql
-- Weekly temperature trends
SELECT 
    DATE_TRUNC('week', fetch_date) as week,
    AVG(temperature) as avg_weekly_temp
FROM weather_db_schema.historical_weather_data 
WHERE fetch_date >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY week
ORDER BY week;
```

### Operational Analytics
```sql
-- Temperature alerts for logistics planning
SELECT * FROM weather_db_schema.daily_temperature_view 
WHERE max_temp > 30 OR min_temp < -5
ORDER BY date DESC;
```

## Support & Maintenance

### Common Operations
- **Monitor pipeline health**: Check Airflow UI for task status
- **Review data quality**: Query daily_temperature_view for anomalies
- **Handle API issues**: Check email notifications for detailed error context

### Troubleshooting
- **API failures**: Verify API key and check AccuWeather service status
- **Database connections**: Confirm PostgreSQL credentials and network access
- **Data gaps**: Use backfill DAG with appropriate date parameters

---

**Production-ready data engineering using battle-tested tools.**  
*Demonstrates reliable pipeline patterns suitable for enterprise environments.*

## Contact
For questions about implementation patterns or scaling strategies, feel free to reach out.
