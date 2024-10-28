import os
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, FilterExpression, Filter
from sqlalchemy import create_engine
from google.oauth2.credentials import Credentials

# OAuth 2.0 Access Token
ACCESS_TOKEN = 'ya29.a0AcM612yynASDCs-ngUJqgapGm2vqNprXiXzPS9eyuKyB-N-67P_PuoC0IoL9eXnDn3S2S7jvnp6SjuIsNdw70Zw20C41BAYhAM6ZoM2wOE6IlwyDty0U454rsUtYuJoQV6r5ql74e4N1Iuw-d3anUszdWl4scEfErhxnrDMMaCgYKAWoSARESFQHGX2MidK9oEKEuli3cSXg3PqoW2Q0175'

# Create an instance of Credentials with the access token
credentials = Credentials(token=ACCESS_TOKEN)

# Initialize the BetaAnalyticsDataClient using the credentials
client = BetaAnalyticsDataClient(credentials=credentials)


# Define the property ID
property_id = '267191963'

# Define the date ranges (you can extend this list as needed)
date_ranges = [
    ('2023-10-01', '2024-09-30')
]

# Define the channel groupings to filter
channel_groupings = ["Organic Search", "Paid Search"]

# Define filter expression for channel groupings
channel_grouping_filter_expression = FilterExpression(
    filter=Filter(
        field_name="sessionDefaultChannelGroup",
        in_list_filter=Filter.InListFilter(
            values=channel_groupings
        )
    )
)

# Function to query and process API response
def query_data(api_response):
    dimension_headers = [header.name for header in api_response.dimension_headers]
    metric_headers = [header.name for header in api_response.metric_headers]
    data = []
    for row in api_response.rows:
        record = {header: row.dimension_values[i].value for i, header in enumerate(dimension_headers)}
        record.update({header: row.metric_values[i].value for i, header in enumerate(metric_headers)})
        data.append(record)
    return pd.DataFrame(data)

# Loop through each date range and make a request for each month's data
for starting_date, ending_date in date_ranges:
    request_api = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name="sessionDefaultChannelGroup")
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="conversions")
        ],
        date_ranges=[DateRange(start_date=starting_date, end_date=ending_date)],
        dimension_filter=channel_grouping_filter_expression
    )

    response = client.run_report(request_api)

    # Query data and process the response
    result_df = query_data(response)

    # Column mapping for Google Analytics fields to MySQL table columns
    column_mapping = {
        'date': 'Month',  # Use 'Month' to match the database schema
        'sessionDefaultChannelGroup': 'Channel',
        'sessions': 'Sessions',
        'conversions': 'Conversions'
    }

    # Rename columns based on column_mapping
    result_df.rename(columns=column_mapping, inplace=True)

    try:
        # Convert the 'Month' column to a proper datetime format
        result_df['Month'] = pd.to_datetime(result_df['Month'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

        # Create SQLAlchemy engine
        engine = create_engine('mysql+pymysql://mainlogan:Mrmojo69@35.222.252.227/solo')

        # Insert DataFrame data into MySQL table
        result_df.to_sql('overall_ga4_data', con=engine, if_exists='append', index=False)

        print('Data successfully inserted into MySQL.')

    except Exception as e:
        print(f"Error: {e}")

print('All done')
