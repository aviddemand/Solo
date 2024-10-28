import pandas as pd
from sqlalchemy import create_engine

# Database connection details
db_url = 'mysql+pymysql://mainlogan:Mrmojo69@35.222.252.227/payjunction'

# Create SQLAlchemy engine
engine = create_engine(db_url)

# Function to load data from MySQL tables
def load_data_from_db(table_name):
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, con=engine)

# Load data from the GSC and GAds tables
gsc_data = load_data_from_db('overall_gsc_data')
gads_data = load_data_from_db('overall_gads_data')

# Print the loaded data for debugging
print("GSC Data:")
print(gsc_data.head())
print("GAds Data:")
print(gads_data.head())

# Aggregate data by 'Month' to ensure each date has only one entry
gsc_data = gsc_data.groupby('Month', as_index=False)['Impressions'].sum()
gads_data = gads_data.groupby('Month', as_index=False)['Impressions'].sum()

# Merge the data based on the 'Month' column
merged_data = pd.merge(gsc_data, gads_data, on='Month', suffixes=('_seo', '_sem'))

# Rename columns to match the target table's structure
merged_data.rename(columns={
    'Month': 'Month',  # Keep as Month
    'Impressions_seo': 'seo_impr',
    'Impressions_sem': 'sem_impr'
}, inplace=True)

# Print the merged data for verification
print("Merged Data:")
print(merged_data)

# Insert merged data into the 'overall_gsc_gads_data' table
try:
    with engine.begin() as connection:
        merged_data.to_sql('overall_gsc_gads_data', con=connection, if_exists='append', index=False)
    print("Data successfully inserted into 'overall_gsc_gads_data'.")
except Exception as e:
    print(f"An error occurred: {e}")

print("All done")
