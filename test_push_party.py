import os
import pandas as pd
import pyodbc


SERVER = os.getenv('DW_DATABASE_SERVER', 'DefaultKeyIfNotSet')
DATABASE = os.getenv('DW_DATABASE_NAME', 'DefaultKeyIfNotSet')
USERNAME = os.getenv('DW_DATABASE_USERNAME', 'DefaultKeyIfNotSet')
PASSWORD = os.getenv('DW_DATABASE_PASSWORD', 'DefaultKeyIfNotSet')


dsn="DRIVER={SQL SERVER};server=" + f"{SERVER};database={DATABASE};uid={USERNAME};pwd={PASSWORD}"

try:
    conn = pyodbc.connect(dsn)
    conn.autocommit = False
except Exception as e:
    print(e)
    print(f'Connection failed.')
    exit()

print(f'Connection succeeded.')

data_file_vendor = 'dps_query_vendor_2.csv'
data_file_client = 'dps_query_client_7.csv'

df_vendor = pd.read_csv(data_file_vendor)
df_client = pd.read_csv(data_file_client)

df = pd.concat([df_vendor, df_client], ignore_index=True)

# truncate columns
df['PARTY_NAME'] = df['PARTY_NAME'].str[:40]
df['PARTY_ADDRESS1'] = df['PARTY_ADDRESS1'].str[:40]
df['PARTY_CITY'] = df['PARTY_CITY'].str[:30]
df['PARTY_STATE'] = df['PARTY_STATE'].str[:10]
df['PARTY_COUNTRY'] = df['PARTY_COUNTRY'].str[:20]


table_name = '[ovaregroup-etl-uat].[AzureMasterData].[PARTY_TEST]'


for col in df.select_dtypes(include=['float']).columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Loop through each row in the DataFrame
for index, row in df.iterrows():
    # Dynamically generate the column names and placeholders
    columns = ", ".join(row.index)  # Join column names with commas
    placeholders = ", ".join(["?"] * len(row))  # Create placeholders for each column
    
    # Build the SQL INSERT statement
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    # Convert row values to a list and ensure all are Python native types
    row_values = [None if pd.isna(value) else float(value) if isinstance(value, float) else value for value in row]

    cursor = conn.cursor()
    cursor.execute(sql, row_values)
    cursor.commit()

# Close the cursor and connection after inserting
cursor.close()
print("Data inserted successfully.")