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

data_file = 'dps_query_map_test_9.csv'
df = pd.read_csv(data_file)


table_name = '[ovaregroup-etl-uat].[AzureMasterData].[GENERAL_LEDGER_TEST]'

# *** DROP COLUMNS NOT IN GL TABLE ***
df = df.drop(columns=['Cuenta', 'Nombre', 'Clase', 'SubClase', 'Rubro', 'SubRubro', 'Client', 'Supplier'])

# df = df.fillna(value='NULL')
for col in df.select_dtypes(include=['float']).columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')  # Ensure numeric types

# Loop through each row in the DataFrame
for index, row in df.iterrows():
    # Dynamically generate the column names and placeholders
    columns = ", ".join(row.index)  # Join column names with commas
    placeholders = ", ".join(["?"] * len(row))  # Create placeholders for each column
    
    # Build the SQL INSERT statement
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    # Convert row values to a list and ensure all are Python native types
    row_values = [None if pd.isna(value) else float(value) if isinstance(value, float) else value for value in row]

    # Execute the SQL command
    cursor = conn.cursor()
    cursor.execute(sql, row_values)
    cursor.commit()

# Close the cursor and connection after inserting
cursor.close()
print("Data inserted successfully.")