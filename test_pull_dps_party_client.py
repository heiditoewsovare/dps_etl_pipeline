




import os
import pandas as pd
import pyodbc


SERVER = 'srv17.advertys.com.ar,13577'
USERNAME = "extbase"
PASSWORD = "2024Base@"

DATABASE1 = 'Advertys_FCB'
DATABASE2 = 'Advertys_Infinity'
DATABASE3 = 'Advertys_XYZ'

dsn="DRIVER={SQL SERVER};server=" + f"{SERVER};database={DATABASE1};uid={USERNAME};pwd={PASSWORD}"

try:
    conn = pyodbc.connect(dsn)
    conn.autocommit = False
except Exception as e:
    print(e)
    print(f'Connection failed.')
    exit()

print(f'Connection succeeded.')

# 0: office_name, 1: office_code, 2: investment_id
investment = {
    'Advertys_FCB': ['FCB', '81', 1081,],
    'Advertys_Infinity': ['Infinity', '82', 1082,],
    'Advertys_XYZ': ['XYZ', '83', 1083,],
}

query = f"""SELECT 
    '{DATABASE1}' as [SOURCE_DATA],
    'dps' as [SOURCE_SYSTEM],
    'CLIENT' as [PARTY_TYPE],
    '{investment[DATABASE1][2]}' as [INVESTMENT_ID],
    [CL_CODE] = cli.IdCliente,
    [MSTR_CREATE_TIME] = cli.FechaAlta,
    [ACTIVE_FLAG] = cli.Habilitado,
    [PARTY_NAME] = cli.Nombre,
    [PARTY_ADDRESS1] = dat.Direccion,
    [PARTY_CITY] = dat.Localidad,
    [PARTY_COUNTRY] = pai.Nombre,
    [PARTY_ZIP] = dat.CodigoPostal,
    [PARTY_FOOTER] = con.Nombre,
    [PARTY_CURRENCY] = mon.Nombre
FROM 
    [{DATABASE1}].[dbo].[Cliente] as cli
    LEFT JOIN [{DATABASE1}].[dbo].[DatosGenerales] as dat on cli.DatosGenerales = dat.IdDatosGenerales
    LEFT JOIN [{DATABASE1}].[dbo].[Pais] as pai on dat.Pais = pai.IdPais
    LEFT JOIN [{DATABASE1}].[dbo].[CondicionPago] as con on cli.CondicionPago = con.IdCondicionPago
    LEFT JOIN [{DATABASE1}].[dbo].[Moneda] as mon ON cli.Moneda = mon.IdMoneda
UNION ALL
SELECT 
    '{DATABASE2}' as [SOURCE_DATA],
    'dps' as [SOURCE_SYSTEM],
    'CLIENT' as [PARTY_TYPE],
    '{investment[DATABASE2][2]}' as [INVESTMENT_ID],
    [CL_CODE] = cli.IdCliente,
    [MSTR_CREATE_TIME] = cli.FechaAlta,
    [ACTIVE_FLAG] = cli.Habilitado,
    [PARTY_NAME] = cli.Nombre,
    [PARTY_ADDRESS1] = dat.Direccion,
    [PARTY_CITY] = dat.Localidad,
    [PARTY_COUNTRY] = pai.Nombre,
    [PARTY_ZIP] = dat.CodigoPostal,
    [PARTY_FOOTER] = con.Nombre,
    [PARTY_CURRENCY] = mon.Nombre
FROM 
    [{DATABASE2}].[dbo].[Cliente] as cli
    LEFT JOIN [{DATABASE2}].[dbo].[DatosGenerales] as dat on cli.DatosGenerales = dat.IdDatosGenerales
    LEFT JOIN [{DATABASE2}].[dbo].[Pais] as pai on dat.Pais = pai.IdPais
    LEFT JOIN [{DATABASE2}].[dbo].[CondicionPago] as con on cli.CondicionPago = con.IdCondicionPago
    LEFT JOIN [{DATABASE2}].[dbo].[Moneda] as mon ON cli.Moneda = mon.IdMoneda
UNION ALL
SELECT 
    '{DATABASE3}' as [SOURCE_DATA],
    'dps' as [SOURCE_SYSTEM],
    'CLIENT' as [PARTY_TYPE],
    '{investment[DATABASE3][2]}' as [INVESTMENT_ID],
    [CL_CODE] = cli.IdCliente,
    [MSTR_CREATE_TIME] = cli.FechaAlta,
    [ACTIVE_FLAG] = cli.Habilitado,
    [PARTY_NAME] = cli.Nombre,
    [PARTY_ADDRESS1] = dat.Direccion,
    [PARTY_CITY] = dat.Localidad,
    [PARTY_COUNTRY] = pai.Nombre,
    [PARTY_ZIP] = dat.CodigoPostal,
    [PARTY_FOOTER] = con.Nombre,
    [PARTY_CURRENCY] = mon.Nombre
FROM 
    [{DATABASE3}].[dbo].[Cliente] as cli
    LEFT JOIN [{DATABASE3}].[dbo].[DatosGenerales] as dat on cli.DatosGenerales = dat.IdDatosGenerales
    LEFT JOIN [{DATABASE3}].[dbo].[Pais] as pai on dat.Pais = pai.IdPais
    LEFT JOIN [{DATABASE3}].[dbo].[CondicionPago] as con on cli.CondicionPago = con.IdCondicionPago
    LEFT JOIN [{DATABASE3}].[dbo].[Moneda] as mon ON cli.Moneda = mon.IdMoneda
"""

cursor = conn.cursor()
try:
    cursor.execute(query)
except Exception as e:
    print(e)
    exit()


rows = []
while True:
    row = cursor.fetchone()
    if row is None:
        break
    rows.append(row)


# Extract column names
columns = [column[0] for column in cursor.description]

# Convert to pandas DataFrame
df = pd.DataFrame.from_records(rows, columns=columns)

df['PARTY_ID'] = df['SOURCE_DATA'] + "-" + df['INVESTMENT_ID'].astype(str) + "-" + df['CL_CODE'].astype(str) + "_" + df['PARTY_TYPE']

# df['CREATED_BY'] = (df['CREATED_BY_fn'].str[:2] + df['CREATED_BY_ln'].str[:2]).str.upper() + "-" + df['CREATED_BY_id'].astype(str)
df = df.drop(columns=['CL_CODE',])

print(df)

df.to_csv('dps_query_client.csv', index=False)

# Close the cursor and connection
cursor.close()
conn.close()