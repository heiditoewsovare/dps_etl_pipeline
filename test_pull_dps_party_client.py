import os
import pandas as pd
import pyodbc


mapping_file_state = 'location_abbreviations/dps_province_abbrev.csv'
mapping_file_country = 'location_abbreviations/dps_country_abbrev.csv'
investment_id_file = 'other data files/Field Mappings from DPS.xlsx'
investment_id_sheet = 'Investment IDs'

SERVER = os.getenv('DPS_DATABASE_SERVER', 'DefaultKeyIfNotSet')
USERNAME = os.getenv('DPS_DATABASE_USERNAME', 'DefaultKeyIfNotSet')
PASSWORD = os.getenv('DPS_DATABASE_PASSWORD', 'DefaultKeyIfNotSet')

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


investment_df = pd.read_excel(investment_id_file, sheet_name=investment_id_sheet)

query = f"""SELECT 
    '{DATABASE1}' as [SOURCE_DATA],
    'dps' as [SOURCE_SYSTEM],
    'CLIENT' as [PARTY_TYPE],
    '{investment_df.loc[investment_df['SOURCE_DATA'] == DATABASE1, 'INVESTMENT_ID'].iloc[0]}' as [INVESTMENT_ID],
    [CL_CODE] = cli.IdCliente,
    [MSTR_CREATE_TIME] = cli.FechaAlta,
    [ACTIVE_FLAG] = cli.Habilitado,
    [PARTY_NAME] = cli.Nombre,
    [PARTY_ADDRESS1] = dat.Direccion,
    [PARTY_CITY] = dat.Localidad,
    [Provincia] = pro.Nombre,
    [Pais] = pai.Nombre,
    [PARTY_ZIP] = dat.CodigoPostal,
    [PARTY_FOOTER] = con.Nombre,
    [PARTY_CURRENCY] = mon.Nombre
FROM 
    [{DATABASE1}].[dbo].[Cliente] as cli
    LEFT JOIN [{DATABASE1}].[dbo].[DatosGenerales] as dat on cli.DatosGenerales = dat.IdDatosGenerales
    LEFT JOIN [{DATABASE1}].[dbo].[Provincia] as pro on dat.Provincia = pro.IdProvincia
    LEFT JOIN [{DATABASE1}].[dbo].[Pais] as pai on dat.Pais = pai.IdPais
    LEFT JOIN [{DATABASE1}].[dbo].[CondicionPago] as con on cli.CondicionPago = con.IdCondicionPago
    LEFT JOIN [{DATABASE1}].[dbo].[Moneda] as mon ON cli.Moneda = mon.IdMoneda
UNION ALL
SELECT 
    '{DATABASE2}' as [SOURCE_DATA],
    'dps' as [SOURCE_SYSTEM],
    'CLIENT' as [PARTY_TYPE],
    '{investment_df.loc[investment_df['SOURCE_DATA'] == DATABASE2, 'INVESTMENT_ID'].iloc[0]}' as [INVESTMENT_ID],
    [CL_CODE] = cli.IdCliente,
    [MSTR_CREATE_TIME] = cli.FechaAlta,
    [ACTIVE_FLAG] = cli.Habilitado,
    [PARTY_NAME] = cli.Nombre,
    [PARTY_ADDRESS1] = dat.Direccion,
    [PARTY_CITY] = dat.Localidad,
    [Provincia] = pro.Nombre,
    [Pais] = pai.Nombre,
    [PARTY_ZIP] = dat.CodigoPostal,
    [PARTY_FOOTER] = con.Nombre,
    [PARTY_CURRENCY] = mon.Nombre
FROM 
    [{DATABASE2}].[dbo].[Cliente] as cli
    LEFT JOIN [{DATABASE2}].[dbo].[DatosGenerales] as dat on cli.DatosGenerales = dat.IdDatosGenerales
    LEFT JOIN [{DATABASE2}].[dbo].[Provincia] as pro on dat.Provincia = pro.IdProvincia
    LEFT JOIN [{DATABASE2}].[dbo].[Pais] as pai on dat.Pais = pai.IdPais
    LEFT JOIN [{DATABASE2}].[dbo].[CondicionPago] as con on cli.CondicionPago = con.IdCondicionPago
    LEFT JOIN [{DATABASE2}].[dbo].[Moneda] as mon ON cli.Moneda = mon.IdMoneda
UNION ALL
SELECT 
    '{DATABASE3}' as [SOURCE_DATA],
    'dps' as [SOURCE_SYSTEM],
    'CLIENT' as [PARTY_TYPE],
    '{investment_df.loc[investment_df['SOURCE_DATA'] == DATABASE3, 'INVESTMENT_ID'].iloc[0]}' as [INVESTMENT_ID],
    [CL_CODE] = cli.IdCliente,
    [MSTR_CREATE_TIME] = cli.FechaAlta,
    [ACTIVE_FLAG] = cli.Habilitado,
    [PARTY_NAME] = cli.Nombre,
    [PARTY_ADDRESS1] = dat.Direccion,
    [PARTY_CITY] = dat.Localidad,
    [Provincia] = pro.Nombre,
    [Pais] = pai.Nombre,
    [PARTY_ZIP] = dat.CodigoPostal,
    [PARTY_FOOTER] = con.Nombre,
    [PARTY_CURRENCY] = mon.Nombre
FROM 
    [{DATABASE3}].[dbo].[Cliente] as cli
    LEFT JOIN [{DATABASE3}].[dbo].[DatosGenerales] as dat on cli.DatosGenerales = dat.IdDatosGenerales
    LEFT JOIN [{DATABASE3}].[dbo].[Provincia] as pro on dat.Provincia = pro.IdProvincia
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

mapping_df_state = pd.read_csv(mapping_file_state, keep_default_na=False)
mapping_df_state = mapping_df_state.astype(object)
mapping_df_state['Provincia'] = mapping_df_state['Provincia'].str.lower()
df['Provincia'] = df['Provincia'].str.lower()
df = pd.merge(
    df,
    mapping_df_state,
    on=['Provincia'],
    how='left'
)

mapping_df_country = pd.read_csv(mapping_file_country, keep_default_na=False)
mapping_df_country = mapping_df_country.astype(object)
mapping_df_country['Pais'] = mapping_df_country['Pais'].str.lower()
df['Pais'] = df['Pais'].str.lower()
df = pd.merge(
    df,
    mapping_df_country,
    on=['Pais'],
    how='left'
)

df = df.drop(columns=['CL_CODE', 'Provincia', 'Pais',])

print(df)

df.to_csv('dps_query_client_7.csv', index=False)

# Close the cursor and connection
cursor.close()
conn.close()