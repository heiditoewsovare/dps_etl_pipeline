import os
import pandas as pd
import pyodbc


output_file_name = 'dps_query_map_test_4.csv'
mapping_file_db1 = 'dps_account_mappings/dps_FCB_account_mappings.csv'
mapping_file_db2 = 'dps_account_mappings/dps_Infinity_account_mappings.csv'
mapping_file_db3 = 'dps_account_mappings/dps_XYZ_account_mappings.csv'


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


# 0: office_name, 1: office_code, 2: investment_id
investment = {
    'Advertys_FCB': ['FCB', '81', 1081,],
    'Advertys_Infinity': ['Infinity', '82', 1082,],
    'Advertys_XYZ': ['XYZ', '83', 1083,],
}

query = f"""SELECT TOP(2000)
    '{DATABASE1}' as [SOURCE_DATA],
    'dps' as [SOURCE_SYSTEM],
    [CURRENCY] = mon.Nombre,
    '{investment[DATABASE1][2]}' as [INVESTMENT_ID],
    '{investment[DATABASE1][0]}' as [OFFICE_NAME],
    '{investment[DATABASE1][1]} - {investment[DATABASE1][0]}' as [OFFICE],
    [TRANSACTION_ID] = imp.IdImputacion,
    [DESCRIPTION] = imp.Leyenda,
    [REFERENCE] = asi.LeyendaGeneral,
    [TRANSACTION_DATE] = asi.Fecha,
    1 as [SEQUENCE_NUMBER],
    [CREATED_BY_fn] = emp.Nombre,
    [CREATED_BY_ln] = emp.Apellido,
    [CREATED_BY_id] = emp.IdEmpleado,
    [CREDIT] = CASE WHEN imp.Importe < 0 THEN imp.Importe ELSE 0 END,
    [DEBIT] = CASE WHEN imp.Importe > 0 THEN imp.Importe ELSE 0 END,
    [TOTAL] = imp.Importe,
    [Client] = cli.Nombre,
    [Supplier] = pro.Nombre,
    [Cuenta] = cue.Cuenta,
    [Nombre] = cue.Nombre,
    [Clase] = cla.Nombre,
    [SubClase] = subcla.Nombre,
    [Rubro] = ru.Nombre,
    [SubRubro] = subru.Nombre
FROM 
    [{DATABASE1}].[dbo].[Imputacion] as imp 
    INNER JOIN [{DATABASE1}].[dbo].[Asiento] as asi ON imp.Asiento = asi.IdAsiento
    LEFT JOIN [{DATABASE1}].[dbo].[Moneda] as mon ON asi.Moneda = mon.IdMoneda
    LEFT JOIN [{DATABASE1}].[dbo].[Empleado] as emp on asi.UsuarioAlta = emp.IdEmpleado
    LEFT JOIN [{DATABASE1}].[dbo].[Cliente] as cli on imp.Cliente = cli.IdCliente
    LEFT JOIN [{DATABASE1}].[dbo].[Proveedor] as pro on imp.Proveedor = pro.IdProveedor
    LEFT JOIN [{DATABASE1}].[dbo].[PlanCuenta] as cue on imp.Cuenta = cue.Cuenta
    LEFT JOIN [{DATABASE1}].[dbo].[Clase] as cla on cue.Clase = cla.IdClase
    LEFT JOIN [{DATABASE1}].[dbo].[SubClase] as subcla on cue.SubClase = subcla.IdSubClase
    LEFT JOIN [{DATABASE1}].[dbo].[Rubro] as ru on cue.Rubro = ru.IdRubro
    LEFT JOIN [{DATABASE1}].[dbo].[SubRubro] as subru on cue.SubRubro = subru.IdSubRubro
UNION ALL
SELECT TOP(2000)
    '{DATABASE2}' as [SOURCE_DATA],
    'dps' as [SOURCE_SYSTEM],
    [CURRENCY] = mon.Nombre,
    '{investment[DATABASE2][2]}' as [INVESTMENT_ID],
    '{investment[DATABASE2][0]}' as [OFFICE_NAME],
    '{investment[DATABASE2][1]} - {investment[DATABASE2][0]}' as [OFFICE],
    [TRANSACTION_ID] = imp.IdImputacion,
    [DESCRIPTION] = imp.Leyenda,
    [REFERENCE] = asi.LeyendaGeneral,
    [TRANSACTION_DATE] = asi.Fecha,
    1 as [SEQUENCE_NUMBER],
    [CREATED_BY_fn] = emp.Nombre,
    [CREATED_BY_ln] = emp.Apellido,
    [CREATED_BY_id] = emp.IdEmpleado,
    [CREDIT] = CASE WHEN imp.Importe < 0 THEN imp.Importe ELSE 0 END,
    [DEBIT] = CASE WHEN imp.Importe > 0 THEN imp.Importe ELSE 0 END,
    [TOTAL] = imp.Importe,
    [Client] = cli.Nombre,
    [Supplier] = pro.Nombre,
    [Cuenta] = cue.Cuenta,
    [Nombre] = cue.Nombre,
    [Clase] = cla.Nombre,
    [SubClase] = subcla.Nombre,
    [Rubro] = ru.Nombre,
    [SubRubro] = subru.Nombre
FROM 
    [{DATABASE2}].[dbo].[Imputacion] as imp 
    INNER JOIN [{DATABASE2}].[dbo].[Asiento] as asi ON imp.Asiento = asi.IdAsiento
    LEFT JOIN [{DATABASE2}].[dbo].[Moneda] as mon ON asi.Moneda = mon.IdMoneda
    LEFT JOIN [{DATABASE2}].[dbo].[Empleado] as emp on asi.UsuarioAlta = emp.IdEmpleado
    LEFT JOIN [{DATABASE2}].[dbo].[Cliente] as cli on imp.Cliente = cli.IdCliente
    LEFT JOIN [{DATABASE2}].[dbo].[Proveedor] as pro on imp.Proveedor = pro.IdProveedor
    LEFT JOIN [{DATABASE2}].[dbo].[PlanCuenta] as cue on imp.Cuenta = cue.Cuenta
    LEFT JOIN [{DATABASE2}].[dbo].[Clase] as cla on cue.Clase = cla.IdClase
    LEFT JOIN [{DATABASE2}].[dbo].[SubClase] as subcla on cue.SubClase = subcla.IdSubClase
    LEFT JOIN [{DATABASE2}].[dbo].[Rubro] as ru on cue.Rubro = ru.IdRubro
    LEFT JOIN [{DATABASE2}].[dbo].[SubRubro] as subru on cue.SubRubro = subru.IdSubRubro
UNION ALL
SELECT TOP(2000)
    '{DATABASE3}' as [SOURCE_DATA],
    'dps' as [SOURCE_SYSTEM],
    [CURRENCY] = mon.Nombre,
    '{investment[DATABASE3][2]}' as [INVESTMENT_ID],
    '{investment[DATABASE3][0]}' as [OFFICE_NAME],
    '{investment[DATABASE3][1]} - {investment[DATABASE3][0]}' as [OFFICE],
    [TRANSACTION_ID] = imp.IdImputacion,
    [DESCRIPTION] = imp.Leyenda,
    [REFERENCE] = asi.LeyendaGeneral,
    [TRANSACTION_DATE] = asi.Fecha,
    1 as [SEQUENCE_NUMBER],
    [CREATED_BY_fn] = emp.Nombre,
    [CREATED_BY_ln] = emp.Apellido,
    [CREATED_BY_id] = emp.IdEmpleado,
    [CREDIT] = CASE WHEN imp.Importe < 0 THEN imp.Importe ELSE 0 END,
    [DEBIT] = CASE WHEN imp.Importe > 0 THEN imp.Importe ELSE 0 END,
    [TOTAL] = imp.Importe,
    [Client] = cli.Nombre,
    [Supplier] = pro.Nombre,
    [Cuenta] = cue.Cuenta,
    [Nombre] = cue.Nombre,
    [Clase] = cla.Nombre,
    [SubClase] = subcla.Nombre,
    [Rubro] = ru.Nombre,
    [SubRubro] = subru.Nombre
FROM 
    [{DATABASE3}].[dbo].[Imputacion] as imp 
    INNER JOIN [{DATABASE3}].[dbo].[Asiento] as asi ON imp.Asiento = asi.IdAsiento
    LEFT JOIN [{DATABASE3}].[dbo].[Moneda] as mon ON asi.Moneda = mon.IdMoneda
    LEFT JOIN [{DATABASE3}].[dbo].[Empleado] as emp on asi.UsuarioAlta = emp.IdEmpleado
    LEFT JOIN [{DATABASE3}].[dbo].[Cliente] as cli on imp.Cliente = cli.IdCliente
    LEFT JOIN [{DATABASE3}].[dbo].[Proveedor] as pro on imp.Proveedor = pro.IdProveedor
    LEFT JOIN [{DATABASE3}].[dbo].[PlanCuenta] as cue on imp.Cuenta = cue.Cuenta
    LEFT JOIN [{DATABASE3}].[dbo].[Clase] as cla on cue.Clase = cla.IdClase
    LEFT JOIN [{DATABASE3}].[dbo].[SubClase] as subcla on cue.SubClase = subcla.IdSubClase
    LEFT JOIN [{DATABASE3}].[dbo].[Rubro] as ru on cue.Rubro = ru.IdRubro
    LEFT JOIN [{DATABASE3}].[dbo].[SubRubro] as subru on cue.SubRubro = subru.IdSubRubro
"""

# connect to database
cursor = conn.cursor()
try:
    cursor.execute(query)
except Exception as e:
    print(e)
    exit()

# get data
rows = []
while True:
    row = cursor.fetchone()
    if row is None:
        break
    rows.append(row)

# convert to dataframe
columns = [column[0] for column in cursor.description]
df = pd.DataFrame.from_records(rows, columns=columns)

# create GL_ID
df['GL_ID'] = df['SOURCE_DATA'] + "-" + df['TRANSACTION_ID'].astype(str) + "-" + df['SEQUENCE_NUMBER'].astype(str)

# create CREATED_BY
df['CREATED_BY'] = (df['CREATED_BY_fn'].str[:2] + df['CREATED_BY_ln'].str[:2]).str.upper() + "-" + df['CREATED_BY_id'].astype(str)
df = df.drop(columns=['CREATED_BY_fn', 'CREATED_BY_ln', 'CREATED_BY_id'])

# create POST_PERIOD_CODE
df['POST_PERIOD_CODE'] = df['TRANSACTION_DATE'].dt.year * 100 + df['TRANSACTION_DATE'].dt.month


db_list = [DATABASE1, DATABASE2, DATABASE3]
map_file_list = [mapping_file_db1, mapping_file_db2, mapping_file_db3]
df_list = []

for i in range(0, 3):
    db_name = db_list[i]
    map_file = map_file_list[i]

    df_db = df[df['SOURCE_DATA'] == db_name].copy()
    map_file_df = pd.read_csv(map_file)
    map_file_df = map_file_df.astype(object)

    df_db = pd.merge(
        df_db,
        map_file_df,
        on=['Clase', 'SubClase', 'Rubro', 'SubRubro', 'Cuenta', 'Nombre'],
        how='left'
    )

    df_list.append(df_db)

df = pd.concat(df_list, ignore_index=True)


# create GLA_DESCRIPTION
# mapping_df_db1 = pd.read_csv(mapping_file_db1)
# mapping_df_db1['SOURCE_DATA'] = DATABASE1
# mapping_df_db1 = mapping_df_db1.astype(object)
# df = pd.merge(
#     df,
#     mapping_df_db1,
#     on=['SOURCE_DATA', 'Clase', 'SubClase', 'Rubro', 'SubRubro', 'Cuenta', 'Nombre'],
#     how='left'
# )

# mapping_df_db2 = pd.read_csv(mapping_file_db2)
# mapping_df_db2['SOURCE_DATA'] = DATABASE2
# mapping_df_db2 = mapping_df_db2.astype(object)
# df = pd.merge(
#     df,
#     mapping_df_db2,
#     on=['SOURCE_DATA', 'Clase', 'SubClase', 'Rubro', 'SubRubro', 'Cuenta', 'Nombre'],
#     how='left'
# )

# mapping_df_db3 = pd.read_csv(mapping_file_db3)
# mapping_df_db3['SOURCE_DATA'] = DATABASE3
# mapping_df_db3 = mapping_df_db3.astype(object)
# df = pd.merge(
#     df,
#     mapping_df_db3,
#     on=['SOURCE_DATA', 'Clase', 'SubClase', 'Rubro', 'SubRubro', 'Cuenta', 'Nombre'],
#     how='left'
# )

print(df)

df.to_csv(output_file_name, index=False)

# Close the cursor and connection
cursor.close()
conn.close()