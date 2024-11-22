import pandas as pd


data_file = 'dps_query_map_test_9.csv'
output_file_name = 'dps_desc_not_mapped_4.csv'


df = pd.read_csv(data_file)

print(df['GLA_DESCRIPTION'].unique())

empty_rows = df[df['GLA_DESCRIPTION'].isna() | df['GLA_CODE'].isna()]

print(empty_rows)

empty_rows.to_csv(output_file_name, index=False)