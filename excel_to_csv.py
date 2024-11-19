import pandas as pd

def excel_to_csv(excel_file, sheet_name, csv_file):
    """
    Converts a single sheet from an Excel file into a CSV file.

    Parameters:
        excel_file (str): Path to the Excel file.
        sheet_name (str): Name of the sheet to convert.
        csv_file (str): Path to save the resulting CSV file.
    """
    try:
        # Read the specified sheet from the Excel file
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        
        # Save the DataFrame to a CSV file
        df.to_csv(csv_file, index=False)
        
        print(f"Sheet '{sheet_name}' has been successfully converted to '{csv_file}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
# Replace 'example.xlsx', 'Sheet1', and 'output.csv' with your file and sheet names
excel_to_csv('DPS Mapping_v1119.xlsx', 'TW', 'dps_FCB_account_mappings.csv')
excel_to_csv('DPS Mapping_v1119.xlsx', 'XYZ', 'dps_XYZ_account_mappings.csv')
excel_to_csv('DPS Mapping_v1119.xlsx', 'IM', 'dps_Infinity_account_mappings.csv')