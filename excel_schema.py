import os
import pandas as pd
import json


def pandas_dtype_to_sql(dtype) -> str:
    """
    Map a pandas dtype to a SQL-equivalent type.
    """
    if pd.api.types.is_integer_dtype(dtype):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    else:
        # fall back to text
        return 'VARCHAR'





def excel_schema_to_json(excel_path: str) -> dict:
    """
    Read all sheets from the given Excel file and build a JSON-serializable schema.

    Returns a dict with:
      - file_path: absolute path to the Excel file
      - tables: list of sheets, each a dict with 'name' and 'columns' list,
                where each column is a dict {'name': ..., 'type': ...}
    """
    abs_path = os.path.abspath(excel_path)

    # Load every sheet into a dict of DataFrames
    sheets = pd.read_excel(excel_path, sheet_name=None)

    tables = []
    for sheet_name, df in sheets.items():
        cols = []
        for col in df.columns:
            sql_type = pandas_dtype_to_sql(df[col].dtype)
            cols.append({
                'name': str(col),
                'type': sql_type
            })
        tables.append({
            'name': sheet_name,
            'columns': cols
        })

    return {
        'file_path': abs_path,
        'tables': tables
    }

# Your Excel file path
excel_path = r"C:\Users\ksaik\OneDrive\Desktop\MSCriptAutomation\MSCriptAutomation\combined_datasets.xlsx"

# Output JSON file path
output_json_path = r"C:\Users\ksaik\OneDrive\Desktop\MSCriptAutomation\MSCriptAutomation\excel_schema.json"

# Generate schema
schema = excel_schema_to_json(excel_path)

# Save to JSON file
with open(output_json_path, 'w', encoding='utf-8') as f:
    json.dump(schema, f, indent=4)

print(f"Schema saved to: {output_json_path}")
