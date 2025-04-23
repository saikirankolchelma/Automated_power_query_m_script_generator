import os
import pandas as pd
import json

def infer_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "Integer"
    elif pd.api.types.is_float_dtype(dtype):
        return "Decimal"
    elif pd.api.types.is_bool_dtype(dtype):
        return "Boolean"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DateTime"
    else:
        return "Text"

def extract_schema_from_dataframe(df, sheet_name=None):
    schema = {
        "name": sheet_name if sheet_name else "UnnamedSheet",
        "columns": []
    }
    for col in df.columns:
        col_type = infer_dtype(df[col].dtype)
        schema["columns"].append({
            "name": col,
            "type": col_type
        })
    return schema

def save_sheet_schema(sheet_schema, output_dir):
    filename = f"{sheet_schema['name']}_schema.json"
    output_path = os.path.join(output_dir, filename)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(sheet_schema, f, indent=4)
    print(f"✅ Saved schema for sheet '{sheet_schema['name']}': {output_path}")

def get_data_schema(file_path):
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        df = pd.read_csv(file_path)
        return [extract_schema_from_dataframe(df, "Main")]

    elif ext in [".xls", ".xlsx"]:
        xl = pd.ExcelFile(file_path)
        return [extract_schema_from_dataframe(xl.parse(sheet), sheet) for sheet in xl.sheet_names]

    elif ext == ".json":
        df = pd.read_json(file_path)
        return [extract_schema_from_dataframe(df, "Root")]

    else:
        raise ValueError("Unsupported file format: " + ext)

# --- MAIN ---
if __name__ == "__main__":
    file_path = r"C:\Users\ksaik\OneDrive\Desktop\MSCriptAutomation\MSCriptAutomation\combined_datasets.xlsx"
    output_dir = os.path.dirname(file_path)

    all_schemas = get_data_schema(file_path)

    for schema in all_schemas:
        save_sheet_schema(schema, output_dir)
