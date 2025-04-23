import pandas as pd
import os
import json

def infer_dtype(series):
    if pd.api.types.is_integer_dtype(series):
        return "Integer"
    elif pd.api.types.is_float_dtype(series):
        return "Decimal"
    elif pd.api.types.is_bool_dtype(series):
        return "Boolean"
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "DateTime"
    elif pd.api.types.is_string_dtype(series):
        return "Text"
    else:
        return "Unknown"

def detect_column_types(file_path):
    _, ext = os.path.splitext(file_path)
    ext = ext.lower()

    column_types = {}

    if ext == ".csv":
        df = pd.read_csv(file_path)
        column_types = {col: infer_dtype(df[col]) for col in df.columns}
    elif ext in [".xls", ".xlsx"]:
        xls = pd.ExcelFile(file_path)
        for sheet_name in xls.sheet_names:
            df = xls.parse(sheet_name)
            column_types[sheet_name] = {col: infer_dtype(df[col]) for col in df.columns}
    else:
        raise ValueError("Unsupported file type. Use .csv or .xlsx/.xls")

    return column_types

if __name__ == "__main__":
    file_path = r"C:\Users\ksaik\OneDrive\Desktop\MSCriptAutomation\MSCriptAutomation\combined_datasets.xlsx"  # Update your path here

    types = detect_column_types(file_path)

    # Save as JSON
    output_path = os.path.splitext(file_path)[0] + "_types.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(types, f, indent=4)

    print(f"✅ Data types saved to: {output_path}")
