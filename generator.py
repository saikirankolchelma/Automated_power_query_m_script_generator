import os
import json
import google.generativeai as genai

# --- CONFIGURATION ---
api_key        = "AIzaSyAPT0bK2YHGpZhbS1cmOkKholQiPYo152M"
model_name     = "models/gemini-1.5-flash"
flow_json_path = r"C:\Users\ksaik\OneDrive\Desktop\MSCriptAutomation\New_Dashboard_Parameter_Updated.json"
excel_path     = r"C:\Users\ksaik\OneDrive\Desktop\MSCriptAutomation\MSCriptAutomation\combined_datasets.xlsx"
schema_path    = r"C:\Users\ksaik\OneDrive\Desktop\MSCriptAutomation\excel_schema.json"
output_dir     = r"C:\Users\ksaik\OneDrive\Desktop\MSCriptAutomation\multi_scripts"

# --- INIT GEMINI ---
genai.configure(api_key=api_key)
model = genai.GenerativeModel(model_name)

# --- LOAD JSON CONFIGS ---
def load_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Failed to load JSON file: {path}\nError: {e}")
        exit(1)

schema = load_json(schema_path)
flow   = load_json(flow_json_path)

# --- ESCAPE JSON FOR M-SCRIPT SAFETY ---
def clean_json(data):
    return json.dumps(data, indent=2).replace("\\", "\\\\")

# Escape backslashes in the Excel file path
escaped_excel_path = excel_path.replace("\\", "\\\\")

# --- CREATE OUTPUT DIRECTORY ---
os.makedirs(output_dir, exist_ok=True)

# --- GENERATE INDIVIDUAL M SCRIPTS FOR EACH TABLE ---
for table in schema["tables"]:
    table_name = table["name"]
    columns_json = clean_json(table["columns"])

    prompt = f"""
You are an expert Power Query M developer.

Generate a Power Query M script for:
- Sheet/Table name: "{table_name}"
- Excel path: File.Contents("{escaped_excel_path}")
- Columns: 
{columns_json}

### Requirements:
- Load the sheet using `Excel.Workbook(File.Contents(...))`.
- Filter rows to get only the relevant sheet (Name = "{table_name}").
- Promote headers.
- Apply Table.TransformColumnTypes with correct types.
- Name the final output variable: {table_name}_Table
- Wrap everything inside `let ... in` block.
- Output only the valid Power Query M code.
"""

    try:
        response = model.generate_content(prompt)
        m_script = response.text.strip()

        output_path = os.path.join(output_dir, f"{table_name}.pq")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(m_script)

        print(f"✅ Saved M script for '{table_name}' to {output_path}")

    except Exception as e:
        print(f"❌ Error generating script for {table_name}: {e}")
