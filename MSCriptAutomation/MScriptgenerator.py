import json
import pandas as pd
import google.generativeai as genai
import time
import logging
from typing import Dict, List


# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configure Gemini API
genai.configure(api_key="AIzaSyBSABI4eV7diN_NW30bbi0YM4CrDWG1TLI")


def extract_excel_schema(excel_path: str) -> Dict:
    xls = pd.ExcelFile(excel_path)
    schema = {}
    for sheet in xls.sheet_names:
        df = xls.parse(sheet, nrows=10)
        schema[sheet] = {
            "columns": list(df.columns),
            "preview": df.head(2).to_dict(orient="records")
        }
    return schema


def extract_tfl_nodes(json_path: str) -> List[Dict]:
    with open(json_path, 'r', encoding='utf-8') as f:
        tfl = json.load(f)
    return [
        {
            "id": node.get("id"),
            "type": node.get("type"),
            "name": node.get("name"),
            "config": node.get("configuration", {})
        }
        for node in tfl.get("nodes", []) if isinstance(node, dict)
    ]


def generate_prompt(schema: Dict, tfl_nodes: List[Dict]) -> str:
    prompt_parts = ["### Task: Generate a Power Query M script from Excel & Tableau flow"]
    
    for sheet, meta in schema.items():
        prompt_parts.append(f"\n#### Sheet: {sheet}")
        prompt_parts.append(f"- Columns: {', '.join(meta['columns'])}")
        prompt_parts.append(f"- Sample rows:\n{json.dumps(meta['preview'], indent=2)}")
    
    prompt_parts.append("\n#### Tableau Flow Nodes")
    for node in tfl_nodes:
        prompt_parts.append(f"- [{node['type']}] {node['name']} -> {json.dumps(node['config'], indent=2)}")

    prompt_parts.append("""
#### Output Requirement
Generate a clean and production-ready Power Query M script:
- Use functions and let-in structure
- Include comments
- Apply joins, filters, renaming, null handling from Tableau flow
- Make it modular and readable
- Do not guess if logic is ambiguous — insert placeholder comment
""")

    return "\n".join(prompt_parts)


def get_llm_response(prompt: str, retries=3, delay=2) -> str:
    model = genai.GenerativeModel("gemini-1.5-pro")
    for attempt in range(retries):
        try:
            logging.info("Sending prompt to Gemini...")
            response = model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            logging.warning(f"LLM attempt {attempt+1} failed: {e}")
            time.sleep(delay)
    return None


def validate_m_script(script: str) -> bool:
    # You can integrate actual Power BI APIs or regex validation later.
    return "let" in script and "in" in script and "Table." in script


def convert_json_to_pq(json_path: str, excel_path: str, output_path: str):
    schema = extract_excel_schema(excel_path)
    tfl_nodes = extract_tfl_nodes(json_path)
    prompt = generate_prompt(schema, tfl_nodes)
    
    script = get_llm_response(prompt)
    if not script:
        logging.error("Script generation failed.")
        return
    
    if validate_m_script(script):
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(script)
        logging.info(f"Power Query M script written to {output_path}")
    else:
        logging.error("Generated M script appears invalid.")
        logging.debug(f"Script output:\n{script}")


if __name__ == "__main__":
    json_path = r"C:\Users\ksaik\OneDrive\Desktop\MSCriptAutomation\MSCriptAutomation\New_Dashboard_Parameter_Updated_extracted\flow"
    excel_path = r"C:\Users\ksaik\OneDrive\Desktop\MSCriptAutomation\MSCriptAutomation\combined_datasets.xlsx"
    output_path = r"C:\Users\ksaik\OneDrive\Desktop\MSCriptAutomation\MSCriptAutomation\output.pq"
    convert_json_to_pq(json_path, excel_path, output_path)
