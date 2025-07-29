📘 M Script Automation using LLM-Powered Workflow
This project automates the generation of Power Query M scripts for Power BI by leveraging structured metadata and a large language model (LLM). It streamlines Excel-based transformation tasks and eliminates the need for writing manual M code.

✨ Key Features
✅ Excel Schema Extraction — Sheet names, column names, and data types

✅ Business Logic Parsing — Joins, filters, and rename operations extracted from a JSON-based TFL (Transformation Flow Logic)

✅ LLM-Based Script Generation — Automates M script creation using schema + flow logic

🏗️ Project Structure
graphql

          ├── parser_tfl.py                     # Parses transformation rules from the TFL JSON file
          ├── excel_schema.py                   # Extracts sheet names and column data types from the Excel file
          ├── generator.py                      # Generates M script using schema and logic via LLM
          ├── requirements.txt                  # Python dependencies
          ├── New_Dashboard_Parameter_Updated.json  # Input: TFL file with transformation logic
          ├── flow.json                         # Output: Cleaned flow extracted from TFL
          ├── excel_schema.json                 # Output: Excel schema
          ├── combined_datasets.xlsx           # Input: Excel file with multiple sheets
          └── output.pq                         # Final Power Query M script ready for Power BI
🔧 Setup Instructions
1. 📦 Install Dependencies
Make sure Python 3.7+ is installed, then run:

bash
Copy
Edit
pip install -r requirements.txt
🚀 How to Use
Step 1: Extract Transformation Logic
Parse business rules like joins, filters, and renaming operations from the TFL file.

bash
Copy
Edit
python parser_tfl.py
Input: New_Dashboard_Parameter_Updated.json

✅ Output: flow.json

Step 2: Extract Excel Schema
Analyze the Excel file and extract schema metadata including:

Sheet names

Column names

Column data types

bash
Copy
Edit
python excel_schema.py
Input: combined_datasets.xlsx

✅ Output: excel_schema.json

Step 3: Generate Power Query M Script
This script integrates the Excel schema and transformation flow logic, then uses an LLM to generate a valid Power Query script.

bash
Copy
Edit
python generator.py
Inputs:

flow.json

excel_schema.json

✅ Output: output.pq — ready to paste in Power BI Advanced Editor

🔐 LLM API Key Setup
The generator requires access to an LLM. Set your API key inside generator.py:

python
Copy
Edit
api_key = "YOUR_API_KEY_HERE"
🔗 You can use any modern LLM provider that supports structured input (e.g., OpenAI, Gemini, Claude, etc.).

📝 Notes
File paths can be adjusted as per your directory setup.

You can rerun the same workflow for different Excel + TFL pairs.

Generated .pq script is clean, optimized, and compatible with Power BI.

Automatically handles:

Join conditions

Null value processing

Data type conversions

🛠️ Example Use Case
Imagine you’re given:

An Excel file with multiple sheets

A JSON file describing how to join, filter, and transform them

Without writing a single line of M code, this tool will:

Parse your transformation logic

Understand the Excel structure

Generate a ready-to-use Power Query M script in seconds ⚡

