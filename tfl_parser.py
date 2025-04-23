import json
import zipfile
import os
import io
import logging

# --- Configure Logging ---
# Configure basic logging. The calling script can potentially override this.
# Using getLogger allows for more fine-grained control if needed later.
logger = logging.getLogger(__name__)
if not logger.hasHandlers(): # Avoid adding handlers multiple times if imported multiple times
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# --- Helper Functions ---
def _get_node_details(node_id, nodes_data):
    """Internal helper function to get node details."""
    if not node_id:
        return {"name": "Unknown Node (No ID)", "nodeType": "Unknown"}
    details = nodes_data.get(node_id)
    if not details:
        logger.warning(f"Node details not found for ID: {node_id}")
        return {"name": f"Unknown Node ({node_id})", "nodeType": "Unknown"}
    details.setdefault('name', f"Unnamed Node ({node_id})")
    return details

def _parse_nodes_recursive(nodes_dict, connections_dict, extracted_info, parent_container_id=None):
    """
    Internal recursive function to parse nodes.
    """
    if not isinstance(nodes_dict, dict):
        logger.warning(f"Expected a dictionary of nodes, but got {type(nodes_dict)}. Skipping this level.")
        return

    logger.debug(f"Entering _parse_nodes_recursive. Parent: {parent_container_id}. Nodes count: {len(nodes_dict)}")

    for node_id, node in nodes_dict.items():
        if not isinstance(node, dict):
            logger.warning(f"Skipping invalid node entry for ID {node_id} (Parent: {parent_container_id}). Expected dict, got {type(node)}.")
            continue

        node_name = node.get('name', f'Unnamed_{node_id[:8]}')
        node_type = node.get('nodeType', 'Unknown')
        base_type = node.get('baseType', 'unknown')
        logger.debug(f"Processing Node ID: {node_id}, Name: {node_name}, Type: {node_type}, BaseType: {base_type}, Parent: {parent_container_id}")

        # --- Store basic info and relationship ---
        if node_id not in extracted_info["nodes_summary"]:
             extracted_info["nodes_summary"][node_id] = {
                 "name": node_name,
                 "type": node_type,
                 "base_type": base_type,
                 "parent_container_id": parent_container_id
             }
        else:
             extracted_info["nodes_summary"][node_id]["parent_container_id"] = parent_container_id

        next_nodes_info = node.get('nextNodes', [])
        extracted_info["relationships"][node_id] = [
            nn.get('nextNodeId') for nn in next_nodes_info if nn.get('nextNodeId')
        ]
        logger.debug(f"  Node {node_id} connects to: {extracted_info['relationships'][node_id]}")


        # --- Extract details based on node type ---
        node_info_base = {
             "id": node_id,
             "name": node_name,
             "type": node_type,
             "parent_container_id": parent_container_id
        }

        # --- Specific Type Parsing (Inputs, Outputs, Joins, Unions, Cleaning Steps, Containers) ---
        # (Using the detailed parsing logic from the previous version)
        if base_type == 'input':
            logger.debug(f"  Identified as INPUT node: {node_id}")
            input_info = node_info_base.copy()
            conn_id = node.get('connectionId')
            if conn_id and conn_id in connections_dict:
                conn_details = connections_dict.get(conn_id, {})
                input_info["connection_name"] = conn_details.get('name')
                conn_attrs = conn_details.get('connectionAttributes', {})
                input_info["source_file"] = conn_attrs.get('filename')
                input_info["db_class"] = conn_attrs.get('class')
                input_info["server"] = conn_attrs.get('server')
                logger.debug(f"    Input Connection: {input_info.get('connection_name')}, File: {input_info.get('source_file')}")
            else:
                 if not parent_container_id: # Only warn for top-level inputs missing connection info
                     logger.warning(f"    Top-level Input node {node_id} has missing or invalid connectionId: {conn_id}")

            relation = node.get('relation', {})
            if relation.get('type') == 'table': input_info["table_or_sheet"] = relation.get('table')
            elif relation.get('type') == 'query': input_info["custom_sql"] = relation.get('query')
            extracted_info["inputs"].append(input_info)

        elif base_type == 'output':
            logger.debug(f"  Identified as OUTPUT node: {node_id}")
            output_info = node_info_base.copy()
            output_info["output_file"] = node.get('hyperOutputFile') or node.get('csvOutputFile') or node.get('xlsxOutputFile') or node.get('extractOutputFile')
            extracted_info["outputs"].append(output_info)

        elif 'Join' in node_type and node.get('actionNode'):
             logger.debug(f"  Identified as JOIN node: {node_id}")
             action_node = node.get('actionNode', {})
             join_info = node_info_base.copy()
             join_info.update({"join_type": action_node.get('joinType'), "conditions": action_node.get('conditions', []), "left_input_node_id": None, "right_input_node_id": None})
             extracted_info["joins"].append(join_info)

        elif 'Union' in node_type and node.get('actionNode'):
            logger.debug(f"  Identified as UNION node: {node_id}")
            union_info = node_info_base.copy()
            union_info["input_node_ids"] = []
            extracted_info["unions"].append(union_info)

        elif node_type.endswith('.RemoveColumns'):
            logger.debug(f"  Identified as RemoveColumns step: {node_id}")
            cleaning_step = node_info_base.copy()
            cleaning_step.update({"step_type": "RemoveColumns", "columns_removed": node.get('columnNames', [])})
            extracted_info["cleaning_steps"].append(cleaning_step)

        elif node_type.endswith('.RenameColumn'):
             logger.debug(f"  Identified as RenameColumn step: {node_id}")
             cleaning_step = node_info_base.copy()
             cleaning_step.update({"step_type": "RenameColumn", "original_name": node.get('columnName'), "new_name": node.get('rename')})
             extracted_info["cleaning_steps"].append(cleaning_step)

        elif node_type.endswith('.ChangeColumnType'):
             logger.debug(f"  Identified as ChangeColumnType step: {node_id}")
             cleaning_step = node_info_base.copy()
             # Extract changed fields info (can be complex)
             changed_fields = node.get('fields', {})
             details = []
             for col, change in changed_fields.items():
                 details.append(f"{col} -> {change.get('type', 'N/A')}" + (f" (Calc: {change.get('calc')})" if change.get('calc') else ""))
             cleaning_step.update({"step_type": "ChangeColumnType", "changes": details})
             extracted_info["cleaning_steps"].append(cleaning_step)

        elif node_type.endswith('.Filter') or 'Filter' in node_type:
             logger.debug(f"  Identified as FILTER step: {node_id} (Type: {node_type})")
             cleaning_step = node_info_base.copy()
             cleaning_step["step_type"] = "Filter"
             cleaning_step["filter_details"] = "Details not fully parsed"
             cleaning_step["column_name"] = node.get('columnName') or node.get('fieldId')
             # Add specific details for known filter types if needed
             if 'RichNullFilter' in node_type: cleaning_step["filter_details"] = f"Null Filter (Mode: {node.get('filter', '?')})"
             if 'RangeFilter' in node_type: cleaning_step["filter_details"] = f"Range Filter (Exclude: {node.get('exclude', '?')}, Ranges: {node.get('ranges', {})})"

             extracted_info["cleaning_steps"].append(cleaning_step)

        elif node_type.endswith('.Calculation') or 'Formula' in node_type:
             logger.debug(f"  Identified as CALCULATION step: {node_id} (Type: {node_type})")
             cleaning_step = node_info_base.copy()
             cleaning_step["step_type"] = "Calculation"
             action_node_calc = node.get('actionNode', {})
             cleaning_step["calculation_details"] = action_node_calc.get('calculation', node.get('formula', 'Details not parsed')) # Prioritize actionNode if available
             extracted_info["cleaning_steps"].append(cleaning_step)

        # --- Handle Containers Recursively ---
        elif base_type == 'container':
             logger.debug(f"  Identified as CONTAINER node: {node_id}. Processing children...")
             container_info = node_info_base.copy()
             extracted_info["containers"].append(container_info)
             loom_container = node.get('loomContainer')
             if isinstance(loom_container, dict):
                 nested_nodes = loom_container.get('nodes', {})
                 if nested_nodes:
                      _parse_nodes_recursive(nested_nodes, connections_dict, extracted_info, parent_container_id=node_id)
                 else: logger.debug(f"    Container {node_id} has no nested nodes.")
             else: logger.warning(f"    Container {node_id} is missing 'loomContainer' or it's not a dictionary.")

    logger.debug(f"Exiting _parse_nodes_recursive. Parent: {parent_container_id}.")


# --- Public Functions ---
def extract_prep_flow_info(file_path):
    """
    Parses a Tableau Prep flow file (assumed to be a ZIP archive named .tfl or .tflx)
    by extracting and parsing the internal 'flow' file, including nested containers.

    Args:
        file_path (str): Path to the .tfl or .tflx file (treated as ZIP).

    Returns:
        dict: A dictionary containing extracted flow information (inputs, outputs,
              joins, unions, cleaning_steps, containers, relationships, nodes_summary),
              or None if an error occurs during file reading or initial parsing.
              Returns an empty dict structure if parsing succeeds but finds nothing.
    """
    logger.info(f"Attempting to parse file (treating as ZIP): {file_path}")
    if not os.path.exists(file_path):
        logger.error(f"File not found at {file_path}")
        return None
    logger.info(f"File exists: {file_path}")

    flow_data = None
    file_successfully_processed = False

    try:
        logger.info("Attempting to open as ZIP archive...")
        with zipfile.ZipFile(file_path, 'r') as z:
            logger.info(f"Successfully opened ZIP archive. Contents: {z.namelist()}")
            if 'flow' in z.namelist():
                logger.info("Found required 'flow' entry within archive. Attempting to load JSON.")
                with z.open('flow') as f:
                    # (Encoding detection logic remains the same)
                    encodings_to_try = ['utf-8', 'cp1252', 'latin-1', 'utf-8-sig']
                    loaded_json = False
                    for enc in encodings_to_try:
                        logger.info(f"  Attempting to decode 'flow' entry with encoding: {enc}")
                        try:
                            f.seek(0)
                            wrapper = io.TextIOWrapper(f, encoding=enc)
                            flow_data = json.load(wrapper)
                            logger.info(f"  Successfully loaded JSON from 'flow' entry using encoding: {enc}")
                            loaded_json = True
                            file_successfully_processed = True
                            break
                        except UnicodeDecodeError: logger.warning(f"  Failed to decode 'flow' entry using {enc}. Trying next encoding.")
                        except json.JSONDecodeError as e_json:
                             logger.error(f"  Invalid JSON structure inside 'flow' entry (with encoding {enc}): {e_json}")
                             flow_data = None; loaded_json = False; break
                        except Exception as e_inner:
                             logger.error(f"  Error reading/parsing 'flow' entry from zip with encoding {enc}: {e_inner}", exc_info=True)
                             flow_data = None; loaded_json = False; break
                    if not loaded_json: logger.error("Could not load valid JSON from the 'flow' entry using any attempted encoding.")
            else: logger.error(f"File opened as ZIP, but required 'flow' entry not found inside {file_path}.")
    except zipfile.BadZipFile: logger.error(f"File '{file_path}' is not a valid ZIP archive."); return None
    except FileNotFoundError: logger.error(f"File not found: {file_path}"); return None # Should be caught earlier
    except Exception as e_zip: logger.error(f"An unexpected error occurred processing ZIP: {e_zip}", exc_info=True); return None

    if not file_successfully_processed or flow_data is None:
        logger.error("Failed to load valid flow data from the file.")
        return None

    logger.info("Flow data successfully loaded. Initializing extraction.")

    nodes = flow_data.get('nodes', {})
    connections = flow_data.get('connections', {})
    initial_node_ids = flow_data.get('initialNodes', [])
    logger.info(f"Found {len(nodes)} top-level nodes and {len(connections)} connections defined.")

    # Initialize the structure to hold results
    extracted_info = {
        "file_path": file_path, "inputs": [], "outputs": [], "joins": [], "unions": [],
        "cleaning_steps": [], "containers": [], "relationships": {}, "nodes_summary": {}
    }

    # --- Pass 1: Recursive Node Parsing ---
    logger.info("Starting Pass 1: Recursive node parsing...")
    _parse_nodes_recursive(nodes, connections, extracted_info) # Use the internal helper
    logger.info("Finished Pass 1.")

    # --- Pass 2: Resolve Join/Union Inputs ---
    logger.info("Starting Pass 2: Resolving Join/Union inputs...")
    nodes_feeding_into = {}
    for source_id, target_ids in extracted_info.get("relationships", {}).items():
        if target_ids is None: continue
        for target_id in target_ids:
            if target_id is None: continue
            if target_id not in nodes_feeding_into: nodes_feeding_into[target_id] = []
            if source_id not in nodes_feeding_into[target_id]: nodes_feeding_into[target_id].append(source_id)

    logger.debug(f"Built reverse lookup (nodes feeding into): {nodes_feeding_into}")

    for join in extracted_info.get("joins", []):
        join_id = join.get("id"); left_input, right_input = None, None
        if not join_id: continue
        inputs = nodes_feeding_into.get(join_id, [])
        logger.debug(f"Resolving inputs for Join {join_id} ({join.get('name')}). Found feeding nodes: {inputs}")
        if len(inputs) >= 1: left_input = inputs[0]
        if len(inputs) >= 2: right_input = inputs[1] # Simple assignment
        join["left_input_node_id"] = left_input
        join["right_input_node_id"] = right_input
        if len(inputs) < 2: logger.warning(f"Join {join_id} has {len(inputs)} inputs found. May be incomplete.")

    for union in extracted_info.get("unions", []):
        union_id = union.get("id")
        if not union_id: continue
        inputs = nodes_feeding_into.get(union_id, [])
        logger.debug(f"Resolving inputs for Union {union_id} ({union.get('name')}). Found feeding nodes: {inputs}")
        union["input_node_ids"] = inputs

    logger.info("Finished Pass 2.")
    logger.info(f"Finished processing all nodes for {file_path}. Returning extracted info.")
    return extracted_info

def print_flow_summary(info):
    """
    Prints a formatted summary of the extracted flow information dictionary.

    Args:
        info (dict): The dictionary returned by extract_prep_flow_info.
    """
    if not info or not isinstance(info, dict):
        print("Error: Invalid or empty info dictionary provided to print_flow_summary.")
        logger.warning("print_flow_summary called with invalid or empty info.")
        return

    logger.info(f"Generating summary for {info.get('file_path', 'Unknown File')}")
    print("\n" + "=" * 60)
    print(f"Flow Summary for: {info.get('file_path', 'Unknown File')}")
    print("=" * 60)

    all_nodes_summary = info.get('nodes_summary', {})

    def get_parent_container_name(parent_id):
        if not parent_id: return None
        parent_node_details = all_nodes_summary.get(parent_id)
        return parent_node_details.get('name', f'Unknown Container ({parent_id})') if parent_node_details else None

    # --- Print Inputs ---
    print("\n--- Inputs ---")
    inputs_list = info.get("inputs", [])
    if inputs_list:
        for i in inputs_list:
            parent_name = get_parent_container_name(i.get('parent_container_id'))
            print(f"  Node: {i.get('name', 'N/A')} ({i.get('id', 'N/A')})" + (f" [Inside: {parent_name}]" if parent_name else ""))
            print(f"    Type: {i.get('type', 'N/A')}")
            if i.get('source_file'): print(f"    Source File: {i['source_file']}")
            if i.get('table_or_sheet'): print(f"    Table/Sheet: {i['table_or_sheet']}")
            if i.get('custom_sql'): print(f"    Custom SQL: Yes")
            print(f"    Connection: {i.get('connection_name', 'N/A')}")
    else: print("  No inputs found.")

    # --- Print Joins ---
    print("\n--- Joins ---")
    joins_list = info.get("joins", [])
    if joins_list:
        for j in joins_list:
            parent_name = get_parent_container_name(j.get('parent_container_id'))
            left_node = _get_node_details(j.get('left_input_node_id'), all_nodes_summary)
            right_node = _get_node_details(j.get('right_input_node_id'), all_nodes_summary)
            print(f"  Node: {j.get('name', 'N/A')} ({j.get('id', 'N/A')})" + (f" [Inside: {parent_name}]" if parent_name else ""))
            print(f"    Type: {j.get('join_type', 'N/A')}")
            print(f"    Left Input: {left_node.get('name', 'N/A')} ({j.get('left_input_node_id', 'N/A')})")
            print(f"    Right Input: {right_node.get('name', 'N/A')} ({j.get('right_input_node_id', 'N/A')})")
            conditions = j.get('conditions', [])
            if conditions:
                print(f"    Conditions:")
                for cond in conditions: print(f"      - {cond.get('leftExpression', '?')} {cond.get('comparator', '?')} {cond.get('rightExpression', '?')}")
            else: print(f"    Conditions: None specified or found.")
    else: print("  No joins found.")

    # --- Print Unions ---
    print("\n--- Unions ---")
    unions_list = info.get("unions", [])
    if unions_list:
         for u in unions_list:
             parent_name = get_parent_container_name(u.get('parent_container_id'))
             print(f"  Node: {u.get('name', 'N/A')} ({u.get('id', 'N/A')})" + (f" [Inside: {parent_name}]" if parent_name else ""))
             input_ids = u.get('input_node_ids', [])
             if input_ids:
                 print(f"    Input Nodes:")
                 for input_id in input_ids:
                     input_node = _get_node_details(input_id, all_nodes_summary)
                     print(f"      - {input_node.get('name', 'N/A')} ({input_id})")
             else: print("    Input Nodes: None found.")
    else: print("  No unions found.")

    # --- Print Cleaning Steps (Grouped) ---
    print("\n--- Cleaning Steps ---")
    cleaning_steps_list = info.get("cleaning_steps", [])
    if cleaning_steps_list:
        steps_by_container = {}
        for step in cleaning_steps_list:
            parent_id = step.get('parent_container_id')
            if parent_id not in steps_by_container: steps_by_container[parent_id] = []
            steps_by_container[parent_id].append(step)

        if None in steps_by_container:
             print("  (Top Level Steps)")
             for step in steps_by_container[None]:
                 print(f"    Node: {step.get('name', 'N/A')} ({step.get('id', 'N/A')})")
                 print(f"      Type: {step.get('step_type', step.get('type', 'N/A'))}")
                 # Print details based on step_type
                 if step.get('step_type') == 'RemoveColumns': print(f"      Columns Removed: {', '.join(step.get('columns_removed', []))}")
                 elif step.get('step_type') == 'RenameColumn': print(f"      Rename: '{step.get('original_name', 'N/A')}' -> '{step.get('new_name', 'N/A')}'")
                 elif step.get('step_type') == 'ChangeColumnType': print(f"      Changes: {'; '.join(step.get('changes', []))}")
                 elif step.get('step_type') == 'Filter':
                     print(f"      Filter Column: {step.get('column_name', 'N/A')}")
                     print(f"      Filter Details: {step.get('filter_details', 'N/A')}")
                 elif step.get('step_type') == 'Calculation': print(f"      Calculation Details: {step.get('calculation_details', 'N/A')}")

        for parent_id, steps in steps_by_container.items():
            if parent_id is None: continue
            parent_name = get_parent_container_name(parent_id)
            print(f"\n  --- Steps Inside Container: {parent_name} ({parent_id}) ---")
            for step in steps:
                 print(f"    Node: {step.get('name', 'N/A')} ({step.get('id', 'N/A')})")
                 print(f"      Type: {step.get('step_type', step.get('type', 'N/A'))}")
                 # Print details based on step_type
                 if step.get('step_type') == 'RemoveColumns': print(f"      Columns Removed: {', '.join(step.get('columns_removed', []))}")
                 elif step.get('step_type') == 'RenameColumn': print(f"      Rename: '{step.get('original_name', 'N/A')}' -> '{step.get('new_name', 'N/A')}'")
                 elif step.get('step_type') == 'ChangeColumnType': print(f"      Changes: {'; '.join(step.get('changes', []))}")
                 elif step.get('step_type') == 'Filter':
                     print(f"      Filter Column: {step.get('column_name', 'N/A')}")
                     print(f"      Filter Details: {step.get('filter_details', 'N/A')}")
                 elif step.get('step_type') == 'Calculation': print(f"      Calculation Details: {step.get('calculation_details', 'N/A')}")

    else: print("  No cleaning steps extracted.")

    # --- Print Outputs ---
    print("\n--- Outputs ---")
    outputs_list = info.get("outputs", [])
    if outputs_list:
        for o in outputs_list:
            parent_name = get_parent_container_name(o.get('parent_container_id'))
            print(f"  Node: {o.get('name', 'N/A')} ({o.get('id', 'N/A')})" + (f" [Inside: {parent_name}]" if parent_name else ""))
            print(f"    Type: {o.get('type', 'N/A')}")
            print(f"    Output File: {o.get('output_file', 'N/A')}")
    else: print("  No outputs found.")

    print("=" * 60 + "\n")
    logger.info("Finished generating summary.")


# --- Main execution block (only runs when script is executed directly) ---
if __name__ == "__main__":
    logger.info("tfl_parser.py executed directly. Running example usage.")
    example1_file = r'C:\Users\ksaik\OneDrive\Desktop\MSCriptAutomation\MSCriptAutomation\New_Dashboard_Parameter_Updated.tfl'
    if os.path.exists(example1_file):
        logger.info(f"--- Running Example 1: {example1_file} ---")
        flow_info_1 = extract_prep_flow_info(example1_file)
        if flow_info_1:
            logger.info(f"Successfully extracted info from {example1_file}.")
            
            # Write to JSON file
            output_filename = os.path.splitext(example1_file)[0] + '.json'
            with open(output_filename, 'w') as f:
                json.dump(flow_info_1, f, indent=4, default=str)
            logger.info(f"Output written to {output_filename}")
        else:
            logger.error(f"Failed to extract info from {example1_file}.")
    else:
        logger.warning(f"Example file '{example1_file}' not found. Skipping Example 1.")