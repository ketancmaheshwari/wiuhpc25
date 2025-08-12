import csv
import os

def parse_and_convert_sched_output_v2(input_sched_file_path, output_csv_file_path, max_assigned_nodes_display=5):
    """
    Parses an Accasim 'sched-' output file with complex embedded delimiters
    and converts it into a more human-readable CSV format with proper headers.

    It handles the '{queue_time}__{assignations}__{start_time}' compound field
    and further breaks down 'assignations' into separate columns for each assigned node.

    Args:
        input_sched_file_path (str): The full path to the input sched- output file.
        output_csv_file_path (str): The full path for the output CSV file.
        max_assigned_nodes_display (int): The maximum number of assigned nodes
                                          to show in explicit columns (e.g., Node 1 ID, Node 1 Cores, etc.).
                                          Any additional assigned nodes will be grouped into one column.
    """
    # Base headers that are always present (9 fields)
    base_headers = [
        "Job ID",
        "User ID",
        "Queue Time",
        "Start Time",
        "End Time",
        "Total Nodes Requested",
        "Total CPU Cores Requested",
        "Total Memory Requested (MB)",
        "Expected Duration (Sim Units)"
    ]

    # Dynamically create headers for assigned nodes
    assigned_node_headers = []
    for i in range(1, max_assigned_nodes_display + 1):
        assigned_node_headers.extend([
            f"Assigned Node {i} ID",
            f"Assigned Node {i} Cores",
            f"Assigned Node {i} Mem (MB)"
        ])
    
    # Add an overflow column for any assignments beyond max_assigned_nodes_display
    final_headers = base_headers + assigned_node_headers + ["Additional Assigned Details"]

    all_parsed_rows = []

    try:
        with open(input_sched_file_path, 'r') as infile:
            for line_num, line in enumerate(infile, 1):
                line = line.strip()
                if not line: # Skip empty lines
                    continue

                # Use string manipulation to correctly parse the fields
                # Format: "{job_id};{user};{queue_time}__{assignations}__{start_time};{end_time};{total_nodes};{total_cpu};{total_mem};{expected_duration};"

                # 1. Extract job_id and user_id (first two fields)
                parts_initial = line.split(';', 2) # Split only at the first two semicolons
                if len(parts_initial) < 3:
                    print(f"Warning: Line {line_num} has fewer than 3 main sections. Skipping: {line}")
                    continue
                job_id = parts_initial[0]
                user_id = parts_initial[1]
                
                # The rest of the line, containing the compound field and trailing fields
                remaining_line_from_compound = parts_initial[2]

                # 2. Extract the last 5 fixed fields + the compound field from the end
                #    These are: end_time, total_nodes, total_cpu, total_mem, expected_duration, (empty for trailing ';')
                #    So, we look for 6 semicolons from the right.
                parts_from_right = remaining_line_from_compound.rsplit(';', 6)
                
                if len(parts_from_right) < 7: # 6 splits means 7 parts
                    print(f"Warning: Line {line_num} has fewer than 7 parts after right-split. Skipping: {line}")
                    continue

                # Assign the trailing fixed fields
                end_time = parts_from_right[1]
                total_nodes = parts_from_right[2]
                total_cpu = parts_from_right[3]
                total_mem = parts_from_right[4]
                expected_duration = parts_from_right[5] # The actual expected duration value
                # parts_from_right[6] would be an empty string if there was a trailing semicolon in the input

                # The first part is the raw compound field
                compound_field_raw = parts_from_right[0]

                # 3. Parse the compound field: '{queue_time}__{assignations}__{start_time}'
                queue_time = "N/A"
                assignations_string = "N/A"
                start_time = "N/A"

                first_double_underscore_idx = compound_field_raw.find('__')
                last_double_underscore_idx = compound_field_raw.rfind('__')

                if first_double_underscore_idx != -1 and last_double_underscore_idx != -1 and \
                   first_double_underscore_idx < last_double_underscore_idx:
                    queue_time = compound_field_raw[:first_double_underscore_idx]
                    assignations_string = compound_field_raw[first_double_underscore_idx + 2 : last_double_underscore_idx]
                    start_time = compound_field_raw[last_double_underscore_idx + 2:]
                else:
                    print(f"Warning: Unexpected 'compound_field' format on line {line_num}: {compound_field_raw}. Using raw string for all parts.")
                    queue_time = compound_field_raw
                    assignations_string = "PARSE_ERROR"
                    start_time = "PARSE_ERROR"


                # 4. Parse 'assignations_string' (e.g., '1;2;896959#2;2;896959#3;2;896959')
                individual_assignments_blocks = assignations_string.split('#')
                
                current_assigned_node_details = []
                additional_assignments = []

                for i, assign_block in enumerate(individual_assignments_blocks):
                    if not assign_block: # Skip empty blocks if any
                        continue
                    
                    node_id_cores_mem = assign_block.split(';')
                    if len(node_id_cores_mem) == 3:
                        if i < max_assigned_nodes_display:
                            current_assigned_node_details.extend(node_id_cores_mem)
                        else:
                            additional_assignments.append(assign_block)
                    else:
                        print(f"Warning: Malformed assignment block '{assign_block}' on line {line_num}. Skipping.")
                        if i < max_assigned_nodes_display: # Still try to fill if within explicit range
                             current_assigned_node_details.extend(["N/A", "N/A", "N/A"])


                # Pad the explicit assigned node details with empty strings if fewer than expected
                while len(current_assigned_node_details) < max_assigned_nodes_display * 3:
                    current_assigned_node_details.append('')

                # Combine all parts for the current row
                row_data = [
                    job_id,
                    user_id,
                    queue_time,
                    start_time,
                    end_time,
                    total_nodes,
                    total_cpu,
                    total_mem,
                    expected_duration
                ]
                row_data.extend(current_assigned_node_details[:max_assigned_nodes_display * 3]) # Ensure we only add up to the max explicit columns
                row_data.append("#".join(additional_assignments)) # Add remaining assignments as a string

                all_parsed_rows.append(row_data)

        # Write to CSV
        with open(output_csv_file_path, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(final_headers) # Write headers
            writer.writerows(all_parsed_rows) # Write data rows

        print(f"Successfully converted '{input_sched_file_path}' to '{output_csv_file_path}'")
        print(f"Total columns in output CSV: {len(final_headers)}")

    except FileNotFoundError:
        print(f"Error: Input file not found at '{input_sched_file_path}'")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for debugging

if __name__ == "__main__":
    # --- Configuration ---
    # IMPORTANT: Replace with the actual path to your sched- output file
    # Check your Accasim results folder for the exact filename, e.g.,
    # /home/er3/ORNL-Work/Simulators/accasim/results/Demo_Experiment/EASYBackfilling_FirstFit/
    # The file might be named something like 'sched-sample_workload.swf' or 'sched-sample_workload_20250716_120000.swf'
    input_file_path = '/home/er3/ORNL-Work/Simulators/accasim/results/Demo_Experiment/EBF#BF/sched-sample_workload.swf'
    
    # Define the output CSV file path
    output_directory = os.path.dirname(input_file_path)
    output_file_name = 'parsed_schedule_output.csv'
    output_file_path = os.path.join(output_directory, output_file_name)

    # Set the maximum number of assigned nodes you want to display in separate columns.
    # If a job is assigned to more nodes, the extra ones will go into 'Additional Assigned Details'.
    # A value of 3 will result in 9 (base) + 3*3 (assigned) + 1 (additional) = 19 columns.
    # To get 17 columns, you might want to use 2 for max_assigned_nodes_display,
    # as 9 + 2*3 + 1 = 16. The final 17th could be a specific field if it's always there.
    # Let's start with 3 to capture your example, which results in 19 columns.
    # You can adjust this to fit your exact 17-column expectation if you know the exact source.
    MAX_ASSIGNED_NODES = 2

    # --- Run the conversion ---
    parse_and_convert_sched_output_v2(input_file_path, output_file_path, max_assigned_nodes_display=MAX_ASSIGNED_NODES)

    print("\nRemember to check the actual file name (e.g., with timestamps) in your results directory.")
    print("Adjust 'input_file_path' and 'MAX_ASSIGNED_NODES' in the script as needed.")