import pandas as pd
import os
import sys

def create_mapping(output_name=None):
    if output_name is None:
        print("Error: Output name is required")
        sys.exit(1)
        
    # Read the suggested matches
    suggested_matches = pd.read_csv(os.path.join("match_finder", "suggested_match", "data.csv"))
    
    # Create mapping dataframe with required columns
    mapping_df = pd.DataFrame({
        'origin_data_source_category_id': suggested_matches['data_source_cat_id'],
        'target_data_source_category_id': suggested_matches['best_match_data_source_cat_id']
    })
    
    # Ensure the mapping_files directory exists
    os.makedirs("mapping_files", exist_ok=True)
    
    # Save the mapping file
    output_path = os.path.join("mapping_files", f"{output_name}.csv")
    mapping_df.to_csv(output_path, index=False)
    print(f"Mapping file created at: {output_path}")

def main():
    if len(sys.argv) != 2:
        print("Usage: create-mapping <output_name>")
        sys.exit(1)
    
    output_name = sys.argv[1]
    create_mapping(output_name)

if __name__ == "__main__":
    main() 