import pandas as pd
import json
import os
import sys
from datetime import datetime
import subprocess

def calculate_size_change(original_size, new_size):
    """Calculate the percentage change between two sizes"""
    if original_size == 0:
        return 0
    return round((new_size / original_size) * 100)

def enrich_mapping_with_sizes(source_id, target_id, mapping_file_name):
    # First ensure we have current size data by running get-sizes
    subprocess.run(['poetry', 'run', 'get-sizes'], check=True)

    with open('match_finder/current_cat_sizes/data.json', 'r') as f:
        size_data = json.load(f)
    
    # Get audience size for a data source and category
    def get_audience_size(data_source_id, cat_id):
        try:
            return size_data['data_source_counts'][str(data_source_id)]['counts'][str(cat_id)]
        except (KeyError, TypeError):
            return 0
    
    input_file = f'mapping_files/{mapping_file_name}'
    output_dir = 'qa_size'
    os.makedirs(output_dir, exist_ok=True)
    
    # Add timestamp to output filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    base_name = os.path.splitext(mapping_file_name)[0]
    output_file = f'{output_dir}/{timestamp}_{base_name}_with_size.csv'
    
    try:
        df = pd.read_csv(input_file)
        
        # Add size columns using the provided source_id and target_id
        df['origin_data_source_category_size'] = df.apply(
            lambda row: get_audience_size(source_id, row['origin_data_source_category_id']),
            axis=1
        )
        
        df['target_data_source_category_size'] = df.apply(
            lambda row: get_audience_size(target_id, row['target_data_source_category_id']),
            axis=1
        )
        
        # Calculate size change percentage
        df['size_change'] = df.apply(
            lambda row: calculate_size_change(row['origin_data_source_category_size'], row['target_data_source_category_size']),
            axis=1
        )
        
        # Save enriched data
        df.to_csv(output_file, index=False)
        print(f"Successfully processed {input_file} -> {output_file}")
        
        print(f"\nStatistics for {mapping_file_name}:")
        print(f"Total rows processed: {len(df)}")
        print(f"Average size change: {df['size_change'].mean():.2f}%")
        print(f"Median size change: {df['size_change'].median():.2f}%")
        print(f"Rows with size increase: {len(df[df['size_change'] > 100])}")
        print(f"Rows with size decrease: {len(df[df['size_change'] < 100])}")
        print(f"Rows with no change: {len(df[df['size_change'] == 100])}\n")
        
    except Exception as e:
        print(f"Error processing {input_file}: {str(e)}")
        return 1
    return 0

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Enrich mapping file with size data')
    parser.add_argument('source_id', type=int, help='Source data source ID')
    parser.add_argument('target_id', type=int, help='Target data source ID')
    parser.add_argument('mapping_file', type=str, help='Name of the mapping file in mapping_files directory')
    
    args = parser.parse_args()
    return enrich_mapping_with_sizes(args.source_id, args.target_id, args.mapping_file)

if __name__ == "__main__":
    sys.exit(main()) 