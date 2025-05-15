import pandas as pd
import json
import os
import subprocess

def calculate_size_change(original_size, new_size):
    """Calculate the percentage change between two sizes"""
    if original_size == 0:
        return 0
    return round((new_size / original_size) * 100)

def enrich_data_with_sizes():
    # First ensure we have current size data by running get-sizes
    subprocess.run(['poetry', 'run', 'get-sizes'], check=True)
    
    # Read the audience size data
    with open('match_finder/current_cat_sizes/data.json', 'r') as f:
        size_data = json.load(f)
    
    # Get audience size for a data source and category
    def get_audience_size(data_source_id, cat_id):
        try:
            return size_data['data_source_counts'][str(data_source_id)]['counts'][str(cat_id)]
        except (KeyError, TypeError):
            return 0
    
    # Process each CSV file
    files_to_process = [
        ('match_finder/similarity_too_low/data.csv', 'match_finder/similarity_too_low/data_with_size.csv'),
        ('match_finder/suggested_match/data.csv', 'match_finder/suggested_match/data_with_size.csv')
    ]
    
    for input_file, output_file in files_to_process:
        try:
            df = pd.read_csv(input_file)
            
            # Add size columns
            df['data_source_cat_size'] = df.apply(
                lambda row: get_audience_size(row['data_source_id'], row['data_source_cat_id']),
                axis=1
            )
            
            df['best_match_data_source_cat_size'] = df.apply(
                lambda row: get_audience_size(row['best_match_data_source_id'], row['best_match_data_source_cat_id']),
                axis=1
            )
            
            # Calculate size change percentage
            df['size_change'] = df.apply(
                lambda row: calculate_size_change(row['data_source_cat_size'], row['best_match_data_source_cat_size']),
                axis=1
            )
            
            # Save enriched data
            df.to_csv(output_file, index=False)
            print(f"Successfully processed {input_file} -> {output_file}")
            
            print(f"\nStatistics for {os.path.basename(input_file)}:")
            print(f"Total rows processed: {len(df)}")
            print(f"Average size change: {df['size_change'].mean():.2f}%")
            print(f"Median size change: {df['size_change'].median():.2f}%")
            print(f"Rows with size increase: {len(df[df['size_change'] > 100])}")
            print(f"Rows with size decrease: {len(df[df['size_change'] < 100])}")
            print(f"Rows with no change: {len(df[df['size_change'] == 100])}\n")
            
        except Exception as e:
            print(f"Error processing {input_file}: {str(e)}")

if __name__ == "__main__":
    enrich_data_with_sizes()