import requests
import json
import os
from datetime import datetime

def fetch_and_store_cat_sizes():
    url = "http://a48e0dc5459e74ed9a460f718d19f2e8-a863d546fb12e42c.elb.us-west-2.amazonaws.com/totals"
    headers = {
        "Host": "audience-service-prod.core-prod.k8.steelhouse.com"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Ensure the directory exists
        output_dir = "match_finder/current_cat_sizes"
        os.makedirs(output_dir, exist_ok=True)
        
        # Write the response to a file
        output_file = f"{output_dir}/data.json"
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
            
        print(f"Successfully stored category sizes in: {output_file}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {str(e)}")
    except Exception as e:
        print(f"Error processing or storing data: {str(e)}")

if __name__ == "__main__":
    fetch_and_store_cat_sizes()