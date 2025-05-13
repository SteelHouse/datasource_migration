"""
One off to map DS11 to DS35
"""
import json
import re
import time
import traceback

from python.audience_service import update_audience_expression, get_all_audience_expressions
from python.utils.config import config

####################################
#         GLOBAL VARIABLES         #
####################################

ORIGIN_DATA_SOURCE_ID = 11  # LiveRamp
TARGET_DATA_SOURCE_ID = 35  # New Data Source

def update_expression_datasource(expr_json):
    modified = False
    # Case 1: interest-style structure (expressionTypeId 2)
    if 'interest' in expr_json:
        for section in ['include', 'exclude']:
            items = expr_json.get('interest', {}).get(section, [])
            for item in items:
                for condition in item.get('or', []):
                    if condition.get('data_source_id') == ORIGIN_DATA_SOURCE_ID:
                        condition['data_source_id'] = TARGET_DATA_SOURCE_ID
                        modified = True

    # Case 2: category-style structure with version 2 — use regex
    elif expr_json.get('version') == '2':
        env = config('../config.ini',  'environment')['env']
        print(f"[{env.upper()}] Processing category-style structure with version 2 for {expr_json}")
        original_str = json.dumps(expr_json)
        updated_str = re.sub(r'"data_source_id"\s*:\s*11\b', '"data_source_id": 35', original_str)
        if updated_str != original_str:
            expr_json = json.loads(updated_str)
            modified = True

    return expr_json if modified else None

def apply_datasource_update():
    batch_size = 20
    sleep_between_batches = 3  # seconds

    rows = get_all_audience_expressions(ORIGIN_DATA_SOURCE_ID)
    print(f"Found {len(rows)} audiences to process")

    impacted = {}

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        print(f"\nProcessing batch {i // batch_size + 1} [{i + 1}-{i + len(batch)}]")

        for j, (audience_id, expression_str, advertiser_id, company_name) in enumerate(batch):
            # print(f"  → Audience Id: {audience_id} Advertiser Id: {advertiser_id} Company Name: {company_name}")
            # print(f"  → Expression Str: {expression_str}")
            expression_json = json.loads(expression_str)
            updated_expr_json = update_expression_datasource(expression_json)
            if updated_expr_json:
                try:
                    print(f"  → Updated expression: {updated_expr_json}")
                    update_audience_expression(audience_id, {
                        "expression": json.dumps(updated_expr_json, separators=(',', ':')),
                        "expressionTypeId": 2
                    })
                    impacted[advertiser_id] = company_name
                    time.sleep(0.1)
                except Exception as e:
                    print(f"  → Error updating audience {audience_id}: {e}")
                    traceback.print_exc()
        print(f"Sleeping {sleep_between_batches}s before next batch...")
        time.sleep(sleep_between_batches)

    print("\n=== Impacted Advertisers ===")
    print(json.dumps(impacted, indent=4))

    print(f"Writing impacted advertisers to CSV file...")


if __name__ == '__main__':
    print("Starting datasource update from 11 to 35...")
    apply_datasource_update()
    print("Process complete.")