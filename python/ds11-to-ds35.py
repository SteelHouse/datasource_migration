"""
One off to map DS11 to DS35
"""
import json
import re
import time
import traceback
from utils.db_util import execute_fetch_all_query, integration_prod_db_config, integration_qa_db_config
from utils.request_util import send, audience_service_qa_config, audience_service_prod_config, \
    audience_service_path_config

####################################
#         GLOBAL VARIABLES         #
####################################
ENV = 'qa'  # Make sure this stays 'qa' until you're ready for prod
X_USER_ID_QA = '120351'
ORIGIN_DATA_SOURCE_ID = 11  # LiveRamp
TARGET_DATA_SOURCE_ID = 35  # New Data Source

def get_db_config():
    if ENV == 'qa':
        return integration_qa_db_config
    if ENV == 'prod':
        return integration_prod_db_config

def get_audience_service_config():
    if ENV == 'qa':
        return audience_service_qa_config
    if ENV == 'prod':
        return audience_service_prod_config

def update_audience_expression(audience_expression_audience_id, expression_to_update):
    config = get_audience_service_config()
    base_url = config['url']
    host = config['host']
    url = base_url + audience_service_path_config['audience']
    print(f"[{ENV.upper()}] Updating audience {audience_expression_audience_id} at: {url}")
    if url and host:
        headers = {'Host': host, 'X-User-Id': X_USER_ID_QA}
        send(method='PUT', url=url,
             path_param_key="{audience_id}", path_param_value=audience_expression_audience_id,
             json_data={'expression': expression_to_update['expression'],
                        'expressionTypeId': 2},
             request_headers=headers, retry_timer=0)
    else:
        raise Exception('Could not get audience-service config')

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
        print(f"[{ENV.upper()}] Processing category-style structure with version 2 for {expr_json}")
        original_str = json.dumps(expr_json)
        updated_str = re.sub(r'"data_source_id"\s*:\s*11\b', '"data_source_id": 35', original_str)
        if updated_str != original_str:
            expr_json = json.loads(updated_str)
            modified = True

    return expr_json if modified else None

def apply_datasource_update():
    BATCH_SIZE = 20
    SLEEP_BETWEEN_BATCHES = 3  # seconds

    rows = execute_fetch_all_query("""
        SELECT a.audience_id, a.expression, adv.advertiser_id, adv.company_name
        FROM audience.audiences a
        INNER JOIN public.advertisers adv USING (advertiser_id)
        WHERE expression_type_id = 2
        AND a.expression ~ '.*"data_source_id":\w?11\w?[,}].*'::text
        ORDER BY a.audience_id;
    """, get_db_config())
    print(f"Found {len(rows)} audiences to process")

    impacted = {}

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        print(f"\nProcessing batch {i // BATCH_SIZE + 1} [{i + 1}-{i + len(batch)}]")

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
        print(f"Sleeping {SLEEP_BETWEEN_BATCHES}s before next batch...")
        time.sleep(SLEEP_BETWEEN_BATCHES)

    print("\n=== Impacted Advertisers ===")
    print(json.dumps(impacted, indent=4))

    print(f"Writing impacted advertisers to CSV file...")


if __name__ == '__main__':
    print("Starting datasource update from 11 to 35...")
    apply_datasource_update()
    print("Process complete.")