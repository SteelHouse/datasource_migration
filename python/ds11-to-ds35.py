"""
One off to map DS11 to DS35
"""
import json

from utils.db_util import execute_fetch_all_query, integration_prod_db_config, integration_qa_db_config
from utils.request_util import send, audience_service_qa_config, audience_service_prod_config, \
    audience_service_path_config

####################################
#         GLOBAL VARIABLES         #
####################################
ENV = 'qa'  # Make sure this stays 'qa' until you're ready for prod
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
    if url and host:
        headers = {'Host': host}
        send(method='PUT', url=url,
             path_param_key="{audience_id}", path_param_value=audience_expression_audience_id,
             json_data={'expression': expression_to_update['expression'],
                        'expressionTypeId': 2},
             request_headers=headers, retry_timer=0)
    else:
        raise Exception('Could not get audience-service config')

def update_expression_datasource(expr_json):
    modified = False

    # Go through both 'include' and 'exclude' sections
    for section in ['include', 'exclude']:
        items = expr_json.get('interest', {}).get(section, [])
        for item in items:
            for condition in item.get('or', []):
                if condition.get('data_source_id') == ORIGIN_DATA_SOURCE_ID:
                    condition['data_source_id'] = TARGET_DATA_SOURCE_ID
                    modified = True
    return expr_json if modified else None

def apply_datasource_update():
    BATCH_SIZE = 50
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
            print(f"  → Audience Id: {audience_id} Advertiser Id: {advertiser_id} Company Name: {company_name}")
            print(f"  → Expression Str: {expression_str}")
            expression_json = json.loads(expression_str)
            updated_expr_json = update_expression_datasource(expression_json)
            if updated_expr_json:
                print("updated_expr_json:  " + json.dumps(updated_expr_json, separators=(',', ':')))
                # update_audience_expression(audience_id, {
                #     "expression": json.dumps(updated_expr_json, separators=(',', ':')),
                #     "expressionTypeId": 2
                # })
                impacted[advertiser_id] = company_name
        #         time.sleep(0.1)
        # print(f"Sleeping {SLEEP_BETWEEN_BATCHES}s before next batch...")
        # time.sleep(SLEEP_BETWEEN_BATCHES)

    print("\n=== Impacted Advertisers ===")
    print(json.dumps(impacted, indent=4))

if __name__ == '__main__':
    print("Starting datasource update from 11 to 35...")
    apply_datasource_update()
    print("Process complete.")