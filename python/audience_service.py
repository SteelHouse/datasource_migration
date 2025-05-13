from python.utils.config import config
from python.utils.db_util import execute_fetch_all_query
from python.utils.request_util import send

audience_service_qa_config = config('../config.ini', 'audience_service_qa')
audience_service_prod_config = config('../config.ini', 'audience_service_prod')
audience_service_path_config = config('../config.ini', 'audience_service_path_urls')
env = config('../config.ini', 'environment')['env']

def get_audience_service_config():
    if env == 'qa':
        return audience_service_qa_config
    elif env == 'prod':
        return audience_service_prod_config
    else:
        raise ValueError(f"Unknown environment: {env}")


def get_all_audience_expressions(data_source_id):
    return execute_fetch_all_query(f"""
    select a.audience_id, expression, adv.advertiser_id, adv.company_name
    from audience.audiences a
    inner join public.advertisers adv using (advertiser_id)
    where expression_type_id = 2 
    and expression ~ '.*"data_source_id":\w?{data_source_id}\w?[,}}].*'::text
    order by audience_id desc
    """)


def get_audience_expressions_for_active_campaign_groups(data_source_id):

    return execute_fetch_all_query(f"""
    select a.audience_id, a.expression, adv.advertiser_id, adv.company_name
    from audience.audiences a
    inner join public.advertisers adv using (advertiser_id)
    left join audience.audience_x_campaign_groups cg using (audience_id)
    left join audience.active_campaign_groups ac using (campaign_group_id)
    where ac.campaign_group_id is not null and expression_type_id = 2
    and expression ~ '.*"data_source_id":\w?{data_source_id}\w?[,}}].*'::text
    """)


def update_audience_expression(audience_expression_audience_id, expression_to_update):
    as_config = get_audience_service_config()
    x_user_id = as_config['x_user_id']
    base_url = as_config['url']
    # host = as_config['host']
    url = base_url + audience_service_path_config['audience']
    print(f"[{env.upper()}] Updating audience {audience_expression_audience_id} at: {url}")
    if url:
        headers = {'X-User-Id': x_user_id}
        send(method='PUT', url=url,
             path_param_key="{audience_id}", path_param_value=audience_expression_audience_id,
             json_data={'expression': expression_to_update['expression'],
                        'expressionTypeId': 2},
             request_headers=headers, retry_timer=0)
    else:
        raise Exception('Could not get audience-service config')