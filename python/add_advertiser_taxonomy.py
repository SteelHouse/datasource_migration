import time

from python.integration_repository.IntegrationRepository import IntegrationRepository
from python.utils.config import config
from python.utils.request_util import (send, audience_service_qa_config, audience_service_prod_config,
                                       audience_service_path_config)


def get_advertisers_ids(repo):
    return [result['advertiser_id'] for result in repo.execute_fetch_all(f"""
        select distinct a.advertiser_id
        from public.advertisers a
        left join fpa.categories c on a.advertiser_id = c.advertiser_id and c.data_source_id = 16
        where c.advertiser_id is null
    """, None)]


def get_audience_service_config():
    env = config('../config.ini', 'environment')['env']
    if env == 'qa':
        return audience_service_qa_config
    if env == 'prod':
        return audience_service_prod_config


def create_advertiser_taxonomy(advertiser_id):
    """
    Create Advertiser FpaAudienceTaxonomy
    POST /advertiser/{advertiser}/fpa_audience_taxonomy
    :param advertiser_id:
    :return:
    """
    audience_service_config = get_audience_service_config()
    base_url = audience_service_config['url']
    host = audience_service_config['host']
    path = audience_service_path_config['create_audience_taxonomy']
    url = base_url + path
    if url and host:
        headers = {'Host': host}
        send(method='POST', url=url, path_param_key='{advertiser}', path_param_value=advertiser_id,
             request_headers=headers, retry_timer=0)
    else:
        raise Exception('Could not get audience-service config')


if __name__ == '__main__':
    repository = IntegrationRepository()
    advertisers = get_advertisers_ids(repository)
    batch_size = 20
    num_advertisers = len(advertisers)
    print(f"Processing {num_advertisers} advertisers over {num_advertisers // batch_size} batches")
    advertiser_batches = [advertisers[i:i + batch_size] for i in range(0, num_advertisers, batch_size)]
    for count, advertiser_batch in enumerate(advertiser_batches):
        print(f"Starting iteration {count}")
        for advertiser in advertiser_batch:
            create_advertiser_taxonomy(advertiser)
            print('.', sep=' ', end='')
        time.sleep(10)
        print('')
