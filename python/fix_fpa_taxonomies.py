import json

from python.integration_repository.IntegrationRepository import IntegrationRepository


def migrate_taxonomies(advertiser_id=0):
    repository = IntegrationRepository()
    rows = repository.execute_fetch_all(
        """        
        select c.data_source_category_id, c.advertiser_id, a.company_name, c.name, c.description, c.path, c.mntn_id
        from fpa.categories c
        inner join public.advertisers a using (advertiser_id)
        where ((%(advertiser_id)s = 0 OR advertiser_id = %(advertiser_id)s) and c.path_from_root_types is null)
          or ((%(advertiser_id)s = 0 OR advertiser_id = %(advertiser_id)s) and path similar to '%%\\d+%%')
          or ((%(advertiser_id)s = 0 OR advertiser_id = %(advertiser_id)s) and c.parent_id is null and name similar to '%%\\d+%%')
          and data_source_id = 16
        order by c.advertiser_id, c.path
        """,
        {'advertiser_id': advertiser_id}
    )
    type_ids = {
        'AdvertiserID': 1,
        'PageViews': 2,
        'Conversions': 3,
        'Impressions': 4,
        'VV': 5,
        'Prospecting': 6,
        'MultiTouch': 7,
        'Retargeting': 8,
        'CampaignGroupID': 9,
        'CampaignID': 10,
    }
    taxonomy = {}
    for i, row in enumerate(rows):
        data_source_category_id = row['data_source_category_id']
        advertiser_id = row['advertiser_id']
        company_name = row['company_name']
        name = row['name']
        description = row['description']
        # In some cases mntn_id has already been set to the correct value along with name and description but,
        # path, names and path from root are still the old way. This causes the update query to fail because of the
        # the data type for mntn_id is int.
        mntn_id_already_set = isinstance(row['mntn_id'], int)
        path = row['path'].split(' > ')
        path[0] = 'AdvertiserID'
        if len(path) == 1:
            taxonomy['name'] = f'AdvertiserID - {company_name}'
            taxonomy['description'] = f'AdvertiserID - {company_name}'
            taxonomy['mntn_id'] = advertiser_id
        elif len(path) == 4:
            path[3] = 'CampaignGroupID'
            taxonomy['name'] = 'CampaignGroupID'
            taxonomy['description'] = 'CampaignGroupID'
            if not mntn_id_already_set:
                taxonomy['mntn_id'] = name
            else:
                taxonomy['mntn_id'] = row['mntn_id']
        elif len(path) == 5:
            path[3] = 'CampaignGroupID'
            path[4] = 'CampaignID'
            taxonomy['name'] = 'CampaignID'
            taxonomy['description'] = 'CampaignID'
            if not mntn_id_already_set:
                taxonomy['mntn_id'] = name
            else:
                taxonomy['mntn_id'] = row['mntn_id']
        else:
            taxonomy['name'] = name
            taxonomy['description'] = description
            taxonomy['mntn_id'] = None
        taxonomy['data_source_category_id'] = data_source_category_id
        taxonomy['path'] = ' > '.join(path)
        taxonomy['names'] = json.dumps({'names': ['ROOT'] + path})
        taxonomy['mntn_id_type'] = type_ids[path[-1]]
        taxonomy['path_from_root_types'] = json.dumps({'pathFromRootTypes': [0] + [type_ids[p] for p in path]})
        repository.execute_query(
            """
            update fpa.categories
            set
                name = %(name)s,
                description = %(description)s,
                mntn_id = %(mntn_id)s,
                path = %(path)s,
                names = %(names)s,
                mntn_id_type = %(mntn_id_type)s,
                path_from_root_types = %(path_from_root_types)s
            where
                data_source_category_id = %(data_source_category_id)s
                and data_source_id = 16
            """,
            taxonomy
        )
        print(f'done - [{i+1}/{len(rows)}]')



if __name__ == '__main__':
    migrate_taxonomies()
