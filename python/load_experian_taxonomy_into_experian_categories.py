import json
import sys

from integration_repository import IntegrationRepository

EXPERIAN_TAXONOMY_FILE_NAME = '../taxonomy_files/DIGITAL_MASTER_DATA_20240911_072.metadata.jsonl'


def get_experian_taxonomy_from_file(experian_taxonomy_file_name=EXPERIAN_TAXONOMY_FILE_NAME):
    """
    parses jsonl file and returns a list of taxonomies
    :param experian_taxonomy_file_name:
    :return:
    """
    # add root and experian parent nodes
    experian_taxonomies = [{
        'data_source_id': 22,
        'data_source_category_id': 0,
        'experian_segment_id': None,
        'parent_id': None,
        'partner_id': 0,
        'name': 'ROOT',
        'description': 'ROOT',
        'path': 'ROOT',
        'names': '{"names": ["ROOT"]}',
        'path_from_root': '{"pathFromRoot": [0]}',
        'is_leaf_node': False,
        'navigation_only': False,
        'advertiser_id': None,
        'deprecated': False,
        'public': False,
        'sort_order': None,
        'cpm': None
    }, {
        'data_source_id': 22,
        'data_source_category_id': 1,
        'experian_segment_id': None,
        'parent_id': 0,
        'partner_id': 0,
        'name': 'Experian',
        'description': 'Experian',
        'path': 'Experian',
        'names': '{"names": ["ROOT", "Experian"]}',
        'path_from_root': '{"pathFromRoot": [0, 1]}',
        'is_leaf_node': False,
        'navigation_only': False,
        'advertiser_id': None,
        'deprecated': False,
        'public': False,
        'sort_order': None,
        'cpm': None
    }]
    with open(experian_taxonomy_file_name, 'r') as file:
        for index, line in enumerate(file):
            json_line = json.loads(line)
            name = json_line['FullPath']
            experian_taxonomies.append({
                'data_source_id': 22,
                'data_source_category_id': index + 2,
                'experian_segment_id': json_line['SegmentID'],
                'parent_id': 1,
                'partner_id': 0,
                'name': name,
                'description': json_line['SegmentDescription'],
                'path': 'Experian > ' + name,
                'names': '{"names": ["ROOT", "Experian", "' + name + '"]}',
                'path_from_root': '{"pathFromRoot": [0, 1, ' + str(index + 2) + ']}',
                'is_leaf_node': False,
                'navigation_only': False,
                'advertiser_id': None,
                'deprecated': False,
                'public': False,
                'sort_order': None,
                'cpm': json_line['SegmentCPM']})
    return experian_taxonomies


def add_experian_taxonomies_to_dw(experian_taxonomies):
    """
    adds experian taxonomies to the dw
    make sure config.ini has the correct environment and is_dw set to True
    make sure tpa.experian_categories table exists and is empty before running this
    :param experian_taxonomies:
    :return:
    """
    if experian_taxonomies is None:
        raise ValueError("taxonomies are required")
    try:
        repository = IntegrationRepository.IntegrationRepository()
        repository.execute_batch_query(f"""
            INSERT INTO {repository.schema}.experian_categories (data_source_id,data_source_category_id,
            experian_segment_id,parent_id,partner_id,name,description,path,names,path_from_root,is_leaf_node,
            navigation_only,advertiser_id,deprecated,public,sort_order,cpm)
                VALUES (%(data_source_id)s, %(data_source_category_id)s, %(experian_segment_id)s, %(parent_id)s,
                %(partner_id)s, %(name)s, %(description)s, %(path)s, %(names)s, %(path_from_root)s, %(is_leaf_node)s,
                %(navigation_only)s, %(advertiser_id)s, %(deprecated)s, %(public)s, %(sort_order)s, %(cpm)s);
            """, experian_taxonomies)
    except Exception as taxonomy_exception:
        print('Error saving taxonomy: %s' % taxonomy_exception, file=sys.stderr)


if __name__ == '__main__':
    taxonomies = get_experian_taxonomy_from_file()
    add_experian_taxonomies_to_dw(taxonomies)
