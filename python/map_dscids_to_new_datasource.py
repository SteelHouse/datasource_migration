"""
Description

NOTE: This script does not handle version 2 audience expressions!

This script maps origin data source category ids for a given data source to target data source category ids in active audience campaign groups, then reapplies the audiences.
All origin data source category ids are marked deprecated in the taxonomy tables.

Optionally, if the origin data source is Liveramp, it can remove segments from Liveramp distribution and remove origin LiveRamp provider from automated Liveramp updates

Inputs (Set GLOBAL VARIABLES)
1. CSV file in the mapping_files folder
The file must contain the dscid mapping from the origin data source to the target data source and the following
column headers:
origin_data_source_category_id
target_data_source_category_id
2. CSV_FILE_NAME - CSV file name
3. ORIGIN_DATA_SOURCE_NAME - Origin data source name
4. TARGET_DATA_SOURCE_NAME - Target data source name

Prerequisites
The target taxonomy table must be populated with the target data source category ids and tpa.categories (make sure replication to intprod has already happened) must include the target data source

Implementation
1. Retrieve all audiences expressions for active campaigns and for each expression:
If the expression contains the origin category_id (origin data_source_id/data_source_category_id pair = origin_cat_key)
listed in the mapping csv, remove the origin category id and add the target category id
(target data_source_id/data_source_category_id pair = target_cat_key)

2. Reapply the audience expression to the campaign group (using audience-service)

3. Deprecate the categories in the taxonomy tables

4. Optionally, if the origin data source is Liveramp, set the appropriate global variables to
    a. TODO NOT_IMPLEMENTED - Remove segments from Liveramp distribution - controlled by REMOVE_SEGMENTS_FROM_LR_DISTRIBUTION
    b. Remove origin LiveRamp provider from automated Liveramp updates - controlled by REMOVE_ORIGIN_PROVIDER_FROM_AUTOMATED_LR_UPDATES
        i. This prevents the automated Liveramp updater (part of oracle-audience-service) from adding new segments for distribution or re-adding segments that were removed from distribution

TODOs
TODO create version that can be run from command line
TODO Test removal of segments from Liveramp distribution - implementation complete but not tested
"""
import csv
import json
import sys

from python.audience_service import update_audience_expression, get_all_audience_expressions
from python.liveramp_service import remove_provider_from_liveramp_providers
from python.utils.config import config
from python.utils.data_source_util import get_data_source_table, get_data_source_id
from python.utils.db_util import execute_query

####################################
#         GLOBAL VARIABLES         #
####################################

# Per run inputs
REMOVE_ORIGIN_PROVIDER_FROM_AUTOMATED_LR_UPDATES = False # Set this to true if all segments were remapped and liveramp version of these provider segments should not be used
ORIGIN_PROVER_NAME = 'OnAudience'  # Only needed if migrating *from* liveramp segments - must be given as listed in integrationprod.liveramp.non_restricted_providers
REMOVE_SEGMENTS_FROM_LR_DISTRIBUTION = False  # TODO NOT_IMPLEMENTED Set this to True when migrating *from* liveramp segments

ORIGIN_DATA_SOURCE_NAME = 'Dstillery'
TARGET_DATA_SOURCE_NAME = 'LiveRamp'
CSV_FILE_NAME = 'dstillery-to-lr-mapping.csv'


def deprecate_cats(data_source_id, data_source_category_ids):
    deprecate_cats_in_coredw(data_source_id, data_source_category_ids)
    deprecate_cats_by_datasource_in_integration(data_source_id, data_source_category_ids)


def deprecate_cats_in_coredw(data_source_id, data_source_category_ids):
    table_name = get_data_source_table(data_source_id)
    try:
        schema = 'tpa'
        env = config('../config.ini',  'environment')['env']
        if env == 'qa':
            schema = 'test'
        execute_query(f"""
            update {schema}.{table_name}
            set deprecated = true
            set updated_date = date.today()
            where data_source_category_id = ANY(%s);
        """, (data_source_category_ids,), True)
    except Exception as deprecate_exception:
        print('Error deprecating categories in coredw: %s' % deprecate_exception, file=sys.stderr)


def deprecate_cats_by_datasource_in_integration(data_source_id, data_source_category_ids):
    try:
        execute_query("""
                update tpa.categories
                set deprecated = true
                where data_source_id = %s
                and data_source_category_id = ANY(%s);
            """, (data_source_id, data_source_category_ids,))
    except Exception as deprecate_exception:
        print('Error deprecating categories in integration: %s' % deprecate_exception, file=sys.stderr)


def get_origin_target_mapping_from_file(origin_target_mapping_file_name=CSV_FILE_NAME):
    reader = csv.DictReader(open('../mapping_files/' + origin_target_mapping_file_name, "r", encoding='utf-8'))
    origin_target_mapping_dict = {}
    for line in reader:
        origin_target_mapping_dict[int(line['origin_data_source_category_id'])] = int(
            line['target_data_source_category_id'])
    return origin_target_mapping_dict


def apply_mapping(mapping):
    def process_interest(items):
        result = False
        for item in items:
            conditions = item.get('or', [])
            for condition in conditions:
                if condition.get('data_source_id') == get_data_source_id(ORIGIN_DATA_SOURCE_NAME):
                    updated_cats = []
                    target_cats_to_add = []
                    for cat in condition['cats']:
                        if cat in mapping:
                            target_cats_to_add.append(mapping[cat])
                        else:
                            updated_cats.append(cat)
                    if updated_cats:
                        condition['cats'] = updated_cats
                    else:
                        conditions.remove(condition)
                    if target_cats_to_add:
                        result = True
                        for existing_item in items:
                            for existing_condition in existing_item.get('or', []):
                                if existing_condition.get('data_source_id') == get_data_source_id(TARGET_DATA_SOURCE_NAME):
                                    existing_condition['cats'].extend(target_cats_to_add)
                                    existing_condition['cats'] = list(set(existing_condition['cats']))
                                    existing_condition['cats'].sort()
                                    break
                            else:
                                continue
                            break
                        else:
                            conditions.append({'data_source_id': get_data_source_id(TARGET_DATA_SOURCE_NAME), 'cats': target_cats_to_add})
        return result

    impact = {}
    rows = get_all_audience_expressions()
    for i, audience_expression_row in enumerate(rows):
        audience_expression_audience_id = audience_expression_row[0]
        audience_expression_advertiser_id = audience_expression_row[2]
        audience_expression_company_name = audience_expression_row[3]
        audience_expression_expression = json.loads(audience_expression_row[1])
        if 'interest' in audience_expression_expression:
            interest = audience_expression_expression['interest']
            include = interest['include']
            exclude = interest['exclude']
            if process_interest(include) or process_interest(exclude):
                impact[audience_expression_advertiser_id] = audience_expression_company_name
                audience_update = {
                    "expression": json.dumps(audience_expression_expression, separators=(',', ':')),
                    "expressionTypeId": 2
                }
                update_audience_expression(audience_expression_audience_id, audience_update)
        print(f'done - [{i + 1}/{len(rows)}]')
    print(json.dumps(impact, indent=4))


if __name__ == '__main__':
    print("ENV: %s" % config('../config.ini',  'environment')['env'])
    print("Mapping data source categories for dsid %s to dsid %s in active audience expressions using %s" % (
        get_data_source_id(ORIGIN_DATA_SOURCE_NAME), get_data_source_id(TARGET_DATA_SOURCE_NAME), CSV_FILE_NAME))

    origin_target_mapping = get_origin_target_mapping_from_file()

    apply_mapping(origin_target_mapping)

    print("Deprecating categories")
    cats_to_deprecate = list(origin_target_mapping.keys())
    deprecate_cats(get_data_source_id(ORIGIN_DATA_SOURCE_NAME), cats_to_deprecate)
    print("Deprecated %s categories" % len(cats_to_deprecate))

    if get_data_source_id(ORIGIN_DATA_SOURCE_NAME) == get_data_source_id('LiveRamp'):
        if REMOVE_SEGMENTS_FROM_LR_DISTRIBUTION:
            print("")
            # TODO: Needs testing: remove_segments_from_distribution(cats_to_deprecate)
        if REMOVE_ORIGIN_PROVIDER_FROM_AUTOMATED_LR_UPDATES:
            remove_provider_from_liveramp_providers(ORIGIN_PROVER_NAME)

    print("Process complete")