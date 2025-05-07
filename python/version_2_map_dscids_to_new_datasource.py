"""
Description
This script maps origin data source category ids for a given data source to target data source category ids in active audience campaign groups, then reapplies the audiences.
All origin data source category ids are marked deprecated in the taxonomy tables.

Optionally, if the origin data source is Liveramp, it can remove segments from Liveramp distribution and remove origin LiveRamp provider from automated Liveramp updates

Inputs (Set GLOBAL VARIABLES)
1. CSV file in the mapping_files folder
The file must contain the dscid mapping from the origin data source to the target data source and the following
column headers:
origin_data_source_category_id
target_data_source_category_id
2. CSV file name
3. Origin data source id
4. Target data source id

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
"""
import csv
import json
import sys
import traceback

from python.activation_api.activation_api import get_distribution_managers, get_segments_from_distribution_manager, \
    remove_segments_from_distribution_manager
from python.utils.db_util import execute_query, \
    integration_prod_db_config, coredw_prod_db_config, integration_qa_db_config, execute_fetch_all_query
from python.utils.request_util import send, audience_service_qa_config, audience_service_prod_config, \
    audience_service_path_config

####################################
#         GLOBAL VARIABLES         #
####################################
ENV = 'prod'
ORACLE_DATA_SOURCE_ID = 1
LIVERAMP_DATA_SOURCE_ID = 11
EXPERIAN_DATA_SOURCE_ID = 22
SHARETHIS_DATA_SOURCE_ID = 17
DSTILLERY_DATA_SOURCE_ID = 18
ON_AUDIENCE_DATA_SOURCE_ID = 20
MAX_SEGMENTS_PER_REQUEST = 100

# Per run inputs - TODO move to command line args
REMOVE_ORIGIN_PROVIDER_FROM_AUTOMATED_LR_UPDATES = False # Set this to true if all segments were remapped and liveramp version of these provider segments should not be used
REMOVE_SEGMENTS_FROM_LR_DISTRIBUTION = False  # Set this to True when migrating liveramp segments
ORIGIN_PROVER_NAME = 'OnAudience'  # Only needed if migrating liveramp segments - must be given as listed in integrationprod.liveramp.non_restricted_providers
ORIGIN_DATA_SOURCE_ID = ON_AUDIENCE_DATA_SOURCE_ID
TARGET_DATA_SOURCE_ID = LIVERAMP_DATA_SOURCE_ID
CSV_FILE_NAME = 'onaudience_cats.csv'


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


def deprecate_cats(data_source_id, data_source_category_ids):
    deprecate_cats_in_coredw(data_source_id, data_source_category_ids)
    deprecate_cats_by_datasource_in_integration(data_source_id, data_source_category_ids)


def deprecate_cats_in_coredw(data_source_id, data_source_category_ids):
    if data_source_id == ORACLE_DATA_SOURCE_ID:
        table_name = 'oracle_categories'
    elif data_source_id == LIVERAMP_DATA_SOURCE_ID:
        table_name = 'liveramp_categories'
    elif data_source_id == EXPERIAN_DATA_SOURCE_ID:
        table_name = 'experian_categories'
    elif data_source_id == SHARETHIS_DATA_SOURCE_ID:
        table_name = 'sharethis_categories'
    elif data_source_id == DSTILLERY_DATA_SOURCE_ID:
        table_name = 'dstillery_categories'
    elif data_source_id == ON_AUDIENCE_DATA_SOURCE_ID:
        table_name = 'onaudience_categories'
    else:
        raise Exception('Data source id %s is not valid for automatic deprecation.  Please deprecate manually.' %
                        data_source_id)
    try:
        schema = 'tpa'
        if ENV == 'qa':
            schema = 'test'
        execute_query(f"""
            update {schema}.{table_name}
            set deprecated = true
            where data_source_category_id = ANY(%s);
        """, (data_source_category_ids,), coredw_prod_db_config)
    except Exception as deprecate_exception:
        print('Error deprecating categories in coredw: %s' % deprecate_exception, file=sys.stderr)


def deprecate_cats_by_datasource_in_integration(data_source_id, data_source_category_ids):
    try:
        execute_query("""
                update tpa.categories
                set deprecated = true
                where data_source_id = %s
                and data_source_category_id = ANY(%s);
            """, (data_source_id, data_source_category_ids,), get_db_config())
    except Exception as deprecate_exception:
        print('Error deprecating categories in integration: %s' % deprecate_exception, file=sys.stderr)


def remove_provider_from_liveramp_providers(provider_name):
    try:
        execute_query("""
                delete from liveramp.non_restricted_providers
                where provider_name = %s
            """, (provider_name,), get_db_config())
    except Exception as removal_exception:
        print('Error removing provider in integration: %s' % removal_exception, file=sys.stderr)


def get_origin_target_mapping_from_file(origin_target_mapping_file_name=CSV_FILE_NAME):
    reader = csv.DictReader(open('../mapping_files/' + origin_target_mapping_file_name, "r", encoding='utf-8'))
    origin_target_mapping_dict = {}
    for line in reader:
        origin_target_mapping_dict[int(line['origin_data_source_category_id'])] = int(
            line['target_data_source_category_id'])
    return origin_target_mapping_dict


def get_distribution_managers_by_provider(distribution_manager_name):
    """
    Get distribution managers by provider name

    :param distribution_manager_name: the provider name associated with the distribution managers
    :return: a list of distribution managers
    """
    distribution_managers = get_distribution_managers()
    return [manager for manager in distribution_managers if distribution_manager_name in manager['name']]


def remove_segments_from_distribution(deprecated_cats, distribution_managers=None,
                                      distribution_manager_provider_name=None):
    """
    Remove segments from distribution for deprecated categories.  All distribution managers are searched for segments
    which takes a long time.  The time to remove segments can be increased, by inserting a list of distribution
    managers if they are known.  This is useful for rerunning the script after a failure.  The id and name of the
    distribution manager will be printed to the console if an exception is raised.  Alternatively, the name of the
    provider can be given to remove segments from only those distribution managers containing segments distributed
    by that provider.
    :param deprecated_cats: an array of deprecated categories
    :param distribution_managers:
        Format: [{'integrationConnectionID': 5386061, 'name': 'MNTN_Scanbuy_DM_11'}]
    :param distribution_manager_provider_name: the name of the distribution manager to remove segments from.
        Example: 'Scanbuy'
    :return:
    """
    print('removing segments from distribution for ' + str(len(deprecated_cats)) + ' deprecated categories')
    if distribution_managers is None and distribution_manager_provider_name is None:
        distribution_managers = get_distribution_managers()
    elif distribution_manager_provider_name is not None:
        distribution_managers = get_distribution_managers_by_provider(distribution_manager_provider_name)
    segments_removed_from_distribution = []
    exception_occurred = False
    for distribution_manager in distribution_managers:
        manager_id = int(distribution_manager['integrationConnectionID'])
        manager_name = distribution_manager['name']
        try:
            segments = get_segments_from_distribution_manager(manager_id)
            segment_ids = [int(segment['id']) for segment in segments]
            cats_to_remove = list(set(segment_ids) & set(deprecated_cats))
            if cats_to_remove:
                segment_batches = [cats_to_remove[i:i + MAX_SEGMENTS_PER_REQUEST]
                                   for i in range(0, len(cats_to_remove), MAX_SEGMENTS_PER_REQUEST)]
                for segment_batch in segment_batches:
                    remove_segments_from_distribution_manager(manager_id, segment_batch)
                    segments_removed_from_distribution.extend(segment_batch)
                print('removed ' + str(len(cats_to_remove)) + ' segments from distribution manager '
                      + str(manager_name))
            else:
                print('no segments to remove from distribution manager ' + str(manager_name))
        except Exception as e:
            print('Exception raised while removing segments for distribution manager '
                  + str(manager_name) + ' with id ' + str(manager_id) + ': ' + str(e), file=sys.stderr)
            traceback.print_exc()
            exception_occurred = True
            # TODO save the failed distribution manager and cats to a file to retry
            continue
    print('total segments removed: ' + str(len(segments_removed_from_distribution)))
    if exception_occurred:
        print('The following segments may not have been removed from distribution: '
              + str(list(set(deprecated_cats).difference(set(segments_removed_from_distribution)))))


def apply_mapping(mapping):
    def process_interest(items):
        result = False
        for item in items:
            conditions = item.get('or', [])
            for condition in conditions:
                if condition.get('data_source_id') == ORIGIN_DATA_SOURCE_ID:
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
                                if existing_condition.get('data_source_id') == TARGET_DATA_SOURCE_ID:
                                    existing_condition['cats'].extend(target_cats_to_add)
                                    existing_condition['cats'] = list(set(existing_condition['cats']))
                                    existing_condition['cats'].sort()
                                    break
                            else:
                                continue
                            break
                        else:
                            conditions.append({'data_source_id': TARGET_DATA_SOURCE_ID, 'cats': target_cats_to_add})
        return result

    impact = {}
    rows = execute_fetch_all_query("""
    select a.audience_id, expression, adv.advertiser_id, adv.company_name
    from audience.audiences a
    inner join public.advertisers adv using (advertiser_id)
    where expression_type_id = 2 and (expression like '%%"data_source_id": 11%%')
    order by audience_id
    """, get_db_config())
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
                # update_audience_expression(audience_expression_audience_id, audience_update)
        print(f'done - [{i + 1}/{len(rows)}]')
    print(json.dumps(impact, indent=4))


if __name__ == '__main__':
    print("ENV: %s" % ENV)
    print("Mapping data source categories for dsid %s to dsid %s in active audience expressions using %s" % (
        ORIGIN_DATA_SOURCE_ID, TARGET_DATA_SOURCE_ID, CSV_FILE_NAME))

    origin_target_mapping = get_origin_target_mapping_from_file()

    apply_mapping(origin_target_mapping)

    print("Deprecating categories")
    cats_to_deprecate = list(origin_target_mapping.keys())
    # deprecate_cats(ORIGIN_DATA_SOURCE_ID, cats_to_deprecate)
    print("Deprecated %s categories" % len(cats_to_deprecate))

    if ORIGIN_DATA_SOURCE_ID == LIVERAMP_DATA_SOURCE_ID:
        if REMOVE_SEGMENTS_FROM_LR_DISTRIBUTION:
            try:
                print("Removing segments from LiveRamp distribution")
                # TODO: Needs testing: remove_segments_from_distribution(cats_to_deprecate)
                # TODO: Add functionality to delete empty distribution managers
            except Exception as exception:
                traceback.print_exc()
                print('%s' % exception, file=sys.stderr)

        if REMOVE_ORIGIN_PROVIDER_FROM_AUTOMATED_LR_UPDATES:
            print("Removing origin provider from automated liveramp updates")
            remove_provider_from_liveramp_providers(ORIGIN_PROVER_NAME)

    print("Process complete")