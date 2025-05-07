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

from python.activation_api.activation_api import get_distribution_managers
from python.utils.db_util import execute_fetch_all_with_vars_query, execute_query, \
    integration_prod_db_config, coredw_prod_db_config, integration_qa_db_config
from python.utils.request_util import send, audience_service_qa_config, audience_service_prod_config, \
    audience_service_path_config

####################################
#         GLOBAL VARIABLES         #
####################################
BATCH_SIZE = 50
PROGRESS_REPORT_FREQUENCY = 50
IS_TEST = False
ENV = 'qa'
ORACLE_DATA_SOURCE_ID = 1
LIVERAMP_DATA_SOURCE_ID = 11
EXPERIAN_DATA_SOURCE_ID = 22
SHARETHIS_DATA_SOURCE_ID = 17
DSTILLERY_DATA_SOURCE_ID = 18

# Per run inputs - TODO move to command line args
REMOVE_ORIGIN_PROVIDER_FROM_AUTOMATED_LR_UPDATES = False
REMOVE_SEGMENTS_FROM_LR_DISTRIBUTION = False  # Set this to True when migrating liveramp segments
ORIGIN_PROVER_NAME = 'Experian'  # Only needed if migrating liveramp segments - must be given as listed in integrationprod.liveramp.non_restricted_providers
ORIGIN_DATA_SOURCE_ID = LIVERAMP_DATA_SOURCE_ID
TARGET_DATA_SOURCE_ID = EXPERIAN_DATA_SOURCE_ID
CSV_FILE_NAME = 'check_liveramp_experian_segments.csv'

global origin_target_mapping


####################################
#         TEST METHODS             #
####################################
def generate_test_segment():
    """
    Generate test segments for testing the expression replacement
    tuple of segment_id, expression, advertiser_id
    :return:
    """
    return [
        (1,
         '{"interest":{"include":[{"or":[{"data_source_id":1,"cats":[1,2,3,4,5]},{"data_source_id":11,"cats":[1006,1007]}]},{"or":[{"data_source_id":1,"cats":[1,2,3,4]},{"data_source_id":11,"cats":[1001,1007]}]},{"or":[{"data_source_id":1,"cats":[2,3,4]},{"data_source_id":11,"cats":[1001,1007]}]},{"or":[{"data_source_id":1,"cats":[1,2,4]},{"data_source_id":11,"cats":[1001,1007]}]},{"or":[{"data_source_id":1,"cats":[1,2,3]},{"data_source_id":11,"cats":[1007]}]},{"or":[{"data_source_id":1,"cats":[5,6]},{"data_source_id":11,"cats":[1007]}]},{"or":[{"data_source_id":1,"cats":[1,2]},{"data_source_id":11,"cats":[1007]}]},{"or":[{"data_source_id":1,"cats":[1,2]},{"data_source_id":11,"cats":[1001]}]},{"or":[{"data_source_id":1,"cats":[1,2]}]},{"or":[{"data_source_id":1,"cats":[1,2]},{"data_source_id":11,"cats":[1003,1004]}]},{"or":[{"data_source_id":11,"cats":[1003,1004]}]},{"or":[{"data_source_id":1,"cats":[1]},{"data_source_id":11,"cats":[1001]}]}],"exclude":[{"or":[{"data_source_id":11,"cats":[1001]}]},{"or":[{"data_source_id":1,"cats":[1]}]},{"or":[{"data_source_id":1,"cats":[6]}]}]},"age":[],"gender":[],"geo":{"include":[491],"exclude":[187347],"radii_include":[],"radii_exclude":[]}}',
         1)
    ]


def generate_expected_test_segment():
    return [
        (1,
         '{"interest":{"include":[{"or":[{"data_source_id":1,"cats":[4,5]},{"data_source_id":11,"cats":[1001,1004,1005,1006,1007]}]},{"or":[{"data_source_id":1,"cats":[4]},{"data_source_id":11,"cats":[1001,1004,1005,1007]}]},{"or":[{"data_source_id":1,"cats":[4]},{"data_source_id":11,"cats":[1001,1004,1005,1007]}]},{"or":[{"data_source_id":1,"cats":[4]},{"data_source_id":11,"cats":[1001,1004,1007]}]},{"or":[{"data_source_id":11,"cats":[1001,1004,1005,1007]}]},{"or":[{"data_source_id":1,"cats":[5,6]},{"data_source_id":11,"cats":[1007]}]},{"or":[{"data_source_id":11,"cats":[1001,1004,1007]}]},{"or":[{"data_source_id":11,"cats":[1001,1004]}]},{"or":[{"data_source_id":11,"cats":[1001,1004]}]},{"or":[{"data_source_id":11,"cats":[1001,1003,1004]}]},{"or":[{"data_source_id":11,"cats":[1003,1004]}]},{"or":[{"data_source_id":11,"cats":[1001]}]}],"exclude":[{"or":[{"data_source_id":11,"cats":[1001]}]},{"or":[{"data_source_id":11,"cats":[1001]}]},{"or":[{"data_source_id":1,"cats":[6]}]}]},"age":[],"gender":[],"geo":{"include":[491],"exclude":[187347],"radii_include":[],"radii_exclude":[]}}',
         1)
    ]


def validate_test_segment(generated_expression, expected_expression):
    generated_expression = sort_all_cat_lists_in_expression(generated_expression)
    expected_expression = sort_all_cat_lists_in_expression(expected_expression)
    if generated_expression != expected_expression:
        print("Test Failed")
        print("Expected : %s" % expected_expression)
        print("Generated: %s" % generated_expression)
        try:
            generated_expression_dict = json.loads(generated_expression)
            expected_expression_dict = json.loads(expected_expression)
        except ValueError:
            print("Error parsing oracle expression while running test")
            return False
        expected_include_or_statements = [include["or"] for include in expected_expression_dict["interest"]["include"]]
        expected_exclude_or_statements = [exclude["or"] for exclude in expected_expression_dict["interest"]["exclude"]]
        generated_include_or_statements = [include["or"] for include in
                                           generated_expression_dict["interest"]["include"]]
        generated_exclude_or_statements = [exclude["or"] for exclude in
                                           generated_expression_dict["interest"]["exclude"]]
        if expected_include_or_statements != generated_include_or_statements:
            print("Include OR statements do not match")
            print("Expected : %s" % expected_include_or_statements)
            print("Generated: %s" % generated_include_or_statements)
            return False
        if expected_exclude_or_statements != generated_exclude_or_statements:
            print("Exclude OR statements do not match")
            print("Expected : %s" % expected_exclude_or_statements)
            print("Generated: %s" % generated_exclude_or_statements)
            return False
    else:
        print("Test Passed")
        return True


def sort_all_cat_lists_in_expression(expression_to_sort):
    try:
        expression_dict = json.loads(expression_to_sort)
    except ValueError:
        return
    for include_exclude in ["include", "exclude"]:
        for include_exclude_statement in expression_dict["interest"][include_exclude]:
            for or_clause in include_exclude_statement["or"]:
                or_clause["cats"].sort()
    return json.dumps(expression_dict)


def validate_segments(generated_expressions):
    expected_segment = generate_expected_test_segment()
    for index in range(0, len(expected_segment)):
        current_segment = expected_segment[index]
        segment = current_segment[1]  # 1 = expression
        validate_test_segment(generated_expressions[index]['expression'], segment)


####################################
#        END TEST METHODS          #
####################################


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


def get_audience_expressions_by_data_source_id(data_source_id=ORIGIN_DATA_SOURCE_ID):
    return execute_fetch_all_with_vars_query("""
    select a.audience_id, a.expression, cg.campaign_group_id from audience.audiences a
    left join audience.audience_x_campaign_groups cg using (audience_id)
    left join audience.active_campaign_groups ac using (campaign_group_id)
    where ac.campaign_group_id is not null and expression_type_id = 2
    and (expression like %s)
    """, (f'%data_source_id\":{data_source_id},%',), get_db_config())


def update_audience_expression(audience_expression_audience_id, expression_to_update):
    config = get_audience_service_config()
    base_url = config['url']
    host = config['host']
    url = base_url + audience_service_path_config['audience']
    if url and host:
        headers = {'Host': host}
        send(method='PUT', url=url,
             path_param_key='audience_id', path_param_value=audience_expression_audience_id,
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


def map_target_cats(origin_expression):
    try:
        expression_dict = json.loads(origin_expression)
    except ValueError:
        return origin_expression

    for include_exclude in ["include", "exclude"]:
        for include_exclude_statement in expression_dict["interest"][include_exclude]:
            if include_exclude_statement["or"]:
                map_origin_to_target_in_or_clause(include_exclude_statement["or"])
    return json.dumps(expression_dict)


def map_origin_to_target_in_or_clause(or_clause, origin_data_source_id=ORIGIN_DATA_SOURCE_ID,
                                      target_data_source_id=TARGET_DATA_SOURCE_ID):
    """
    Map origin data source to target data source in the or clause
    example or_clause (using oracle and liveramp data sources):
    {"data_source_id":1,"cats":[4,5]},{"data_source_id":11,"cats":[1001,1002,1003]}
    :param target_data_source_id:
    :param origin_data_source_id:
    :param or_clause:
    :return:
    example result or_clause (mapping dsid 1/dscid 4 to dsid 11/dscid 1004):
    {"data_source_id":1,"cats":[5]},{"data_source_id":11,"cats":[1001,1002,1003,1004]}
    """
    matched_to_target_or_clause_data = set()
    has_target_data = False
    or_clause_copy = or_clause.copy()
    for or_clause_data in or_clause_copy:
        # example or_clause_data: {"data_source_id":1,"cats":[4,5]}
        # check the or clause for origin data source
        if or_clause_data["data_source_id"] == origin_data_source_id:
            # if the clause has origin data, map the origin cats to the target cats and remove the origin cat
            or_clause_data_cats = or_clause_data["cats"].copy()
            # for each data_source_category_id in the categories list
            for cat in or_clause_data_cats:
                # if it can be mapped, remove the origin cat and add the target cat to the data set
                if or_clause_data["data_source_id"] == origin_data_source_id and cat in origin_target_mapping:
                    matched_to_target_or_clause_data.add(origin_target_mapping[cat])
                    or_clause_data["cats"].remove(cat)
                    # if the or clause data has no more cats, remove the or clause data
                    if not or_clause_data["cats"]:
                        or_clause.remove(or_clause_data)
        if or_clause_data["data_source_id"] == target_data_source_id:
            has_target_data = True
    if has_target_data and matched_to_target_or_clause_data:
        update_target_data_source(or_clause, matched_to_target_or_clause_data)
    elif not has_target_data and matched_to_target_or_clause_data:
        or_clause.append({"data_source_id": target_data_source_id, "cats": list(matched_to_target_or_clause_data)})


def update_target_data_source(or_clause, new_cats_or_clause_data, target_data_source_id=TARGET_DATA_SOURCE_ID):
    or_clause_copy = or_clause.copy()
    for or_clause_data in or_clause_copy:
        if or_clause_data["data_source_id"] == target_data_source_id:
            # don't add duplicate target categories
            set_of_cats = set(or_clause_data["cats"])
            set_of_cats.update(new_cats_or_clause_data)
            or_clause_data["cats"] = list(set_of_cats)


def process_expressions(expression_audience_id, expressions):
    for expression_to_process in expressions:
        try:
            update_audience_expression(expression_audience_id, expression_to_process)
        except Exception as process_exception:
            print('%s: %s' % (expression_to_process, process_exception), file=sys.stderr)
            continue


def get_distribution_managers_by_provider(distribution_manager_name):
    """
    Get distribution managers by provider name

    :param distribution_manager_name: the provider name associated with the distribution managers
    :return: a list of distribution managers
    """
    distribution_managers = get_distribution_managers()
    return [manager for manager in distribution_managers if distribution_manager_name in manager['name']]


# def remove_segments_from_distribution(deprecated_cats, distribution_managers=None,
#                                       distribution_manager_provider_name=None):
#     """
#     Remove segments from distribution for deprecated categories.  All distribution managers are searched for segments
#     which takes a long time.  The time to remove segments can be increased, by inserting a list of distribution
#     managers if they are known.  This is useful for rerunning the script after a failure.  The id and name of the
#     distribution manager will be printed to the console if an exception is raised.  Alternatively, the name of the
#     provider can be given to remove segments from only those distribution managers containing segments distributed
#     by that provider.
#     :param deprecated_cats: an array of deprecated categories
#     :param distribution_managers:
#         Format: [{'integrationConnectionID': 5386061, 'name': 'MNTN_Scanbuy_DM_11'}]
#     :param distribution_manager_provider_name: the name of the distribution manager to remove segments from.
#         Example: 'Scanbuy'
#     :return:
#     """
#     print('removing segments from distribution for ' + str(len(deprecated_cats)) + ' deprecated categories')
#     if distribution_managers is None and distribution_manager_provider_name is None:
#         distribution_managers = get_distribution_managers()
#     elif distribution_manager_provider_name is not None:
#         distribution_managers = get_distribution_managers_by_provider(distribution_manager_provider_name)
#     segments_removed_from_distribution = []
#     exception_occurred = False
#     for distribution_manager in distribution_managers:
#         manager_id = int(distribution_manager['integrationConnectionID'])
#         manager_name = distribution_manager['name']
#         try:
#             segments = get_segments_from_distribution_manager(manager_id)
#             segment_ids = [int(segment['id']) for segment in segments]
#             cats_to_remove = list(set(segment_ids) & set(deprecated_cats))
#             if cats_to_remove:
#                 segment_batches = [cats_to_remove[i:i + MAX_SEGMENTS_PER_REQUEST]
#                                    for i in range(0, len(cats_to_remove), MAX_SEGMENTS_PER_REQUEST)]
#                 for segment_batch in segment_batches:
#                     remove_segments_from_distribution_manager(manager_id, segment_batch)
#                     segments_removed_from_distribution.extend(segment_batch)
#                 print('removed ' + str(len(cats_to_remove)) + ' segments from distribution manager '
#                       + str(manager_name))
#             else:
#                 print('no segments to remove from distribution manager ' + str(manager_name))
#         except Exception as e:
#             print('Exception raised while removing segments for distribution manager '
#                   + str(manager_name) + ' with id ' + str(manager_id) + ': ' + str(e), file=sys.stderr)
#             traceback.print_exc()
#             exception_occurred = True
#             # TODO save the failed distribution manager and cats to a file to retry
#             continue
#     print('total segments removed: ' + str(len(segments_removed_from_distribution)))
#     if exception_occurred:
#         print('The following segments may not have been removed from distribution: '
#               + str(list(set(deprecated_cats).difference(set(segments_removed_from_distribution)))))


if __name__ == '__main__':
    print("ENV: %s" % ENV)
    print("Mapping data source categories for dsid %s to dsid %s in active audience expressions using %s" % (
        ORIGIN_DATA_SOURCE_ID, TARGET_DATA_SOURCE_ID, CSV_FILE_NAME))

    # store oracle/liveramp to sharethis mappings in global
    if IS_TEST:
        origin_target_mapping = {1: 1001, 2: 1004, 3: 1005}  # dsid 1  to dsid 11
    else:
        origin_target_mapping = get_origin_target_mapping_from_file()

    ###########################
    #   HANDLE TPA SEGMENTS   #
    ###########################
    if IS_TEST:
        segment_rows = generate_test_segment()
    else:
        segment_rows = get_audience_expressions_by_data_source_id()

    expressions_remapped_counter = 0
    target_expressions = []
    original_expressions = []

    print("Mapping TPA audience expressions for %s audiences" % len(segment_rows))
    for row in segment_rows:
        try:
            audience_id = row[0]
            expression = row[1]
            campaign_group_id = row[2]

            # Generate Target Expressions
            target_expression = map_target_cats(expression).replace(" ", "")
            if expression != target_expression:
                original_expressions.append({'audience_id': audience_id, 'expression': expression,
                                             'campaign_group_id': campaign_group_id})
                target_expressions.append({'audience_id': audience_id, 'expression': target_expression,
                                           'campaign_group_id': campaign_group_id})
                expressions_remapped_counter += 1

            if expressions_remapped_counter != 0 and expressions_remapped_counter % BATCH_SIZE == 0:
                process_expressions(audience_id, target_expressions)
                target_expressions = []

        except Exception as exception:
            traceback.print_exc()
            print('%s: %s' % (row, exception), file=sys.stderr)

        # Report progress
        if expressions_remapped_counter != 0 and expressions_remapped_counter % PROGRESS_REPORT_FREQUENCY == 0:
            print("audience expressions remaining: %s" % str(len(segment_rows) - expressions_remapped_counter))

    # Process remaining items
    try:
        if expressions_remapped_counter > 0:
            if IS_TEST:
                print("Validating test expressions")
                validate_segments(target_expressions)
            else:
                process_expressions(audience_id, target_expressions)
    except Exception as exception:
        traceback.print_exc()
        print('%s' % exception, file=sys.stderr)

    print("audience expressions re-mapped for %s audiences" % str(expressions_remapped_counter))

    if not IS_TEST:
        print("Deprecating categories")
        cats_to_deprecate = list(origin_target_mapping.keys())
        deprecate_cats(ORIGIN_DATA_SOURCE_ID, cats_to_deprecate)
        print("Deprecated %s categories" % len(cats_to_deprecate))

        if ORIGIN_DATA_SOURCE_ID == LIVERAMP_DATA_SOURCE_ID:
            if REMOVE_SEGMENTS_FROM_LR_DISTRIBUTION:
                try:
                    print("Removing segments from LiveRamp distribution")
                    # TODO: remove_segments_from_distribution(cats_to_deprecate)
                except Exception as exception:
                    traceback.print_exc()
                    print('%s' % exception, file=sys.stderr)

            if REMOVE_ORIGIN_PROVIDER_FROM_AUTOMATED_LR_UPDATES:
                print("Removing origin provider from automated liveramp updates")
                remove_provider_from_liveramp_providers(ORIGIN_PROVER_NAME)

    print("Process complete")
