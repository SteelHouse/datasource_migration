"""
implementation for https://mntn.atlassian.net/browse/TGT-2309
Description
take all audiences expressions
if it contains an Oracle category ID (from source 1) listed on the “Oracle-ShareThis Maps” tab, we want to remove it
from the audience expression and add in the ShareThis category ID (from source 17)

if it contains a LiveRamp segment ID (from source 11) listed on the “Liveramp-ShareThis Maps” tab, we want to remove
the LiveRamp segment ID from the audience expression and add in the ShareThis category ID (from source 17)

This will help us get a read on performance more quickly and help us save costs: based on last month’s LR costs, it’ll
save us ~$10k a month if we can switch existing campaigns to the direct versions
"""
import json
import sys
import traceback

import psycopg2
import requests

####################################
#         GLOBAL VARIABLES         #
####################################
BATCH_SIZE = 50
PROGRESS_REPORT_FREQUENCY = 50
IS_TEST = False
ENV = 'prod'
global oracle_sharethis_mapping
global liveramp_sharethis_mapping
global intprod_conn
global coredw_conn
global aus_host
global aus_headers


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
         '{"interest":{"include":[{"or":[{"data_source_id":1,"cats":[1,2,3,4,5]},{"data_source_id":11,"cats":[101,102,103,104]},{"data_source_id":17,"cats":[1006,1007]}]},{"or":[{"data_source_id":1,"cats":[1,2,3,4,5]},{"data_source_id":11,"cats":[100,101,102,103,104]},{"data_source_id":17,"cats":[1006,1007]}]},{"or":[{"data_source_id":1,"cats":[1,2,3,4]},{"data_source_id":11,"cats":[101,105]},{"data_source_id":17,"cats":[1001,1007]}]},{"or":[{"data_source_id":1,"cats":[2,3,4]},{"data_source_id":11,"cats":[100,101,105]},{"data_source_id":17,"cats":[1001,1007]}]},{"or":[{"data_source_id":1,"cats":[1,2,4]},{"data_source_id":11,"cats":[100,101,105]},{"data_source_id":17,"cats":[1001,1007]}]},{"or":[{"data_source_id":1,"cats":[1,2,3]},{"data_source_id":11,"cats":[105,106]},{"data_source_id":17,"cats":[1007]}]},{"or":[{"data_source_id":1,"cats":[5,6]},{"data_source_id":11,"cats":[100,101]},{"data_source_id":17,"cats":[1007]}]},{"or":[{"data_source_id":1,"cats":[1,2]},{"data_source_id":11,"cats":[101,102]},{"data_source_id":17,"cats":[1007]}]},{"or":[{"data_source_id":1,"cats":[1,2]},{"data_source_id":11,"cats":[101,102]},{"data_source_id":17,"cats":[1001]}]},{"or":[{"data_source_id":1,"cats":[1,2]},{"data_source_id":11,"cats":[101,102]}]},{"or":[{"data_source_id":1,"cats":[1,2]},{"data_source_id":11,"cats":[100,102]}]},{"or":[{"data_source_id":1,"cats":[4,5]},{"data_source_id":11,"cats":[100,102]}]},{"or":[{"data_source_id":1,"cats":[1,2]},{"data_source_id":11,"cats":[103,104]}]},{"or":[{"data_source_id":1,"cats":[4,5]},{"data_source_id":11,"cats":[103,104]}]},{"or":[{"data_source_id":1,"cats":[1,2]},{"data_source_id":17,"cats":[1003,1004]}]},{"or":[{"data_source_id":11,"cats":[100,101]},{"data_source_id":17,"cats":[1003,1004]}]},{"or":[{"data_source_id":11,"cats":[100,101]},{"data_source_id":17,"cats":[1001,1004]}]},{"or":[{"data_source_id":11,"cats":[100]},{"data_source_id":17,"cats":[1001]}]},{"or":[{"data_source_id":1,"cats":[1]},{"data_source_id":11,"cats":[100]},{"data_source_id":17,"cats":[1001]}]}],"exclude":[{"or":[{"data_source_id":17,"cats":[1001]}]},{"or":[{"data_source_id":1,"cats":[1]}]},{"or":[{"data_source_id":11,"cats":[100]}]},{"or":[{"data_source_id":11,"cats":[106]}]},{"or":[{"data_source_id":1,"cats":[6]}]}]},"age":[],"gender":[],"geo":{"include":[491],"exclude":[187347],"radii_include":[],"radii_exclude":[]}}',
         1)
    ]


def generate_expected_test_segment():
    return [
        (1,
         '{"interest":{"include":[{"or":[{"data_source_id":1,"cats":[4,5]},{"data_source_id":11,"cats":[103,104]},{"data_source_id":17,"cats":[1001,1004,1005,1002,1003,1006,1007]}]},{"or":[{"data_source_id":1,"cats":[4,5]},{"data_source_id":11,"cats":[103,104]},{"data_source_id":17,"cats":[1001,1004,1005,1002,1003,1006,1007]}]},{"or":[{"data_source_id":1,"cats":[4]},{"data_source_id":11,"cats":[105]},{"data_source_id":17,"cats":[1001,1004,1005,1002,1007]}]},{"or":[{"data_source_id":1,"cats":[4]},{"data_source_id":11,"cats":[105]},{"data_source_id":17,"cats":[1001,1004,1005,1002,1007]}]},{"or":[{"data_source_id":1,"cats":[4]},{"data_source_id":11,"cats":[105]},{"data_source_id":17,"cats":[1001,1002,1004,1007]}]},{"or":[{"data_source_id":11,"cats":[105,106]},{"data_source_id":17,"cats":[1001,1004,1005,1007]}]},{"or":[{"data_source_id":1,"cats":[5,6]},{"data_source_id":17,"cats":[1001,1002,1007]}]},{"or":[{"data_source_id":17,"cats":[1001,1004,1002,1003,1007]}]},{"or":[{"data_source_id":17,"cats":[1001,1004,1002,1003]}]},{"or":[{"data_source_id":17,"cats":[1001,1004,1002,1003]}]},{"or":[{"data_source_id":17,"cats":[1001,1004,1003]}]},{"or":[{"data_source_id":1,"cats":[4,5]},{"data_source_id":17,"cats":[1001,1003]}]},{"or":[{"data_source_id":11,"cats":[103,104]},{"data_source_id":17,"cats":[1001,1004]}]},{"or":[{"data_source_id":1,"cats":[4,5]},{"data_source_id":11,"cats":[103,104]}]},{"or":[{"data_source_id":17,"cats":[1001,1003,1004]}]},{"or":[{"data_source_id":17,"cats":[1001,1002,1003,1004]}]},{"or":[{"data_source_id":17,"cats":[1001,1002,1004]}]},{"or":[{"data_source_id":17,"cats":[1001]}]},{"or":[{"data_source_id":17,"cats":[1001]}]}],"exclude":[{"or":[{"data_source_id":17,"cats":[1001]}]},{"or":[{"data_source_id":17,"cats":[1001]}]},{"or":[{"data_source_id":17,"cats":[1001]}]},{"or":[{"data_source_id":11,"cats":[106]}]},{"or":[{"data_source_id":1,"cats":[6]}]}]},"age":[],"gender":[],"geo":{"include":[491],"exclude":[187347],"radii_include":[],"radii_exclude":[]}}',
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


def query_segments():
    int_prod_cursor = intprod_conn.cursor()
    int_prod_cursor.execute(
        """
        select a.audience_id, a.expression, cg.campaign_group_id from audience.audiences a
        left join audience.audience_x_campaign_groups cg using (audience_id)
        left join audience.active_campaign_groups ac using (campaign_group_id)
        where ac.campaign_group_id is not null and expression_type_id = 2
        and (expression like '%data_source_id\":1,%' or expression like '%data_source_id\":11,%');
        """
    )
    return int_prod_cursor.fetchall()


def get_oracle_sharethis_mapping():
    core_dw_cursor = coredw_conn.cursor()
    core_dw_cursor.execute(
        """
        select data_source_category_id, sharethis_segment_id from tpa.oracle_sharethis_mapping
        where sharethis_segment_id is not null
        """
    )
    output_rows = core_dw_cursor.fetchall()
    return dict(output_rows)


def get_liveramp_sharethis_mapping():
    sql = ("select data_source_category_id, sharethis_segment_id from tpa.liveramp_sharethis_mapping "
           "where sharethis_segment_id is not null")
    core_dw_cursor = coredw_conn.cursor()
    core_dw_cursor.execute(sql)
    output_rows = core_dw_cursor.fetchall()
    return dict(output_rows)


def map_sharethis_cats(oracle_expression):
    try:
        expression_dict = json.loads(oracle_expression)
    except ValueError:
        return oracle_expression

    for include_exclude in ["include", "exclude"]:
        for include_exclude_statement in expression_dict["interest"][include_exclude]:
            if include_exclude_statement["or"]:
                map_oracle_and_liveramp_to_sharethis_in_or_clause(include_exclude_statement["or"])
    return json.dumps(expression_dict)


def map_oracle_and_liveramp_to_sharethis_in_or_clause(or_clause):
    """
    Map oracle and liveramp data sources to sharethis data source in the or clause
    example or_clause:
    {"data_source_id":1,"cats":[4,5]},{"data_source_id":11,"cats":[103,104]},{"data_source_id":17,"cats":[1001,1004,1005,1002,1003,1006,1007]}
    :param or_clause:
    :return:
    """
    matched_to_sharethis_or_clause_data = set()
    has_sharethis_data = False
    or_clause_copy = or_clause.copy()
    for or_clause_data in or_clause_copy:
        # example or_clause_data: {"data_source_id":1,"cats":[4,5]}
        # check the or clause for oracle or liveramp data sources
        if or_clause_data["data_source_id"] == 1 or or_clause_data["data_source_id"] == 11:
            # if the clause has oracle data, map the oracle cats to the sharethis cats and remove the oracle cat
            or_clause_data_cats = or_clause_data["cats"].copy()
            # for each data_source_category_id in the categories list
            for cat in or_clause_data_cats:
                # if it can be mapped, remove the oracle cat and add the sharethis cat to the data set
                if or_clause_data["data_source_id"] == 1 and cat in oracle_sharethis_mapping:
                    matched_to_sharethis_or_clause_data.add(oracle_sharethis_mapping[cat])
                    or_clause_data["cats"].remove(cat)
                    # if the or clause data has no more cats, remove the or clause data
                    if not or_clause_data["cats"]:
                        or_clause.remove(or_clause_data)
                elif or_clause_data["data_source_id"] == 11 and cat in liveramp_sharethis_mapping:
                    matched_to_sharethis_or_clause_data.add(liveramp_sharethis_mapping[cat])
                    or_clause_data["cats"].remove(cat)
                    if not or_clause_data["cats"]:
                        or_clause.remove(or_clause_data)
        if or_clause_data["data_source_id"] == 17:
            has_sharethis_data = True
    if has_sharethis_data and matched_to_sharethis_or_clause_data:
        update_sharethis_data_source(or_clause, matched_to_sharethis_or_clause_data)
    elif not has_sharethis_data and matched_to_sharethis_or_clause_data:
        or_clause.append({"data_source_id": 17, "cats": list(matched_to_sharethis_or_clause_data)})


def update_sharethis_data_source(or_clause, new_cats_or_clause_data):
    or_clause_copy = or_clause.copy()
    for or_clause_data in or_clause_copy:
        if or_clause_data["data_source_id"] == 17:
            # don't add duplicate sharethis categories
            set_of_cats = set(or_clause_data["cats"])
            set_of_cats.update(new_cats_or_clause_data)
            or_clause_data["cats"] = list(set_of_cats)


def update_audience_expression(expression_to_update):
    int_prod_cursor = intprod_conn.cursor()
    int_prod_cursor.execute('update audience.audiences set expression = %s where audience_id = %s',
                            (expression_to_update['expression'], expression_to_update['audience_id']))
    intprod_conn.commit()


def process_expressions(expressions):
    for expression_to_process in expressions:
        update_audience_expression(expression_to_process)
        reapply_audience(expression_to_process)


def reapply_audience(expression_to_reapply):
    resp = requests.put(
        f'http://{aus_host}/audience_campaign_group',
        json={'campaignGroupId': expression_to_reapply['campaign_group_id'],
              'audienceId': expression_to_reapply['audience_id']},
        headers=aus_headers
    )
    resp.raise_for_status()


if __name__ == '__main__':

    # initialize connections
    coredw_conn = psycopg2.connect(
        "dbname='coredw' user='awsuser' host='prod-integration-data.csqufkxaxf37.us-west-2.redshift.amazonaws.com' "
        "password='Sh#242prod#238' port='5439'")

    if ENV == 'prod':
        intprod_conn = psycopg2.connect(
            "dbname='integrationprod' user='steelhousecore' "
            "host='integration-prod-master.crvrygavls2u.us-west-2.rds.amazonaws.com' password='Sh#242prod#238'")
        aus_headers = {'Host': 'audience-service-prod.core-prod.k8.steelhouse.com'}
        aus_host = 'a48e0dc5459e74ed9a460f718d19f2e8-a863d546fb12e42c.elb.us-west-2.amazonaws.com'
    elif ENV == 'qa':
        intprod_conn = psycopg2.connect(
            "dbname='qacoredb' user='qacore' "
            "host='integration-dev.crvrygavls2u.us-west-2.rds.amazonaws.com' password='qa#core07#19'")
        aus_headers = {'Host': 'audience-service-qa.coredev.west2.steelhouse.com'}
        aus_host = 'a15c263080f634c92995b43c4993d7cf-2ab3bb2edaf9380d.elb.us-west-2.amazonaws.com'

    # store oracle/liveramp to sharethis mappings in global
    if IS_TEST:
        oracle_sharethis_mapping = {1: 1001, 2: 1004, 3: 1005}  # dsid 1  to dsid 17
        liveramp_sharethis_mapping = {100: 1001, 101: 1002, 102: 1003}  # dsid 11 to dsid 17
    else:
        oracle_sharethis_mapping = get_oracle_sharethis_mapping()
        liveramp_sharethis_mapping = get_liveramp_sharethis_mapping()

    ###########################
    #   HANDLE TPA SEGMENTS   #
    ###########################
    if IS_TEST:
        segment_rows = generate_test_segment()
    else:
        segment_rows = query_segments()

    expressions_remapped_counter = 0
    sharethis_expressions = []
    original_expressions = []

    print("Mapping TPA audience expressions for %s audiences" % len(segment_rows))
    for row in segment_rows:
        try:
            audience_id = row[0]
            expression = row[1]
            campaign_group_id = row[2]

            # Generate ShareThis Expressions
            sharethis_expression = map_sharethis_cats(expression).replace(" ", "")
            if expression != sharethis_expression:
                original_expressions.append({'audience_id': audience_id, 'expression': expression,
                                             'campaign_group_id': campaign_group_id})
                sharethis_expressions.append({'audience_id': audience_id, 'expression': sharethis_expression,
                                              'campaign_group_id': campaign_group_id})
                expressions_remapped_counter += 1

            if expressions_remapped_counter != 0 and expressions_remapped_counter % BATCH_SIZE == 0:
                # Calculate audience size and insert into sharethis_segment_sizes using batch processing
                process_expressions(sharethis_expressions)
                sharethis_expressions = []

        except Exception as exception:
            traceback.print_exc()
            print('%s: %s' % (row, exception), file=sys.stderr)

        # Report progress
        if expressions_remapped_counter != 0 and expressions_remapped_counter % PROGRESS_REPORT_FREQUENCY == 0:
            print("audience expressions remaining: %s" % str(len(segment_rows) - expressions_remapped_counter))

    # Process remaining batch items
    try:
        if expressions_remapped_counter > 0:
            if IS_TEST:
                print("Validating test expressions")
                validate_segments(sharethis_expressions)
            else:
                process_expressions(sharethis_expressions)
    except Exception as exception:
        traceback.print_exc()
        print('%s' % exception, file=sys.stderr)

    print("audience expressions re-mapped for %s audiences" % str(expressions_remapped_counter))
