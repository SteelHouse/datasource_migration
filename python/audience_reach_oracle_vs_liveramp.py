"""
Implementation for https://mntn.atlassian.net/browse/TGT-1623
Description
we are trying to compare the sizes of each migrated segment
we need to know the oracle size of each segment

LR size of each segment
Flag huge deltas ∆
look at the raw value size comparison
lives in TPA data warehouse
"""
import json
import psycopg2
import requests
import sys
import time
import traceback

####################################
#         GLOBAL VARIABLES         #
####################################
BATCH_SIZE = 50
PROGRESS_REPORT_FREQUENCY = 50
IS_TEST = False
global oracle_live_ramp_mapping

####################################
#         GLOBAL CONNECTION        #
####################################
intprod_conn = psycopg2.connect(
    "dbname='integrationprod' user='steelhousecore' "
    "host='integration-prod-master.crvrygavls2u.us-west-2.rds.amazonaws.com' password='redacted'")

redshift_coredw_conn = psycopg2.connect(
    "dbname='coredw' user='awsuser' host='prod-integration-data.csqufkxaxf37.us-west-2.redshift.amazonaws.com' "
    "password='redacted' port='5439'")


####################################
#         TEST FUNCTIONS           #
####################################
def generate_test_tpa_segment():
    return [
        (1,
         '{"interest":{"include":[{"or":[{"data_source_id":1,"cats":[1,2,3,4,5,6,7,8,9,10]},{"data_source_id":11,"cats":[100,101]}]},{"or":[{"data_source_id":1,"cats":[1,2,3,4]},{"data_source_id":11,"cats":[100,101]}]},{"or":[{"data_source_id":1,"cats":[6,7,8,9]},{"data_source_id":11,"cats":[100,101]}]},{"or":[{"data_source_id":1,"cats":[1,2,3,4,5,6,7,8,9,10]}]}],"exclude":[{"or":[{"data_source_id":1,"cats":[1,2,3,4]}]},{"or":[{"data_source_id":1,"cats":[6,7,8,9]}]}]},"age":[],"gender":[],"geo":{"include":[491],"exclude":[187347],"radii_include":[],"radii_exclude":[]}}',
         1),
        (2,
         '{"interest":{"include":[{"or":[{"data_source_id":1,"cats":[1827737,730638,107912,928133,1609491,575364,575365,1888780,1726475,1466628,1795337,1045275,471838,882450,1466422,1466423,383398,437410,437411,437408,575151,73514,1807159,1155135,1155134,1807156,441524,1807145,494512,1934506,575153,437437,928434,325307,1917789,1917913,437443,216004,386754,839878,1101507,1475650,1605836,6237,6238,1050703,647393,385132,12266,1677938,1917933,31097,340092,1047026,1810406,359290,437113]},{"data_source_id":2,"cats":[205591]}]},{"or":[{"data_source_id":14,"cats":[1]}]}],"exclude":[{"or":[{"data_source_id":1,"cats":[136162,1637593,1891774,162756,737133,1339539,571364,765446,25423,695768,203568,134102,1466627,326040,340089,1452939]},{"data_source_id":8,"cats":[1598,1601,1600,1599,1602]},{"data_source_id":16,"cats":[2488]}]}]},"age":[],"gender":[],"geo":{"include":[237],"exclude":[352261,290813,335866,196605],"radii_include":[],"radii_exclude":[]}}',
         1),
        (3,
         '{"interest":{"include":[{"or":[{"data_source_id":1,"cats":[842093,1965183,842095,358912,842090,32072,1965173,3049,842085,1225787,32778,5259,13771,31021,417960,417961,1957714,1957709,24018,434772,5174,44150,5752,36445]},{"data_source_id":2,"cats":[204236]},{"data_source_id":4,"cats":[2133551]}]},{"or":[{"data_source_id":1,"cats":[36442,5755,61,62,63]}]},{"or":[{"data_source_id":14,"cats":[1]}]}],"exclude":[{"or":[{"data_source_id":16,"cats":[2905]}]}]},"age":[],"gender":[],"geo":{"include":[237],"exclude":[352261,196605],"radii_include":[],"radii_exclude":[]}}',
         1)
    ]


def generate_expected_test_tpa_segment():
    return [
        (1, '{"interest": {"include": [{"or": [{"data_source_id": 11, "cats": [100, 101, 110, 111, 112, 113, 114]}]}, {"or": [{"data_source_id": 11, "cats": [100, 101, 110, 111, 112, 113]}]}, {"or": [{"data_source_id": 11, "cats": [100, 101]}]}, {"or": [{"data_source_id": 11, "cats": [110, 111, 112, 113, 114]}]}], "exclude": [{"or": [{"data_source_id": 11, "cats": [110, 111, 112, 113]}]}]}, "age": [], "gender": [], "geo": {"include": [491], "exclude": [187347], "radii_include": [], "radii_exclude": []}}', 1),
        (2, '{{"interest": {"include": [{"or": [{"data_source_id": 2, "cats": [205591]}]}, {"or": [{"data_source_id": 14, "cats": [1]}]}], "exclude": [{"or": [{"data_source_id": 8, "cats": [1598, 1601, 1600, 1599, 1602]}, {"data_source_id": 16, "cats": [2488]}]}]}, "age": [], "gender": [], "geo": {"include": [237], "exclude": [352261, 290813, 335866, 196605], "radii_include": [], "radii_exclude": []}}}', 1),
        (3, '{{"interest": {"include": [{"or": [{"data_source_id": 2, "cats": [205591]}]}, {"or": [{"data_source_id": 14, "cats": [1]}]}], "exclude": [{"or": [{"data_source_id": 8, "cats": [1598, 1601, 1600, 1599, 1602]}, {"data_source_id": 16, "cats": [2488]}]}]}, "age": [], "gender": [], "geo": {"include": [237], "exclude": [352261, 290813, 335866, 196605], "radii_include": [], "radii_exclude": []}}}', 1)
    ]


def validate_test_tpa_segment(generated_expression, expected_expression):
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


def validate_segments(generated_expressions):
    expected_segment = generate_expected_test_tpa_segment()
    for index in range(0, len(expected_segment)):
        current_segment = expected_segment[index]
        segment = current_segment[1]
        validate_test_tpa_segment(generated_expressions[index], segment)


####################################
#         END TEST FUNCTIONS       #
####################################
def get_batch_tpa_audience_size(expressions, eval_call_counter):
    try:
        url = 'http://a48e0dc5459e74ed9a460f718d19f2e8-a863d546fb12e42c.elb.us-west-2.amazonaws.com/eval_batch'
        json_payload = json.dumps(expressions)
        headers = {
            'Content-Type': 'application/json',
            'Host': 'audience-service-prod.core-prod.k8.steelhouse.com'
        }
        response = requests.post(url, data=json_payload, headers=headers)

        if response.status_code == 200:
            return json.loads(response.text)
        else:
            if response.status_code == 400:
                print('Request failed with status code 400: %s' % response.text)
            if response.status_code == 503 or response.status_code == 500:
                print('Request failed with status code 5XX: %s' % response.text)
                time.sleep(5)
                print('Retrying batch request')
                eval_call_counter += 1
                if eval_call_counter < 2:
                    get_batch_tpa_audience_size(expressions, eval_call_counter)
                else:
                    print(f"url: {url}, headers: {headers}")
            return None
    except Exception as batch_tpa_audience_size_exception:
        traceback.print_exc()
        print('Request Failed: %s' % batch_tpa_audience_size_exception)


def query_tpa_segments():
    sql = "select distinct  \
                a.segment_id, \
                a.expression, \
                c.advertiser_id \
            from audience.audience_segments a \
            join campaigns c on c.campaign_id = a.campaign_id \
            join advertisers ad on ad.advertiser_id = c.advertiser_id \
            where expression like \'%data_source_id\":1,%' \
            and a.campaign_id in ( \
                select vc.campaign_id \
                from dso.valid_campaigns vc \
            ) \
            and a.expression_type_id = 2 \
            group by 1,2,3 \
            "
    int_prod_cursor = intprod_conn.cursor()
    int_prod_cursor.execute(sql)
    return int_prod_cursor.fetchall()


def insert_into_oracle_segment_sizes(segment_id, audience_size, oracle_expression):
    int_prod_cursor = intprod_conn.cursor()
    data = (segment_id, audience_size, str(oracle_expression))
    int_prod_cursor.execute(
        'insert into test.oracle_segment_sizes (segment_id, audience_size, expression) values (%s, %s, %s)', data)
    intprod_conn.commit()


def insert_into_liveramp_segment_sizes(segment_id, audience_size, liveramp_expression):
    int_prod_cursor = intprod_conn.cursor()
    data = (segment_id, audience_size, str(liveramp_expression))
    int_prod_cursor.execute(
        'insert into test.liveRamp_segment_sizes (segment_id, audience_size, expression) values (%s, %s, %s)', data)
    intprod_conn.commit()


def get_oracle_live_ramp_mapping():
    sql = ("select data_source_category_id, liveramp_segment_id from tpa.oracle_liveramp_mapping "
           "where liveramp_segment_id is not null")
    core_dw_cursor = redshift_coredw_conn.cursor()
    core_dw_cursor.execute(sql)
    output_rows = core_dw_cursor.fetchall()
    return dict(output_rows)


def replace_oracle_cats_with_live_ramp_cats(oracle_expression):
    oracle_cats = set()
    try:
        expression_dict = json.loads(oracle_expression)
    except ValueError:
        return oracle_cats

    if expression_dict["interest"]:
        for include_exclude in ["include", "exclude"]:
            if expression_dict["interest"][include_exclude]:
                for include_exclude_statement in expression_dict["interest"][include_exclude]:
                    if include_exclude_statement["or"]:
                        has_live_ramp_data = has_live_ramp_data_source(include_exclude_statement["or"])
                        new_cats_or_clause = []
                        for or_clause_data in include_exclude_statement["or"]:
                            # check the or clause for oracle data source
                            if or_clause_data["data_source_id"] == 1:
                                # if the clause has oracle data, map the oracle cats to the live ramp cats
                                for cat in or_clause_data["cats"]:
                                    if cat in oracle_live_ramp_mapping:
                                        new_cats_or_clause.append(oracle_live_ramp_mapping[cat])
                                # if there is no live ramp data in the or clause, add the live ramp data and change the
                                # data source id to liveramp (11)
                                if not has_live_ramp_data:
                                    or_clause_data["cats"] = new_cats_or_clause
                                    or_clause_data["data_source_id"] = 11
                        if has_live_ramp_data:
                            # add mapped oracle cats to the existing live ramp data source
                            update_live_ramp_data_source(include_exclude_statement["or"], new_cats_or_clause)
                            remove_oracle_data_source(include_exclude_statement["or"])
                        remove_empty_data_source(include_exclude_statement["or"])
                    remove_empty_or_clause(expression_dict["interest"][include_exclude])
    return json.dumps(expression_dict)


def has_live_ramp_data_source(or_statement):
    for or_clause_data in or_statement:
        if or_clause_data["data_source_id"] == 11:
            return True
    return False


def update_live_ramp_data_source(or_statement, new_cats_or_clause):
    for or_clause_data in or_statement:
        if or_clause_data["data_source_id"] == 11:
            or_clause_data["cats"].extend(new_cats_or_clause)


def remove_oracle_data_source(or_statement):
    for or_clause_data in or_statement:
        if or_clause_data["data_source_id"] == 1:
            or_statement.remove(or_clause_data)


def remove_empty_data_source(or_statement):
    for or_clause_data in or_statement:
        if not or_clause_data["cats"]:
            or_statement.remove(or_clause_data)


def remove_empty_or_clause(include_exclude_statement):
    for or_clause in include_exclude_statement:
        if not or_clause["or"]:
            include_exclude_statement.remove(or_clause)


def truncate_segment_sizes_tables():
    int_prod_cursor = intprod_conn.cursor()
    int_prod_cursor.execute('truncate table test.oracle_segment_sizes')
    int_prod_cursor.execute('truncate table test.liveramp_segment_sizes')
    intprod_conn.commit()


def check_array_length_match(size, expressions):
    if len(size) != len(expressions):
        print("ERROR: Batch Size Mismatch")
        print("Expressions length: %s" % len(expressions))
        print("Size Array length: %s" % len(size))
        print("Expressions: %s" % expressions)
        print("Size Array: %s" % size)
        return False
    return True


def process_expression_batch(expressions, is_oracle=True):
    expression_array = [expression["payload"] for expression in expressions]
    tpa_audience_size = get_batch_tpa_audience_size(expression_array, 0)
    if tpa_audience_size is not None and check_array_length_match(tpa_audience_size, expressions):
        for index in range(0, len(tpa_audience_size)):
            segment_id = expressions[index]["segmentId"]
            id_count = tpa_audience_size[index]["id_count"]
            expression = expressions[index]["payload"]["expression"]
            if is_oracle:
                insert_into_oracle_segment_sizes(segment_id, id_count, expression)
            else:
                insert_into_liveramp_segment_sizes(segment_id, id_count, expression)


def create_eval_request_object(segment_id, expression, expression_type_id, advertiser_id):
    """
    Uses the fields of the class EvalRequest to create a dictionary object

    data class EvalRequest(
        var expression: String,
        val expressionTypeId: Int,
        val advertiserId: Int,
        val audienceId: Int?,
        val campaignGroupId: Int?)

    :param segment_id:
    :param expression:
    :param expression_type_id:
    :param advertiser_id:
    :return:
    """
    return {"segmentId": segment_id,
            "payload": {
                "expression": expression,
                "expressionTypeId": expression_type_id,
                "advertiserId": advertiser_id
            }
            }


if __name__ == '__main__':

    truncate_segment_sizes_tables()
    # store oracle to liveramp mapping in global
    if IS_TEST:
        oracle_live_ramp_mapping = {1: 110, 2: 111, 3: 112, 4: 113, 5: 114}
    else:
        oracle_live_ramp_mapping = get_oracle_live_ramp_mapping()

    ###########################
    #   HANDLE TPA SEGMENTS   #
    ###########################
    if IS_TEST:
        tpa_segment_rows = generate_test_tpa_segment()
    else:
        tpa_segment_rows = query_tpa_segments()

    tpa_oracle_expressions = []
    expression_counter = 0
    tpa_live_ramp_expressions = []
    tpa_segment_total_counter = len(tpa_segment_rows)

    print("Calculating TPA audience sizes for %s segments" % len(tpa_segment_rows))
    for row in tpa_segment_rows:
        try:
            tpa_segment_id = row[0]
            tpa_expression = row[1]
            tpa_expression_type_id = 2  # always 2 for TPA
            tpa_advertiser_id = row[2]

            tpa_oracle_expression = tpa_expression

            # Store Oracle Expression for batch processing
            tpa_oracle_expressions.append(create_eval_request_object(tpa_segment_id, tpa_oracle_expression,
                                                                     tpa_expression_type_id, tpa_advertiser_id))

            # Generate LiveRamp Expressions and Store it for batch processing
            tpa_live_ramp_expression = replace_oracle_cats_with_live_ramp_cats(tpa_expression)
            tpa_live_ramp_expressions.append(create_eval_request_object(tpa_segment_id, tpa_live_ramp_expression,
                                                                        tpa_expression_type_id, tpa_advertiser_id))
            expression_counter += 1

            if expression_counter == BATCH_SIZE:
                # Calculate audience size and insert into liveramp_segment_sizes using batch processing
                process_expression_batch(tpa_live_ramp_expressions, False)
                tpa_live_ramp_expressions = []

                # Calculate audience size and insert into oracle_segment_sizes using batch processing
                process_expression_batch(tpa_oracle_expressions, True)
                tpa_oracle_expressions = []

                expression_counter = 0

        except Exception as exception:
            traceback.print_exc()
            print('%s: %s' % (row, exception), file=sys.stderr)

        # Report progress
        if tpa_segment_total_counter % PROGRESS_REPORT_FREQUENCY == 0:
            print("TPA audiences remaining: %s" % tpa_segment_total_counter)
        tpa_segment_total_counter -= 1

    # Process remaining batch items
    try:
        if expression_counter > 0:
            if IS_TEST:
                print("Validating test expressions")
                validate_segments([expression["payload"]["expression"] for expression in tpa_live_ramp_expressions])
            else:
                process_expression_batch(tpa_live_ramp_expressions, False)
                process_expression_batch(tpa_oracle_expressions, True)
    except Exception as exception:
        traceback.print_exc()
        print('%s' % exception, file=sys.stderr)

    print("TPA audience sizes calculated for %s segments" % len(tpa_segment_rows))
