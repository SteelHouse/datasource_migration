from python.utils.db_util import execute_fetch_all_with_vars_query

"""
data_source_id,name
1,Oracle
11,LiveRamp
17,ShareThis
18,Dstillery
20,OnAudience
22,Experian
24,Justuno
25,5x5
26,sharethis_predactiv
27,LaunchLabs
28,33Across
29,deepsync
32,CDK
33,Ophelia's Calendar
35,LiveRamp IP
36,Cybba
37,CallRail
"""

def get_data_source_table(data_source_id):
    table_name = ""
    if data_source_id == get_data_source_id('Oracle'):
        table_name = 'oracle_categories'
    elif data_source_id == get_data_source_id('LiveRamp'):
        table_name = 'liveramp_categories'
    elif data_source_id == get_data_source_id('Experian'):
        table_name = 'experian_categories'
    elif data_source_id == get_data_source_id('ShareThis'):
        table_name = 'sharethis_categories'
    elif data_source_id == get_data_source_id('Dstillery'):
        table_name = 'dstillery_categories'
    elif data_source_id == get_data_source_id('OnAudience'):
        table_name = 'onaudience_categories'
    else:
        raise Exception(f"source table not found for data_source_id {data_source_id}")
    return table_name

def get_data_source_id(data_source_name):
    if data_source_name == 'Dstillery':
        return 18 # Dstillery was deleted from data sources already.
    try:
        return execute_fetch_all_with_vars_query("""
            select data_source_id from audience.data_sources where name = %s
        """, (data_source_name,))[0][0]
    except Exception as e:
        print(f"Error fetching data source ID for {data_source_name}: {e}")
        return None