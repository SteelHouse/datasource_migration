import psycopg2


def execute_database_query():
    sql = """
        SELECT a.audience_id, a.expression, adv.advertiser_id, adv.company_name
        FROM audience.audiences a
        INNER JOIN public.advertisers adv USING (advertiser_id)
        WHERE expression_type_id = 2
        and a.expression ~ '.*"data_source_id":\w?11\w?[,}].*'::text
        ORDER BY a.audience_id;
    """
    conn = None
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host="integration-dev.crvrygavls2u.us-west-2.rds.amazonaws.com",
            port=5432,
            dbname="qacoredb",
            user="qacore",
            password="qa#core07#19"
        )

        # Create a cursor with dictionary cursor factory
        cursor = conn.cursor()
        # Execute the query
        cursor.execute(sql)
        results = cursor.fetchall()
        print(f"Found {len(results)} audiences to process")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        raise
    finally:
        # Close the connection
        if conn is not None:
            conn.close()