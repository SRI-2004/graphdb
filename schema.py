import psycopg2
import json

def get_table_schema(conn, table_name, schema='public'):
    """
    Retrieve the column details (name, data type, and nullability)
    for a specified table from the given schema.
    """
    query = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position;
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table_name))
        columns = cur.fetchall()
    return columns

def main():
    # Update these variables with your Postgres connection info.
    conn_params = {
        "database": "defaultdb",
        "user": "avnadmin",
        "password": "AVNS_yxwIBw5haAiSockuoja",
        "host": "pg-243dee0d-srinivasansridhar918-e25a.k.aivencloud.com",  # Example: "pg.example.aivencloud.com"
        "port": "28021"
    }
    
    # List the table names you want to inspect
    tables_to_inspect = [
        "account_performance_report",
        "ad_group",
        "ad_group_ad",
        "ad_group_ad_label",
        "ad_group_criterion",
        "audience",
        "campaign",
        "campaign_budget",
        "campaign_criterion",
        "campaign_label",
        "click_view",
        "customer",
        "customer_label",
        "geographic_view",
        "keyword_view",
        "label",
        "shopping_performance_view",
        "topic_view",
        "user_interest",
        "user_location_view",
        "ad_group_ad_legacy",
        "ad_group_bidding_strategy"
    ]
    
    schema_output = {}

    try:
        conn = psycopg2.connect(**conn_params)
        print("Connected to Postgres successfully.")
        
        for table in tables_to_inspect:
            columns = get_table_schema(conn, table)
            # Create a list of dictionaries for each column entry.
            column_list = [
                {
                    "column_name": col[0],
                    # "data_type": col[1],
                    # "is_nullable": col[2]
                } for col in columns
            ]
            schema_output[table] = column_list
        
    except Exception as e:
        print("Error connecting to Postgres:", e)
        return
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Connection closed.")

    # Write the schema output to a JSON file.
    with open("schema.json", "w") as json_file:
        json.dump(schema_output, json_file, indent=4)
    print("Schema information written to 'schema.json'.")

if __name__ == "__main__":
    main()