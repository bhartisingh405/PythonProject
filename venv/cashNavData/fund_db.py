import psycopg2
import configparser
import csv
from psycopg2 import sql


def fetch_data_from_tables():
    config = configparser.ConfigParser()
    config.read('./config.ini')
    conn = psycopg2.connect(
        host=config['postgresDB']['host'],
        user=config['postgresDB']['user'],
        password=config['postgresDB']['pass'],
        port=config['postgresDB']['port'],
        database=config['postgresDB']['db']
    )
    output_csv_file = "./fund_results.csv"
    table_list= ["funds_investo2o.user_account_history_p_2024_12_17","funds_investo2o.user_account_history_p_2024_12_16",
                  "funds_investo2o.user_account_history_p_2024_12_15","funds_investo2o.user_account_history_p_2024_12_14",
                  "funds_investo2o.user_account_history_p_2024_12_13","funds_investo2o.user_account_history_p_2024_12_12",
                  "funds_investo2o.user_account_history_p_2024_12_11","funds_investo2o.user_account_history_p_2024_12_10",
                  "funds_investo2o.user_account_history_p_2024_12_09","funds_investo2o.user_account_history_p_2024_12_08",
                  "funds_investo2o.user_account_history_p_2024_12_07","funds_investo2o.user_account_history_p_2024_12_06",
                  "funds_investo2o.user_account_history_p_2024_12_05","funds_investo2o.user_account_history_p_2024_12_04",
                  "funds_investo2o.user_account_history_p_2024_12_03","funds_investo2o.user_account_history_p_2024_12_02",
                  "funds_investo2o.user_account_history_p_2024_12_01","funds_investo2o.user_account_history_p_2024_11_30",
                  "funds_investo2o.user_account_history_p_2024_11_29","funds_investo2o.user_account_history_p_2024_11_28",
                  "funds_investo2o.user_account_history_p_2024_11_27","funds_investo2o.user_account_history_p_2024_11_26",
                  "funds_investo2o.user_account_history_p_2024_11_25","funds_investo2o.user_account_history_p_2024_11_24",
                  "funds_investo2o.user_account_history_p_2024_11_23","funds_investo2o.user_account_history_p_2024_11_22",
                  "funds_investo2o.user_account_history_p_2024_11_21","funds_investo2o.user_account_history_p_2024_11_20",
                  "funds_investo2o.user_account_history_p_2024_11_19","funds_investo2o.user_account_history_p_2024_11_18",
                  "funds_investo2o.user_account_history_p_2024_11_17","funds_investo2o.user_account_history_p_2024_11_16",
                  "funds_investo2o.user_account_history_p_2024_11_15","funds_investo2o.user_account_history_p_2024_11_14",
                  "funds_investo2o.user_account_history_p_2024_11_13","funds_investo2o.user_account_history_p_2024_11_12",
                  "funds_investo2o.user_account_history_p_2024_11_11","funds_investo2o.user_account_history_p_2024_11_10",
                  "funds_investo2o.user_account_history_p_2024_11_09","funds_investo2o.user_account_history_p_2024_11_08",
                  "funds_investo2o.user_account_history_p_2024_11_07","funds_investo2o.user_account_history_p_2024_11_06",
                  "funds_investo2o.user_account_history_p_2024_11_05","funds_investo2o.user_account_history_p_2024_11_04",
                  "funds_investo2o.user_account_history_p_2024_11_03","funds_investo2o.user_account_history_p_2024_11_02",
                  "funds_investo2o.user_account_history_p_2024_11_01","funds_investo2o.user_account_history_p_2024_10_31",
                  "funds_investo2o.user_account_history_p_2024_10_30","funds_investo2o.user_account_history_p_2024_10_29",
                  "funds_investo2o.user_account_history_p_2024_10_28","funds_investo2o.user_account_history_p_2024_10_27",
                  "funds_investo2o.user_account_history_p_2024_10_26","funds_investo2o.user_account_history_p_2024_10_25",
                  "funds_investo2o.user_account_history_p_2024_10_24","funds_investo2o.user_account_history_p_2024_10_23",
                  "funds_investo2o.user_account_history_p_2024_10_22","funds_investo2o.user_account_history_p_2024_10_21",
                  "funds_investo2o.user_account_history_p_2024_10_20","funds_investo2o.user_account_history_p_2024_10_19",
                  "funds_investo2o.user_account_history_p_2024_10_18","funds_investo2o.user_account_history_p_2024_10_17"
                  ]

    cursor = conn.cursor()

    for table_name in table_list:
        try:
            if '.' in table_name:
                schema, table = table_name.split('.')
                formatted_table_name = sql.Identifier(str(schema), str(table))  # Ensure they are strings
            else:
                formatted_table_name = sql.Identifier(str(table_name))  # Ensure table name is a string

            query = sql.SQL("""
                SELECT 
                    (account_response->>'lastUpdated')::text AS history_date, 
                    (account_response->>'userId')::text AS user_id, 
                    (account_response->>'accountId')::text AS account_id, 
                    (account_response->>'nickname')::text AS nickname, 
                    (account_response->'accountBreakUp'->'cash'->>'nav')::text AS cash_nav, 
                    (account_response->'accountBreakUp'->'cash'->'asssetTypeBreakUp'->0->>'navTime')::text AS nav_time,  
                    (account_response->>'currency')::text AS account_currency 
                FROM {table_name} 
                WHERE 
                    account_response IS NOT NULL 
                    AND account_response->'accountBreakUp' IS NOT NULL  
                    AND account_response->'accountBreakUp'->'cash' is not null   
            """).format(table_name=formatted_table_name)

            cursor.execute(query, ('{}', '{}', '{}'))  # Ensure the placeholders match the expected values

            if cursor.description is None:
                print(f"Skipping table '{table_name}': No columns found.")
                continue

            rows = cursor.fetchall()

            if not rows:
                print(f"Table '{table_name}' is empty. Creating CSV with only headers.")

            column_names = [desc[0] for desc in cursor.description]
            with open(output_csv_file, mode="w", newline="") as file:
                csv_writer = csv.writer(file)
                csv_writer.writerow(column_names)
                csv_writer.writerows(rows)

            print(f"Data from table '{table_name}' successfully written to '{output_csv_file}'")

        except Exception as e:
            print(f"Error processing table '{table_name}': {e}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    fetch_data_from_tables()

