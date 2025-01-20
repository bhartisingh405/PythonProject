import gzip
import math
import numpy as np
import pandas as pd
import configparser
import psycopg2
from common import postgresql_to_row_count, postgresql_to_dataframe, load_ka_to_map, load_ak_to_map, add_quotes


def read_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_content = file.read()
    return sql_content.split(';')


def connect_to_database(config):
    return psycopg2.connect(
        host=config['postgresDB']['host'],
        user=config['postgresDB']['user'],
        password=config['postgresDB']['pass'],
        port=config['postgresDB']['port'],
        database=config['postgresDB']['db']
    )


def initialize_output_files():
    error_file = open("../files/out/fund_tally_errors.txt", "w", encoding="utf-8")
    insert_file = gzip.open("../files/out/fund_tally_inserts.sql.gz", "wt", encoding="utf-8")
    return error_file, insert_file


def log_error(errors_file, message):
    errors_file.write(message + "\n")


def process_chunks(chunks, ka_map, ak_map, insert_queries, error_file, insert_file):
    full_insert_count, fund_null_count, ks_null_count = 0, 0, 0

    for row in chunks.itertuples(index=False):
        try:
            if (row.kristal_subscription_id is not None and row.fund_id is not None
                    and not math.isnan(row.kristal_subscription_id) and not math.isnan(row.fund_id)):
                full_insert_count += 1
                insert_file.write(insert_queries['full'].format(int(row.kristal_subscription_id), int(row.fund_id)) + " ;")

            elif row.kristal_subscription_id is not None and not math.isnan(row.kristal_subscription_id):
                fund_null_count += 1
                asset_id = ka_map.get(int(row.kristal_id))
                insert_file.write(insert_queries['fund_null'].format(add_quotes(asset_id), int(row.kristal_subscription_id)) + " ;")

            elif row.fund_id is not None and not math.isnan(row.fund_id):
                ks_null_count += 1
                kristal_id = ak_map.get(int(row.asset_id))
                insert_file.write(insert_queries['ks_null'].format(add_quotes(kristal_id), int(row.fund_id)) + " ;")

        except Exception as e:
            log_error(error_file, f"Error processing row {row}: {e}")

    return full_insert_count, fund_null_count, ks_null_count


def main():
    # Read configuration and SQL commands
    config = read_config('../configs/config.ini')
    sql_commands = read_sql_file('../files/in/fund_tally_queries.sql')

    insert_queries = {
        'full': sql_commands[6],
        'fund_null': sql_commands[5],
        'ks_null': sql_commands[4]
    }

    # Establish database connection
    connection = connect_to_database(config)

    # Load supporting data
    ka_map = load_ka_to_map("../files/itd/kristal_to_asset.csv")
    ak_map = load_ak_to_map("../files/itd/asset_to_kristal.csv")

    # Retrieve data
    columns = ['kristal_subscription_id', 'skey', 'fund_id', 'fkey', 'kristal_id', 'asset_id', 'email', 'compliance_mode',
               'is_model_account', 'account_status']
    data_frame = postgresql_to_dataframe(connection, sql_commands[2], columns)
    data_frame.to_csv('../files/itd/fund_ksub.csv', encoding='utf-8', index=False, header=True, columns=columns)

    # Log dataset statistics
    total_ksub_count = postgresql_to_row_count(connection, sql_commands[0])
    total_asset_count = postgresql_to_row_count(connection, sql_commands[1])

    print("Total kSubs -", total_ksub_count)
    print("Total fundAssets -", total_asset_count)
    print("Total rows in ViewQuery -", len(data_frame))
    print("\n")

    # Initialize output files
    error_file, insert_file = initialize_output_files()

    # Process data in chunks
    chunk_size = 10000
    for chunk in pd.read_csv('../files/itd/fund_ksub.csv', chunksize=chunk_size):
        print(f"Processing chunk with {len(chunk)} rows")
        full, fund_null, ks_null = process_chunks(chunk, ka_map, ak_map, insert_queries, error_file, insert_file)
        print(f"Processed {len(chunk)} rows: {full} full inserts, {fund_null} fund_null inserts, {ks_null} ks_null inserts")

    # Close resources
    error_file.close()
    insert_file.close()
    connection.close()

    print("Finished processing fund_tally_final.py!")


if __name__ == "__main__":
    main()
