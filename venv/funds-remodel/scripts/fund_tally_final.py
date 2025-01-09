import gzip

import numpy as np
import pandas as pd
import configparser
from common import *
import math

config = configparser.ConfigParser()
config.read('../configs/config.ini')
fd = open('../files/in/fund_tally_queries.sql', 'r')
sqlFile = fd.read()
fd.close()
sqlCommands = sqlFile.split(';')
insert_query_full = sqlCommands[6]
insert_query_fund_null = sqlCommands[5]
insert_query_ks_null = sqlCommands[4]

# Process chunks of data to avoid memory overload
chunk_size = 10000  # Adjust chunk size based on memory

connection = psycopg2.connect(host=config['postgresDB']['host'],
                              user=config['postgresDB']['user'],
                              password=config['postgresDB']['pass'],
                              port=config['postgresDB']['port'],
                              database=config['postgresDB']['db'])

errors = open("../files/out/fund_tally_errors.txt", "w", encoding="utf-8")

total_ksub_count = postgresql_to_row_count(connection, sqlCommands[0])
total_asset_count = postgresql_to_row_count(connection, sqlCommands[1])

columns = ['kristal_subscription_id', 'skey', 'fund_id', 'fkey', 'email', 'compliance_mode',
           'is_model_account', 'account_status']
data_frame = postgresql_to_dataframe(connection, sqlCommands[2], columns)
data_frame.to_csv('../files/itd/fund_ksub.csv', encoding='utf-8', index=False, header=True, columns=columns)

print("Started executing fund_tally_generate.py!!!")

print("Total kSubs - ", total_ksub_count)
print("Total fundAssets - ", total_asset_count)
print("Total rows in ViewQuery  -  ", len(data_frame))
print("\n")


def process(chunks):
    total_rows = 0
    full_inserts = 0
    fund_null_inserts = 0
    ks_null_inserts = 0
    for row in chunks.itertuples(index=False):

        if (row.kristal_subscription_id is not None and row.fund_id is not None
                and (not math.isnan(row.kristal_subscription_id) and not math.isnan(row.fund_id))):
            full_inserts += 1
            try:
                insert_psql.write(insert_query_full.format(int(row.kristal_subscription_id), int(row.fund_id)) + " ; ")
            except Exception as e:
                errors.write(f"KsubId - {int(row.kristal_subscription_id)} , FundId - {int(row.fund_id)} An unexpected "
                             f"error occurred: {e}" + "\n")

        elif row.kristal_subscription_id is not None and (not math.isnan(row.kristal_subscription_id)):
            fund_null_inserts += 1
            try:
                insert_psql.write(insert_query_fund_null.format(int(row.kristal_subscription_id)) + " ; ")
            except Exception as e:
                errors.write(f"FundId - null , KsubId - {int(row.kristal_subscription_id)}  An unexpected error "
                             f"occurred: {e}" + "\n")

        elif row.fund_id is not None and (not math.isnan(row.fund_id)):
            ks_null_inserts += 1
            try:
                insert_psql.write(insert_query_ks_null.format(int(row.fund_id)) + " ; ")
            except Exception as e:
                errors.write(f"FundId - {int(row.fund_id)} , GoalId - null  An unexpected error "
                             f"occurred: {e}" + "\n")

    print(f"Processed {total_rows} rows: {full_inserts} full inserts, {fund_null_inserts} fund_null inserts, "
          f"{ks_null_inserts} ks_null inserts")


with gzip.open("../files/out/fund_tally_inserts.sql.gz", "wt", encoding="utf-8") as insert_psql:
    for chunk in pd.read_csv('../files/itd/fund_ksub.csv', chunksize=chunk_size):
        print(f"Processing chunk with {len(chunk)} rows")
        process(chunk)

insert_psql.close()
errors.close()

print("Finished executing fund_tally_generate.py!!!")