import pandas as pd
import configparser
from common import *
import math
import gzip

config = configparser.ConfigParser()
config.read('../configs/config.ini')
fd = open('../files/in/trade_tally_queries.sql', 'r')
sqlFile = fd.read()
fd.close()
sqlCommands = sqlFile.split(';')
insert_query_full = sqlCommands[6]
insert_query_txn_null = sqlCommands[5]
insert_query_ksg_null = sqlCommands[4]

# Process chunks of data to avoid memory overload
chunk_size = 10000  # Adjust chunk size based on memory

connection = psycopg2.connect(host=config['postgresDB']['host'],
                              user=config['postgresDB']['user'],
                              password=config['postgresDB']['pass'],
                              port=config['postgresDB']['port'],
                              database=config['postgresDB']['db'])

errors = open("../files/out/trades_tally_errors.txt", "w", encoding="utf-8")
print("Started executing trades_tally_final.py!!!")
kaMap = load_ka_to_map("../files/itd/kristal_to_asset.csv")
akMap = load_ak_to_map("../files/itd/asset_to_kristal.csv")
gkMap = load_gk_to_map("../files/itd/all_goals.csv")


def process(chunks):
    total_rows = 0
    full_inserts = 0
    txn_null_inserts = 0
    ksg_null_inserts = 0
    for row in chunks.itertuples(index=False):

        total_rows += 1

        if (row.goal_id is not None and row.transaction_id is not None
                and (not math.isnan(row.goal_id) and not math.isnan(row.transaction_id))):
            full_inserts += 1
            try:
                insert_psql.write(insert_query_full.format(int(row.goal_id), int(row.transaction_id)) + " ; ")
            except Exception as e:
                errors.write(f"TransactionId - {int(row.transaction_id)} , GoalId - {int(row.goal_id)}  An unexpected "
                             f"error occurred: {e}" + "\n")

        elif row.goal_id is not None and (not math.isnan(row.goal_id)):
            txn_null_inserts += 1
            kId = gkMap.get(int(row.goal_id))
            aId = None
            if kId is not None:
                aId = kaMap.get(int(kId))
            try:
                insert_psql.write(insert_query_txn_null.format(add_quotes(aId), int(row.goal_id)) + " ; ")
            except Exception as e:
                errors.write(f"TransactionId - null , GoalId - {int(row.goal_id)}  An unexpected error "
                             f"occurred: {e}" + "\n")

        elif row.transaction_id is not None and (not math.isnan(row.transaction_id)):
            ksg_null_inserts += 1
            kId = akMap.get(int(row.asset_id))
            try:
                insert_psql.write(insert_query_ksg_null.format(add_quotes(kId), int(row.transaction_id)) + " ; ")
            except Exception as e:
                errors.write(f"TransactionId - {int(row.transaction_id)} , GoalId - null  An unexpected error "
                             f"occurred: {e}" + "\n")
    print(f"Processed {total_rows} rows: {full_inserts} full inserts, {txn_null_inserts} txn_null inserts, "
          f"{ksg_null_inserts} ksg_null inserts")


with gzip.open("../files/out/trades_tally_inserts.sql.gz", "wt", encoding="utf-8") as insert_psql:
    for chunk in pd.read_csv('../files/itd/all_trades_and_goals.csv', chunksize=chunk_size):
        print(f"Processing chunk with {len(chunk)} rows")
        process(chunk)
insert_psql.close()
errors.close()
print("Finished executing trades_tally_final.py!!!")
