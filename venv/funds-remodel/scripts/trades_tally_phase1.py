import csv
import psycopg2
import configparser
import pandas as pd
from common import *

config = configparser.ConfigParser()
config.read('../configs/config.ini')
pd.set_option("mode.chained_assignment", None)

columns = ["goal_id", "transaction_id", "trade_type", "foi_goal_id", "kristal_subscription_goal_id",
           "kristal_id", "asset_id"]

print("Started executing trades_tally_phase1.py!!!")

try:
    connection = psycopg2.connect(host=config['postgresDB']['host'],
                                  user=config['postgresDB']['user'],
                                  password=config['postgresDB']['pass'],
                                  port=config['postgresDB']['port'],
                                  database=config['postgresDB']['db'])

    # Open and read the file as a single buffer
    fd = open('../files/in/trade_tally_queries.sql', 'r')
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(';')
    total_txn_count = postgresql_to_row_count(connection, sqlCommands[0])

    dfg_all = postgresql_to_dataframe(connection, sqlCommands[1], columns=["goal_id", "kristal_id"])
    dfg_all.to_csv('../files/itd/all_goals.csv', encoding='utf-8', index=False, header=True,
                   columns=["goal_id", "kristal_id"])

    dfg_unapproved = postgresql_to_dataframe(connection, sqlCommands[7], columns=["goal_id", "kristal_id"])
    dfg_unapproved.to_csv('../files/itd/unapproved_goals.csv', encoding='utf-8', index=False, header=True,
                          columns=["goal_id", "kristal_id"])

    df = postgresql_to_dataframe(connection, sqlCommands[2], columns)

    dup_df = df[df.duplicated('transaction_id', keep=False)]
    dup_df.to_csv('../files/itd/new_duplicate_txns.csv', encoding='utf-8', index=False, header=True,
                  columns=["transaction_id", "goal_id", "kristal_id", "asset_id"])

    unmapped_df = df.loc[(df['foi_goal_id'].isnull()) & (df['kristal_subscription_goal_id'].isnull())
                         & ((df['trade_type'] == 'BUY') | (df['trade_type'] == 'SELL')
                            | (df['trade_type'] == 'ASSET_IN')
                            | (df['trade_type'] == 'ASSET_OUT'))]
    unmapped_df.to_csv('../files/itd/new_unmapped_txns.csv', encoding='utf-8', index=False, header=True,
                       columns=["transaction_id", "goal_id", "kristal_id", "asset_id"])

    print("Transactions Count : ", total_txn_count)
    print("Transactions in ViewCount: ", len(df))
    print("Duplicate Transactions in ViewCount : ", int(len(dup_df)))
    print("Unmapped Transactions in ViewCount : ", len(unmapped_df))

    itd_df = df.drop_duplicates(subset='transaction_id', keep=False)
    itd_df.drop(itd_df[(itd_df['foi_goal_id'].isnull()) & (itd_df['kristal_subscription_goal_id'].isnull())
                       & ((itd_df['trade_type'] == 'BUY') | (itd_df['trade_type'] == 'SELL')
                          | (itd_df['trade_type'] == 'ASSET_IN')
                          | (itd_df['trade_type'] == 'ASSET_OUT'))].index, inplace=True)
    df_final = itd_df.T.groupby(level=0).first().T
    df_final.to_csv('../files/itd/mapped_txns.csv', encoding='utf-8', index=False, header=True,
                    columns=["transaction_id", "goal_id", "kristal_id", "asset_id"])

    print("\n")
    print("Stats Verification!!")
    print("Net ViewCount: ", len(df), "-", (str('(') + str(len(dup_df)) + str('+') + str(len(unmapped_df)) + str(')')
                                            + str(' = ') + str(len(df_final))))


finally:
    if connection:
        connection.close()
        print("Finished executing trades_tally_phase1.py!!!")
