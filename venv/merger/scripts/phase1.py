import csv
import psycopg2
import configparser
import pandas as pd
from common import *

config = configparser.ConfigParser()
config.read('../configs/config.ini')
pd.set_option("mode.chained_assignment", None)

columns = ["goal_id", "transaction_id", "trade_type", "foi_goal_id", "kristal_subscription_goal_id"]

try:
    connection = psycopg2.connect(host=config['postgresDB']['host'],
                                  user=config['postgresDB']['user'],
                                  password=config['postgresDB']['pass'],
                                  port=config['postgresDB']['port'],
                                  database=config['postgresDB']['db'])

    # Open and read the file as a single buffer
    fd = open('../files/in/select_txns.sql', 'r')
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(';')
    row_count = postgresql_to_row_count(connection, sqlCommands[0])

    dfak = postgresql_to_dataframe(connection, sqlCommands[5],
                                   columns=['kristal_subscription_id', 'skey', 'fund_id', 'fkey'])
    dfak.head()
    print("Total FundAsset Subs : ", len(dfak))
    dfak.to_csv('../files/itd/fund_ksub.csv', encoding='utf-8', index=False, header=True,
                columns=['kristal_subscription_id', 'skey', 'fund_id', 'fkey'])

    dfg = postgresql_to_dataframe(connection, sqlCommands[4], columns=['goal_id'])
    dfg.head()
    print("Total goals : ", len(dfg))
    dfg.to_csv('../files/itd/all_goals.csv', encoding='utf-8', index=False, header=True, columns=["goal_id"])

    df = postgresql_to_dataframe(connection, sqlCommands[1], columns)
    df.head()
    dup_df = df[df.duplicated('transaction_id', keep=False)]

    unmapped_df = postgresql_to_dataframe(connection, sqlCommands[2],
                                          ["trade_type", "transaction_id", "foi_goal_id", "derived_goal_id",
                                           "txn_key", "goal_key"])
    unmapped_df.head()

    print("Original Transactions : ", row_count)
    print("Transactions View: ", len(df))
    print("Duplicate Transactions : ", len(dup_df))
    print("Unmapped Transactions : ", len(unmapped_df))

    itd_df = df.drop_duplicates(subset='transaction_id', keep=False)
    itd_df.drop(itd_df[(itd_df['foi_goal_id'].isnull()) & (itd_df['kristal_subscription_goal_id'].isnull())
                       & ((itd_df['trade_type'] == 'BUY') | (itd_df['trade_type'] == 'SELL')
                          | (itd_df['trade_type'] == 'ASSET_IN')
                          | (itd_df['trade_type'] == 'ASSET_OUT'))].index, inplace=True)
    df_final = itd_df.T.groupby(level=0).first().T
    df_final.to_csv('../files/itd/mapped_txns.csv', encoding='utf-8', index=False, header=True,
                    columns=["transaction_id", "goal_id"])

    print("Net Count: ", len(df), "-", (str('(') + str(len(dup_df)) + str('+') + str(len(unmapped_df)) + str(')')
                                        + str(' = ') + str(len(df_final))))


finally:
    if connection:
        connection.close()
        print("PostgreSQL connection is closed")
