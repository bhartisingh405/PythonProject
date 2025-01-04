import csv
import psycopg2
import configparser
import pandas as pd
from common import *

config = configparser.ConfigParser()
config.read('../configs/config.ini')
pd.set_option("mode.chained_assignment", None)

columns = ["goal_id", "transaction_id", "trade_type", "foi_goal_id", "kristal_subscription_goal_id"]
known_duplicated_txnIds = frozenset(
    [712066, 712075, 712072, 712069, 632537, 632567, 460758, 460266, 496504, 496521, 496552,
     496496, 182206, 181416, 668235, 668236, 132824, 132820, 159246, 668212, 668201, 132819,
     132823, 132825, 132821])
known_unmapped_txnIds = frozenset(
    [720479, 721242, 491563, 726386, 687997, 684427, 677923, 684792, 696608, 718735, 677920, 491556,
     726690, 682397, 119042, 685091, 165975, 727342, 384144, 726691])
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

    dfg = postgresql_to_dataframe(connection, sqlCommands[1], columns=['goal_id'])
    dfg.to_csv('../files/itd/all_goals.csv', encoding='utf-8', index=False, header=True, columns=["goal_id"])
    df = postgresql_to_dataframe(connection, sqlCommands[2], columns)

    dup_df = df[df.duplicated('transaction_id', keep=False)]
    db_duplicated_txnIds = set(dup_df['transaction_id'].unique())
    new_duplicated_txns_discovered = db_duplicated_txnIds - known_duplicated_txnIds
    with open("../files/itd/new_duplicated_txns.csv", encoding='utf-8', mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(new_duplicated_txns_discovered)

    unmapped_df = df.loc[(df['foi_goal_id'].isnull()) & (df['kristal_subscription_goal_id'].isnull())
                         & ((df['trade_type'] == 'BUY') | (df['trade_type'] == 'SELL')
                            | (df['trade_type'] == 'ASSET_IN')
                            | (df['trade_type'] == 'ASSET_OUT'))]
    db_unmapped_txnIds = set(unmapped_df['transaction_id'].unique())
    new_unmapped_txns_discovered = db_unmapped_txnIds - known_unmapped_txnIds
    with open("../files/itd/new_unmapped_txns.csv", encoding='utf-8', mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(new_unmapped_txns_discovered)

    print("Transactions Count : ", total_txn_count)
    print("Transactions in ViewCount: ", len(df))
    print("Duplicate Transactions in ViewCount : ", len(dup_df))
    print("Unmapped Transactions in ViewCount : ", len(unmapped_df))

    itd_df = df.drop_duplicates(subset='transaction_id', keep=False)
    itd_df.drop(itd_df[(itd_df['foi_goal_id'].isnull()) & (itd_df['kristal_subscription_goal_id'].isnull())
                       & ((itd_df['trade_type'] == 'BUY') | (itd_df['trade_type'] == 'SELL')
                          | (itd_df['trade_type'] == 'ASSET_IN')
                          | (itd_df['trade_type'] == 'ASSET_OUT'))].index, inplace=True)
    df_final = itd_df.T.groupby(level=0).first().T
    df_final.to_csv('../files/itd/mapped_txns.csv', encoding='utf-8', index=False, header=True,
                    columns=["transaction_id", "goal_id"])

    print("\n")
    print("Stats Verification!!")
    print("Net ViewCount: ", len(df), "-", (str('(') + str(len(dup_df)) + str('+') + str(len(unmapped_df)) + str(')')
                                            + str(' = ') + str(len(df_final))))
    print("Net OriginalCount: ", total_txn_count, "-",
          (str('(') + str(int(len(dup_df) / 2)) + str('+') + str(len(unmapped_df))
           + str(')') + str(' = ') + str(len(df_final))))


finally:
    if connection:
        connection.close()
        print("PostgreSQL connection is closed")
