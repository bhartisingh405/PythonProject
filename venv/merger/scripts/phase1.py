import csv
import psycopg2
import configparser
import pandas as pd
from common import *

config = configparser.ConfigParser()
config.read('../configs/config.ini')
pd.set_option("mode.chained_assignment", None)

columns = ["goal_id", "transaction_id", "user_id", "user_account_id", "quantity", "asset_id", "custom_asset_id",
           "asset_type",
           "trade_time", "trade_type", "trade_price", "trade_nav", "fees", "taxes", "created_time",
           "external_transaction_id", "fee_currency", "remarks", "proposed_price", "last_updated_time",
           "wm_fx_rate_to_base", "base_currency", "trade_purpose", "original_trade_time", "original_trade_price",
           "original_trade_nav", "original_transaction_id", "accrued_interest", "biz_notes", "notes_updated_time",
           "foi_goal_id", "txn_key", "ttime", "tctime", "kristal_subscription_goal_id", "kristal_subscription_id",
           "user_id", "approved_units", "approved_amount", "create_time", "kristal_execution_account",
           "subscription_date", "subscribed_by", "approved_date", "approved_by", "goal_id", "source_type",
           "audit_details", "unit_price", "cash_in_kristal_per_unit", "total_cost", "asset_wise_cost_map",
           "subscription_pending_execution_state", "unique_id", "last_update_time", "requested_units",
           "requested_amount", "original_request", "lifecycle_state", "bookkeeping_state", "bk_state_mover",
           "fund_remarks", "user_report_id", "fund_bookkeeping", "flux_units", "kristal_id", "investment_rationale",
           "temp_unit_price", "temp_total_cost", "approval_audit", "platform", "mechanism", "activity_uuid",
           "is_transfer", "transaction_fees", "fee", "tax", "original_subscription_date", "original_unit_nav",
           "original_investment_amount", "original_goal_id", "expert_opinion_id", "broker_price", "client_price",
           "execution_date", "settlement_date", "sn_note_size", "spread", "spread_amount", "broker_settlement_amount",
           "sn_net_subscription_amount", "cost_with_fees", "cost_without_fees", "order_fees", "limit_price",
           "order_currency", "dvp_route", "shared_spread_amount", "shared_spread_percentage", "kristal_spread_amount",
           "kristal_spread_percentage", "kristal_access_fees", "nav_date", "payment_date", "internal_cutoff",
           "estimated_dates_audit", "estimated_subscription_dates_id", "estimated_redemption_dates_id",
           "goal_key", "stime", "atime"]

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
    txn_count = sqlCommands[0]
    txn_view = sqlCommands[1]
    orphan_txn_view = sqlCommands[2]
    insert_query_full = sqlCommands[3]
    row_count = postgresql_to_row_count(connection, txn_count)

    df = postgresql_to_dataframe(connection, txn_view, columns)
    df.head()
    dup_df = df[df.duplicated('transaction_id', keep=False)]

    unmapped_df = postgresql_to_dataframe(connection, orphan_txn_view,
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
