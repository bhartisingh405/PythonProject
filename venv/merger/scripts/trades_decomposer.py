import json
import numbers
from datetime import datetime

import numpy as np

import csv
import psycopg2
import configparser
import pandas as pd
from psycopg2.extras import RealDictCursor


config = configparser.ConfigParser()
config.read('./configs/config.ini')
pd.set_option("mode.chained_assignment", None)


def postgresql_to_record(conn, select_query):
    cursorInner = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursorInner.execute(select_query)
        dictRows = cursorInner.fetchall()
        cursorInner.close()
    except (Exception, psycopg2.DatabaseError) as errors:
        print("Error: %s" % errors)
        cursorInner.close()
        return 1
    return dictRows


def postgresql_to_row_count(conn, select_query):
    cursorInner = conn.cursor()
    try:
        cursorInner.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as errors:
        print("Error: %s" % errors)
        cursorInner.close()
        return 1
    data = cursorInner.fetchone()
    cursorInner.close()
    return data[0]


def postgresql_to_dataframe(conn, select_query, columns):
    cursorInner = conn.cursor()
    try:
        cursorInner.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as errors:
        print("Error: %s" % errors)
        cursorInner.close()
        return 1
    tuples = cursorInner.fetchall()
    cursorInner.close()
    df_inner = pd.DataFrame(tuples, columns=columns)
    return df_inner


dateformat = '%Y-%m-%d %H:%M:%S'


def add_quotes(param):
    if param is None or param is np.nan:
        return 'null'
    elif isinstance(param, str):
        return "\'" + param + "\'"
    elif isinstance(param, numbers.Number):
        return param
    elif isinstance(param, datetime):
        return "\'" + param.strftime(dateformat) + "\'"
    elif isinstance(param, dict):
        return "\'" + json.dumps(param) + "\'"


try:
    connection = psycopg2.connect(host=config['postgresDB']['host'],
                                  user=config['postgresDB']['user'],
                                  password=config['postgresDB']['pass'],
                                  port=config['postgresDB']['port'],
                                  database=config['postgresDB']['db'])

    # Open and read the file as a single buffer
    fd = open('./psql/select_queries.sql', 'r')
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(';')
    txn_count = sqlCommands[0]
    txn_view = sqlCommands[1]
    orphan_txn_view = sqlCommands[2]
    insert_query_full = sqlCommands[3]
    insert_query_partial = sqlCommands[4]

    row_count = postgresql_to_row_count(connection, txn_count)

    df = postgresql_to_dataframe(connection, txn_view,
                                 ["trade_type", "transaction_id", "foi_goal_id", "derived_goal_id",
                                  "txn_key", "goal_key"])
    df.head()
    dup_df = df[df.duplicated('transaction_id', keep=False)]
    dup_df.to_csv("./csv/duplicate_txns.csv", encoding='utf-8', index=False, header=True)

    unmapped_df = postgresql_to_dataframe(connection, orphan_txn_view,
                                          ["trade_type", "transaction_id", "foi_goal_id", "derived_goal_id",
                                           "txn_key", "goal_key"])
    unmapped_df.head()
    unmapped_df.to_csv("./csv/unmapped_txns.csv", encoding='utf-8', index=False, header=True)

    print("Original Transactions : ", row_count)
    print("Transactions View: ", len(df))
    print("Duplicate Transactions : ", len(dup_df))
    print("Unmapped Transactions : ", len(unmapped_df))

    itd_df = df.drop_duplicates(subset='transaction_id', keep=False)
    itd_df.drop(itd_df[(itd_df['foi_goal_id'].isnull()) & (itd_df['derived_goal_id'].isnull())
                       & ((itd_df['trade_type'] == 'BUY') | (itd_df['trade_type'] == 'SELL')
                          | (itd_df['trade_type'] == 'ASSET_IN')
                          | (itd_df['trade_type'] == 'ASSET_OUT'))].index, inplace=True)
    itd_df.to_csv('./csv/final_txns.csv', encoding='utf-8', index=False, header=True)

    print("Net Count: ", len(df), "-", (str('(') + str(len(dup_df)) + str('+') + str(len(unmapped_df)) + str(')')
                                        + str(' = ') + str(len(itd_df))))

    insert_psql = open("./psql/insert_trades.sql", "w", encoding="utf-8")

    with open('./csv/final_txns.csv', 'r', newline='') as file:
        reader = csv.reader(file, delimiter=',')
        headings = next(reader)
        insert_statements = ""
        goal = None
        for row in reader:
            if not ''.join(row).strip():
                continue
            txn = postgresql_to_record(connection, "select * from funds_investo2o.transactions where transaction_id = "
                                                   "{}".format(row[1]))
            if row[0] == 'BUY' or row[0] == 'SELL' or row[0] == 'ASSET_IN' or row[0] == 'ASSET_OUT':
                goal = postgresql_to_record(connection, "select * from funds_kristals.kristal_subscription_goal where "
                                                        "kristal_subscription_goal_id = {} ".format(row[2] or row[3]))
            if goal is None:
                insert_statements += insert_query_partial.format(add_quotes(txn[0]['user_id']),
                                                                 add_quotes(txn[0]['user_account_id']),
                                                                 add_quotes(txn[0]['quantity']),
                                                                 add_quotes(txn[0]['asset_id']),
                                                                 add_quotes(txn[0]['custom_asset_id']),
                                                                 add_quotes(txn[0]['asset_type']),
                                                                 add_quotes(txn[0]['trade_time']),
                                                                 add_quotes(txn[0]['trade_type']),
                                                                 add_quotes(txn[0]['trade_price']),
                                                                 add_quotes(txn[0]['trade_nav']),
                                                                 add_quotes(txn[0]['fees']),
                                                                 add_quotes(txn[0]['taxes']),
                                                                 'now()',
                                                                 'now()',
                                                                 add_quotes(txn[0]['external_transaction_id']),
                                                                 add_quotes(txn[0]['fee_currency']),
                                                                 add_quotes(txn[0]['remarks']),
                                                                 add_quotes(txn[0]['proposed_price']),
                                                                 add_quotes(txn[0]['wm_fx_rate_to_base']),
                                                                 add_quotes(txn[0]['base_currency']),
                                                                 add_quotes(txn[0]['trade_purpose']),
                                                                 add_quotes(txn[0]['original_trade_time']),
                                                                 add_quotes(txn[0]['original_trade_price']),
                                                                 add_quotes(txn[0]['original_trade_nav']),
                                                                 add_quotes(txn[0]['original_transaction_id']),
                                                                 add_quotes(txn[0]['accrued_interest']),
                                                                 add_quotes(txn[0]['biz_notes']),
                                                                 add_quotes(txn[0]['notes_updated_time']))
            else:
                insert_statements += insert_query_full.format(add_quotes(txn[0]['user_id']),
                                                              add_quotes(txn[0]['user_account_id']),
                                                              add_quotes(txn[0]['quantity']),
                                                              add_quotes(goal[0]['approved_amount']),
                                                              add_quotes(txn[0]['asset_id']),
                                                              add_quotes(txn[0]['custom_asset_id']),
                                                              add_quotes(txn[0]['asset_type']),
                                                              add_quotes(txn[0]['trade_time']),
                                                              add_quotes(txn[0]['trade_type']),
                                                              add_quotes(txn[0]['trade_price']),
                                                              add_quotes(txn[0]['trade_nav']),
                                                              add_quotes(txn[0]['fees']),
                                                              add_quotes(txn[0]['taxes']),
                                                              'now()',
                                                              'now()',
                                                              add_quotes(txn[0]['external_transaction_id']),
                                                              add_quotes(txn[0]['fee_currency']),
                                                              add_quotes(txn[0]['remarks']),
                                                              add_quotes(txn[0]['proposed_price']),
                                                              add_quotes(txn[0]['wm_fx_rate_to_base']),
                                                              add_quotes(txn[0]['base_currency']),
                                                              add_quotes(txn[0]['trade_purpose']),
                                                              add_quotes(txn[0]['original_trade_time']),
                                                              add_quotes(txn[0]['original_trade_price']),
                                                              add_quotes(txn[0]['original_trade_nav']),
                                                              add_quotes(txn[0]['original_transaction_id']),
                                                              add_quotes(txn[0]['accrued_interest']),
                                                              add_quotes(txn[0]['biz_notes']),
                                                              add_quotes(txn[0]['notes_updated_time']),
                                                              add_quotes(goal[0]['kristal_subscription_id']),
                                                              add_quotes(goal[0]['subscription_date']),
                                                              add_quotes(goal[0]['subscribed_by']),
                                                              add_quotes(goal[0]['approved_date']),
                                                              add_quotes(goal[0]['approved_by']),
                                                              add_quotes(goal[0]['source_type']),
                                                              add_quotes(goal[0]['audit_details']),
                                                              add_quotes(goal[0]['unit_price']),
                                                              add_quotes(goal[0]['cash_in_kristal_per_unit']),
                                                              add_quotes(goal[0]['total_cost']),
                                                              add_quotes(goal[0]['asset_wise_cost_map']),
                                                              add_quotes(goal[0]['subscription_pending_execution_state']),
                                                              add_quotes(goal[0]['lifecycle_state']),
                                                              add_quotes(goal[0]['bookkeeping_state']),
                                                              add_quotes(goal[0]['unique_id']),
                                                              add_quotes(goal[0]['requested_units']),
                                                              add_quotes(goal[0]['requested_amount']),
                                                              add_quotes(goal[0]['original_request']),
                                                              add_quotes(goal[0]['bk_state_mover']),
                                                              add_quotes(goal[0]['fund_remarks']),
                                                              add_quotes(goal[0]['user_report_id']),
                                                              add_quotes(goal[0]['fund_bookkeeping']),
                                                              add_quotes(goal[0]['kristal_id']),
                                                              add_quotes(goal[0]['investment_rationale']),
                                                              add_quotes(goal[0]['temp_unit_price']),
                                                              add_quotes(goal[0]['temp_total_cost']),
                                                              add_quotes(goal[0]['approval_audit']),
                                                              add_quotes(goal[0]['platform']),
                                                              add_quotes(goal[0]['mechanism']),
                                                              add_quotes(goal[0]['activity_uuid']),
                                                              add_quotes(goal[0]['is_transfer']),
                                                              add_quotes(goal[0]['transaction_fees']),
                                                              add_quotes(goal[0]['original_subscription_date']),
                                                              add_quotes(goal[0]['original_unit_nav']),
                                                              add_quotes(goal[0]['original_investment_amount']),
                                                              add_quotes(goal[0]['expert_opinion_id']),
                                                              add_quotes(goal[0]['broker_price']),
                                                              add_quotes(goal[0]['client_price']),
                                                              add_quotes(goal[0]['execution_date']),
                                                              add_quotes(goal[0]['settlement_date']),
                                                              add_quotes(goal[0]['sn_note_size']),
                                                              add_quotes(goal[0]['spread']),
                                                              add_quotes(goal[0]['spread_amount']),
                                                              add_quotes(goal[0]['broker_settlement_amount']),
                                                              add_quotes(goal[0]['sn_net_subscription_amount']),
                                                              add_quotes(goal[0]['cost_with_fees']),
                                                              add_quotes(goal[0]['cost_without_fees']),
                                                              add_quotes(goal[0]['order_fees']),
                                                              add_quotes(goal[0]['limit_price']),
                                                              add_quotes(goal[0]['order_currency']),
                                                              add_quotes(goal[0]['dvp_route']),
                                                              add_quotes(goal[0]['shared_spread_amount']),
                                                              add_quotes(goal[0]['shared_spread_percentage']),
                                                              add_quotes(goal[0]['kristal_spread_amount']),
                                                              add_quotes(goal[0]['kristal_spread_percentage']),
                                                              add_quotes(goal[0]['kristal_access_fees']),
                                                              add_quotes(goal[0]['nav_date']),
                                                              add_quotes(goal[0]['payment_date']),
                                                              add_quotes(goal[0]['internal_cutoff']),
                                                              add_quotes(goal[0]['estimated_dates_audit']))

            insert_psql.write(insert_statements)
    insert_psql.close()

except (Exception, psycopg2.Error) as error:
    print("Error while fetching data from Postgres", error)

finally:
    if connection:
        connection.close()
        print("PostgreSQL connection is closed")
