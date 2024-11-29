import pandas as pd
import configparser
from common import *
import math

config = configparser.ConfigParser()
config.read('../configs/config.ini')
fd = open('../files/in/select_txns.sql', 'r')
sqlFile = fd.read()
fd.close()
sqlCommands = sqlFile.split(';')
insert_query_full = sqlCommands[3]

columns_to_read = ['transaction_id', 'goal_id']

# Process chunks of data to avoid memory overload
chunk_size = 10000  # Adjust chunk size based on memory

connection = psycopg2.connect(host=config['postgresDB']['host'],
                              user=config['postgresDB']['user'],
                              password=config['postgresDB']['pass'],
                              port=config['postgresDB']['port'],
                              database=config['postgresDB']['db'])

insert_psql = open("../files/out/insert_scripts.sql", "w", encoding="utf-8")
errors = open("../files/out/errors.txt", "w", encoding="utf-8")


def process(chunks):
    global ksg
    global txn
    for row in chunks.itertuples(index=False):

        if row is None:
            continue

        if math.isnan(row.transaction_id) or row.transaction_id is None:
            txn = [{}]
            txn[0] = dict(transaction_id=None, user_id=None, user_account_id=None, quantity=0, asset_id=None,
                          custom_asset_id=None, asset_type='FUND', trade_time=None, trade_type='NA', trade_price=None,
                          trade_nav=None, fee=None, tax=None, external_transaction_id=None, fee_currency=None,
                          remarks=None, proposed_price=None, wm_fx_rate_to_base=None, base_currency=None,
                          trade_purpose='DEFAULT', original_trade_time=None, original_trade_price=None,
                          original_trade_nav=None, original_transaction_id=None, accrued_interest=None, biz_notes=None,
                          notes_updated_time=None)
        else:
            txn = postgresql_to_record(connection,
                                       "select * from funds_investo2o.transactions where transaction_id={}".format(
                                           row.transaction_id))

        if math.isnan(row.goal_id) or row.goal_id is None:
            ksg = [{}]
            ksg[0] = dict(kristal_execution_account=None, user_id=None, kristal_subscription_id=None,
                          kristal_subscription_goal_id=None, subscription_date=None,approved_units=None,
                          subscribed_by=None, approved_date=None, approved_by=None, source_type=None,
                          audit_details=None, unit_price=None, cash_in_kristal_per_unit=None, total_cost=None,
                          asset_wise_cost_map=None, subscription_pending_execution_state='NA',
                          lifecycle_state='NA', bookkeeping_state='NA', unique_id=None, requested_units=0,
                          requested_amount=0, original_request=None, bk_state_mover=None, approved_amount=0,
                          fund_remarks=None, user_report_id=None, fund_bookkeeping=None, kristal_id=None,
                          investment_rationale=None, temp_unit_price=None, temp_total_cost=None,
                          approval_audit=None, platform='NA', mechanism='NA', activity_uuid=None, is_transfer=None,
                          transaction_fees=None, original_subscription_date=None, original_unit_nav=None,
                          original_investment_amount=None, expert_opinion_id=None, broker_price=None, client_price=None,
                          execution_date=None, settlement_date=None, sn_note_size=None, spread=None, spread_amount=None,
                          broker_settlement_amount=None, sn_net_subscription_amount=None, cost_with_fees=None,
                          cost_without_fees=None, order_fees=None, limit_price=None, order_currency=None,
                          dvp_route='FALSE', shared_spread_amount=None, shared_spread_percentage=None,
                          kristal_spread_amount=None, kristal_spread_percentage=None, kristal_access_fees=None,
                          nav_date=None, payment_date=None, internal_cutoff=None, estimated_dates_audit=None,
                          estimated_subscription_dates_id=None, estimated_redemption_dates_id=None)

        else:
            ksg = postgresql_to_record(connection, f"select * from funds_kristals.kristal_subscription_goal where "
                                                   "kristal_subscription_goal_id={}".format(row.goal_id))

        if ksg is not None and txn is not None:
            try:
                insert_psql.write(insert_query_full.format(add_quotes(txn[0]['transaction_id']),
                                                           add_quotes(ksg[0]['kristal_subscription_goal_id']),
                                                           add_quotes(ksg[0]['kristal_subscription_id']),
                                                           add_quotes(txn[0]['user_id'] or ksg[0]['user_id']),
                                                           add_quotes(txn[0]['user_account_id']
                                                                      or ksg[0]['kristal_execution_account']),
                                                           add_quotes(txn[0]['quantity'] or ksg[0]['approved_units']),
                                                           add_quotes((ksg[0]['approved_amount'])),
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
                                                           add_quotes(ksg[0]['subscription_date']),
                                                           add_quotes(ksg[0]['subscribed_by']),
                                                           add_quotes(ksg[0]['approved_date']),
                                                           add_quotes(ksg[0]['approved_by']),
                                                           add_quotes(ksg[0]['source_type']),
                                                           add_quotes(ksg[0]['audit_details']),
                                                           add_quotes(ksg[0]['unit_price']),
                                                           add_quotes(ksg[0]['cash_in_kristal_per_unit']),
                                                           add_quotes(ksg[0]['total_cost']),
                                                           add_quotes(ksg[0]['asset_wise_cost_map']),
                                                           add_quotes((ksg[0]['subscription_pending_execution_state'])),
                                                           add_quotes((ksg[0]['lifecycle_state'])),
                                                           add_quotes((ksg[0]['bookkeeping_state'])),
                                                           add_quotes(ksg[0]['unique_id']),
                                                           add_quotes((ksg[0]['requested_units'])),
                                                           add_quotes((ksg[0]['requested_amount'])),
                                                           add_quotes(ksg[0]['original_request']),
                                                           add_quotes(ksg[0]['bk_state_mover']),
                                                           add_quotes(ksg[0]['fund_remarks']),
                                                           add_quotes(ksg[0]['user_report_id']),
                                                           add_quotes(ksg[0]['fund_bookkeeping']),
                                                           add_quotes(ksg[0]['kristal_id']),
                                                           add_quotes(ksg[0]['investment_rationale']),
                                                           add_quotes(ksg[0]['temp_unit_price']),
                                                           add_quotes(ksg[0]['temp_total_cost']),
                                                           add_quotes(ksg[0]['approval_audit']),
                                                           add_quotes((ksg[0]['platform'])),
                                                           add_quotes((ksg[0]['mechanism'])),
                                                           add_quotes(ksg[0]['activity_uuid']),
                                                           add_quotes(ksg[0]['is_transfer']),
                                                           add_quotes(ksg[0]['transaction_fees']),
                                                           add_quotes(ksg[0]['original_subscription_date']),
                                                           add_quotes(ksg[0]['original_unit_nav']),
                                                           add_quotes(ksg[0]['original_investment_amount']),
                                                           add_quotes(ksg[0]['expert_opinion_id']),
                                                           add_quotes(ksg[0]['broker_price']),
                                                           add_quotes(ksg[0]['client_price']),
                                                           add_quotes(ksg[0]['execution_date']),
                                                           add_quotes(ksg[0]['settlement_date']),
                                                           add_quotes(ksg[0]['sn_note_size']),
                                                           add_quotes(ksg[0]['spread']),
                                                           add_quotes(ksg[0]['spread_amount']),
                                                           add_quotes(ksg[0]['broker_settlement_amount']),
                                                           add_quotes(ksg[0]['sn_net_subscription_amount']),
                                                           add_quotes(ksg[0]['cost_with_fees']),
                                                           add_quotes(ksg[0]['cost_without_fees']),
                                                           add_quotes(ksg[0]['order_fees']),
                                                           add_quotes(ksg[0]['limit_price']),
                                                           add_quotes(ksg[0]['order_currency']),
                                                           add_quotes((ksg[0]['dvp_route'] or 'False')),
                                                           add_quotes(ksg[0]['shared_spread_amount']),
                                                           add_quotes(ksg[0]['shared_spread_percentage']),
                                                           add_quotes(ksg[0]['kristal_spread_amount']),
                                                           add_quotes(ksg[0]['kristal_spread_percentage']),
                                                           add_quotes(ksg[0]['kristal_access_fees']),
                                                           add_quotes(ksg[0]['nav_date']),
                                                           add_quotes(ksg[0]['payment_date']),
                                                           add_quotes(ksg[0]['internal_cutoff']),
                                                           add_quotes(ksg[0]['estimated_dates_audit']),
                                                           add_quotes(ksg[0]['estimated_subscription_dates_id']),
                                                           add_quotes(ksg[0]['estimated_redemption_dates_id']))
                                  + " ; " + " \n")
            except Exception as e:
                errors.write(f"TransactionId - {txn[0]['transaction_id']} An unexpected error occurred: {e}" + "\n")


for chunk in pd.read_csv('../files/itd/all_trades.csv', chunksize=chunk_size):
    process(chunk)
insert_psql.close()
errors.close()
