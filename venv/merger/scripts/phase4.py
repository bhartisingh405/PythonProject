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
insert_query_full = sqlCommands[6]

# Process chunks of data to avoid memory overload
chunk_size = 10000  # Adjust chunk size based on memory

connection = psycopg2.connect(host=config['postgresDB']['host'],
                              user=config['postgresDB']['user'],
                              password=config['postgresDB']['pass'],
                              port=config['postgresDB']['port'],
                              database=config['postgresDB']['db'])

insert_psql = open("../files/out/fund_tally_inserts.sql", "w", encoding="utf-8")
errors = open("../files/out/fund_tally_errors.txt", "w", encoding="utf-8")


def process(chunks):
    global fund
    global ksub
    for row in chunks.itertuples(index=False):

        if row is None:
            continue

        if math.isnan(row.fund_id) or row.fund_id is None:
            fund = [{}]
            fund[0] = dict(fund_id=None, user_id=None, quantity=0, user_account_id=None, cost_nav=0, asset_id=None,
                           custom_asset_id=None, net_asset_value=None, dividends=0, eq_credit=0, eq_debit=0,
                           transaction_fees=0, gain_or_loss=0, nav_calculation_time=None,
                           return_percentage=0, fx_to_account=0, ib_leverage_in_account_currency=0, accrued_interest=0)
        else:
            fund = postgresql_to_record(connection,
                                        "select * from funds_investo2o.fund where fund_id={}".format(
                                            row.fund_id))

        if math.isnan(row.kristal_subscription_id) or row.kristal_subscription_id is None:
            ksub = [{}]
            ksub[0] = dict(kristal_subscription_id=None, kristal_id=None, kristal_execution_account=None, user_id=None,
                           no_of_subscribed_approved_units=0, no_of_subscribed_pending_units=0, flux_units=0,
                           amount_of_mf_order_pending=0, unit_cost_price=0, last_subscription_date=None,
                           last_subscribed_by=None)

        else:
            ksub = postgresql_to_record(connection, f"select * from funds_kristals.kristal_subscription where "
                                                    "kristal_subscription_id={}".format(row.kristal_subscription_id))

        if ksub is not None and fund is not None:
            try:
                insert_psql.write(insert_query_full.format(add_quotes(fund[0]['fund_id']),
                                                           add_quotes(fund[0]['user_account_id']
                                                                      or ksub[0]['kristal_execution_account']),
                                                           add_quotes(fund[0]['quantity']
                                                                      or ksub[0]['no_of_subscribed_approved_units']),
                                                           add_quotes((fund[0]['cost_nav'])),
                                                           add_quotes(fund[0]['net_asset_value']),
                                                           add_quotes(fund[0]['dividends']),
                                                           add_quotes(fund[0]['eq_credit']),
                                                           add_quotes(fund[0]['eq_debit']),
                                                           add_quotes(fund[0]['transaction_fees']),
                                                           add_quotes(fund[0]['gain_or_loss']),
                                                           add_quotes(fund[0]['nav_calculation_time']),
                                                           'now()',
                                                           'now()',
                                                           add_quotes(fund[0]['user_id'] or ksub[0]['user_id']),
                                                           add_quotes(fund[0]['asset_id']),
                                                           add_quotes(fund[0]['custom_asset_id']),
                                                           add_quotes(fund[0]['return_percentage']),
                                                           add_quotes(fund[0]['fx_to_account']),
                                                           add_quotes(fund[0]['ib_leverage_in_account_currency']),
                                                           add_quotes(fund[0]['accrued_interest']),
                                                           add_quotes(ksub[0]['kristal_subscription_id']),
                                                           add_quotes(ksub[0]['kristal_id']),
                                                           add_quotes(ksub[0]['no_of_subscribed_pending_units']),
                                                           add_quotes(ksub[0]['amount_of_mf_order_pending']),
                                                           add_quotes(ksub[0]['unit_cost_price']),
                                                           add_quotes(ksub[0]['last_subscription_date']),
                                                           add_quotes(ksub[0]['last_subscribed_by']))
                                  + " ; " + " \n")
            except Exception as e:
                errors.write(f"FundId - {fund[0]['fund_id']} , KsubId - {ksub[0]['kristal_subscription_id']} => An "
                             f"unexpected error occurred: {e}" + "\n")


for chunk in pd.read_csv('../files/itd/fund_ksub.csv', chunksize=chunk_size):
    process(chunk)
insert_psql.close()
errors.close()
