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
insert_query_full = sqlCommands[3]

# Process chunks of data to avoid memory overload
chunk_size = 10000  # Adjust chunk size based on memory

connection = psycopg2.connect(host=config['postgresDB']['host'],
                              user=config['postgresDB']['user'],
                              password=config['postgresDB']['pass'],
                              port=config['postgresDB']['port'],
                              database=config['postgresDB']['db'])

insert_psql = open("../files/out/fund_tally_inserts.sql", "w", encoding="utf-8")
errors = open("../files/out/fund_tally_errors.txt", "w", encoding="utf-8")
skipped = open("../files/out/fund_tally_skipped.txt", "w", encoding="utf-8")

total_ksub_count = postgresql_to_row_count(connection, sqlCommands[0])
total_asset_count = postgresql_to_row_count(connection, sqlCommands[1])

columns = ['kristal_subscription_id', 'skey', 'fund_id', 'fkey', 'email', 'compliance_mode',
           'is_model_account', 'account_status']
data_frame = postgresql_to_dataframe(connection, sqlCommands[2], columns)
data_frame.to_csv('../files/itd/fund_ksub.csv', encoding='utf-8', index=False, header=True, columns=columns)

rows_where_fund_is_null = len(data_frame.loc[data_frame['fkey'].isnull()])
rows_where_ksub_is_null = len(data_frame.loc[data_frame['skey'].isnull()])
rows_where_ksub_fund_non_null = len(data_frame.loc[(data_frame['skey'].notnull()) & (data_frame['fkey'].notnull())])


def process(chunks):
    global fund, ksub
    for row in chunks.itertuples(index=False):

        if row is None:
            continue

        if row.skey is not None and str(row.skey).split("|")[0] == 0:
            skipped.write(f"Row - {row}" + "\n")
            continue

        if row.fkey is not None and str(row.fkey).split("|")[0] == 0:
            skipped.write(f"Row - {row}" + "\n")
            continue

        if math.isnan(row.fund_id) or row.fund_id is None:
            fund = [{}]
            fund[0] = dict(id=None, user_id=None, quantity=0, user_account_id=None, cost_nav=0, asset_id=None,
                           custom_asset_id=None, net_asset_value=None, dividends=0, eq_credit=0, eq_debit=0,
                           transaction_fees=0, gain_or_loss=0, nav_calculation_time=None,
                           return_percentage=0, fx_to_account=0, ib_leverage_in_account_currency=0, accrued_interest=0)
        else:
            fund = postgresql_to_record(connection,
                                        "select * from funds_investo2o.fund where id={}".format(
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
                insert_psql.write(insert_query_full.format(add_quotes(fund[0]['user_account_id']
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
                                                           add_quotes(ksub[0]['kristal_id']),
                                                           add_quotes(ksub[0]['no_of_subscribed_pending_units']),
                                                           add_quotes(ksub[0]['amount_of_mf_order_pending']),
                                                           add_quotes((ksub[0]['unit_cost_price'] or 0)),
                                                           add_quotes(ksub[0]['last_subscription_date']),
                                                           add_quotes(ksub[0]['last_subscribed_by']))
                                  + " ; ")
            except Exception as e:
                errors.write(f"FundId - {fund[0]['id']} , KsubId - {ksub[0]['kristal_subscription_id']} => An "
                             f"unexpected error occurred: {e}" + "\n")


for chunk in pd.read_csv('../files/itd/fund_ksub.csv', chunksize=chunk_size):
    process(chunk)

print("Assumption!!!")
print("Total kSubs - ", total_ksub_count)
print("Total fundAssets - ", total_asset_count)
print("Total rows in ViewQuery  -  ", len(data_frame))
print("\n")

print("Breakdown!!!")
print("ViewQuery : rows_where_fund_is_null - ", str(rows_where_fund_is_null))
print("ViewQuery : rows_where_ksub_is_null - ", str(rows_where_ksub_is_null))
print("ViewQuery : rows_where_ksub_fund_non_null - ", str(rows_where_ksub_fund_non_null) + "\n")

print("Assertion!!!")
print("Total kSubs in view - ", rows_where_ksub_fund_non_null + rows_where_fund_is_null)
print("Total fundAssets in view - ", rows_where_ksub_is_null + rows_where_ksub_fund_non_null)
print("Total -  ", rows_where_fund_is_null + rows_where_ksub_is_null + rows_where_ksub_fund_non_null)

insert_psql.close()
errors.close()
skipped.close()
