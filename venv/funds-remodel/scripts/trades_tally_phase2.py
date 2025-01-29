import os

import pandas as pd

# Read the three CSV files
df1 = pd.read_csv('../files/itd/mapped_txns.csv')
dfg4 = pd.read_csv('../files/itd/unapproved_goals.csv')
df2 = pd.DataFrame()
df3 = pd.DataFrame()
if (os.path.exists('../files/static/duplicate_txns_corrected.csv')
        and os.path.getsize('../files/static/duplicate_txns_corrected.csv') > 0):
    df2 = pd.read_csv('../files/static/duplicate_txns_corrected.csv')

if os.path.exists('../files/static/unmapped_txns.csv') and os.path.getsize('../files/static/unmapped_txns.csv') > 0:
    df3 = pd.read_csv('../files/static/unmapped_txns.csv')

dfg_all = pd.read_csv('../files/itd/all_goals.csv')

print("Started executing trades_tally_phase2.py!!!")
# Combine the files vertically (append rows)

combined_df = pd.concat([df1, df2, df3], ignore_index=True)
union_df = pd.concat([df1, df2, df3,dfg4], ignore_index=True)

ksg_count = len(dfg_all)
txn_count = len(set(combined_df['transaction_id'].unique()))
linked_ksg = len(set(combined_df['goal_id'].unique()))
unlinked_ksg = len(dfg4)

print("Total Txns - " + str(txn_count))
print("Total Goals in DB - " + str(ksg_count))
print("Goals linked to txns - " + str(linked_ksg))
print("Goals not linked to txns - " + str(unlinked_ksg))
print("All Txn and Goals count - " + str(len(union_df)) + " = [" + str(txn_count) + " + " + str(unlinked_ksg) + "]")

# Save the combined data to a new CSV file
union_df.to_csv('../files/itd/all_trades_and_goals.csv', index=False)

print("Finished executing trades_tally_phase2.py!!!")

