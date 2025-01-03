import pandas as pd

# Read the three CSV files
df1 = pd.read_csv('../files/itd/mapped_txns.csv')
df2 = pd.read_csv('../files/static/duplicate_txns.csv')
df3 = pd.read_csv('../files/static/unmapped_txns.csv')
dfg = pd.read_csv('../files/itd/all_goals.csv')

# Combine the files vertically (append rows)
combined_df = pd.concat([df1, df2, df3], ignore_index=True)
missing_goals_set = set(dfg['goal_id']) - set(combined_df['goal_id'])  # Elements in df2['A'] but not in df1['A']

missing_goals_df = pd.DataFrame(list(missing_goals_set), columns=['goal_id'])
union_df = pd.concat([combined_df, missing_goals_df], ignore_index=True)

# Save the combined data to a new CSV file
union_df.to_csv('../files/itd/all_trades_and_goals.csv', index=False)

print("The three files have been combined and saved as 'all_trades_and_goals.csv'.")

