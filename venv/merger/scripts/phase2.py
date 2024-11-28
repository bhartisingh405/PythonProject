import pandas as pd

# Read the three CSV files
df1 = pd.read_csv('../files/itd/mapped_txns.csv')
df2 = pd.read_csv('../files/static/duplicate_txns.csv')
df3 = pd.read_csv('../files/static/unmapped_txns.csv')

# Combine the files vertically (append rows)
combined_df = pd.concat([df1, df2, df3], ignore_index=True)

# Save the combined data to a new CSV file
combined_df.to_csv('../files/itd/all_txns.csv', index=False)

print("The three files have been combined and saved as 'all_txns.csv'.")

