import pandas as pd
import json

# Load your CSV file into a DataFrame
df = pd.read_csv('/Users/bhartisingh/Desktop/sl_dumps/kp_dump.csv')


# Function to fix entire JSON column
def fix_json_column(json_str):
    try:
        # Attempt to load the JSON string (will raise error if it's malformed)
        json_obj = json.loads(json_str)
        # If the JSON is valid, return the stringified JSON with proper formatting
        return json.dumps(json_obj)
    except json.JSONDecodeError:
        # If the JSON is invalid, return the original string (could log or handle errors)
        print(f"Invalid JSON: {json_str}")
        return json_str  # Keep the original string if it's invalid

# Apply the function to the 'kristal_composition' column (or other JSON columns)
df['kristal_composition'] = df['kristal_composition'].apply(fix_json_column)

# Save the fixed DataFrame back to a CSV file
df.to_csv('/Users/bhartisingh/Desktop/sl_dumps/fixed_kp_dump.csv', index=False)

print("JSON column fixed and saved!")
