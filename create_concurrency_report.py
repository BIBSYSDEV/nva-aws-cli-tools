import glob
import json
import pandas as pd
from openpyxl.utils import get_column_letter
import numpy as np


# Create an empty DataFrame
df = pd.DataFrame()

# Find all JSON files in the current directory
for file_name in glob.glob('*_lambda_concurrency.json'):
    # Extract environment name from file name
    env_name = file_name.split('_lambda_concurrency.json')[0]
    
    # Load JSON data from file
    with open(file_name, 'r') as f:
        data = json.load(f)
        
    # Transform data into DataFrame
    temp_df = pd.DataFrame(data)
    
    # Ensure 'FunctionName' is in DataFrame
    if 'FunctionName' not in temp_df.columns:
        print(f"File {file_name} does not contain 'FunctionName' key.")
        continue

    # Remove the last part of the function name
    temp_df['FunctionName'] = temp_df['FunctionName'].str.rsplit('-', n=1).str[0]
    # Set environment name as column name for ReservedConcurrency
    temp_df = temp_df.rename(columns={'ReservedConcurrency': env_name})
    
    # Merge data into main DataFrame
    if 'FunctionName' in df.columns:
        df = pd.merge(df, temp_df, how='outer', on='FunctionName')
    else:
        df = temp_df

def color_cells(column):
    return ['background-color: yellow' if val == 0 
            else 'background-color: #D3F7C4' if val > 0 
            else '' for val in column]

# Replace NaN values with -1
df.fillna(-1, inplace=True)

# Calculate the sum of all numeric columns for each row, start from the second column
df['sum'] = df.iloc[:, 1:].sum(axis=1)

# Sort DataFrame by 'sum' in descending order and create a new DataFrame
sorted_df = df.sort_values(by='sum', ascending=False)

# Remove the 'sum' column from the sorted DataFrame
sorted_df.drop(columns=['sum'], inplace=True)

# Replace -1 with np.nan or '' in the sorted DataFrame
sorted_df.replace(-1, np.nan, inplace=True)

# Apply color to numeric columns only
styled_df = sorted_df.style.apply(color_cells, subset=sorted_df.columns[1:])

# Create a Pandas Excel writer using openpyxl as the engine
with pd.ExcelWriter('combined_data.xlsx', engine='openpyxl') as writer:
    styled_df.to_excel(writer, index=False)
    
    # Auto-adjust columns' widths
    for column in sorted_df:
        max_length = max(sorted_df[column].astype(str).map(len).max(), len(column))
        adjusted_width = (max_length + 2)
        worksheet = writer.sheets['Sheet1']
        worksheet.column_dimensions[get_column_letter(sorted_df.columns.get_loc(column)+1)].width = adjusted_width