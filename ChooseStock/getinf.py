import pandas as pd

# List of stock tickers
stocks = [
    "ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG", "MBB",
    "MSN", "MWG", "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB", "TCB", "TPB",
    "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE"
]

# Initialize an empty list to store results and missing columns report
results_list = []
missing_columns_report = []

for stock in stocks:
    try:
        # Load the CSV file
        file_path = f'/Quarter_report/{stock}_quarter_report.csv'
        data = pd.read_csv(file_path)

        # Check if the required columns are present
        required_columns = ['Nợ phải trả', 'Tài sản ngắn hạn', 'Nợ ngắn hạn', 'EPS của 4 quý gần nhất', 'BVPS cơ bản',
                            'Tổng tài sản', 'Time']
        missing_columns = [col for col in required_columns if col not in data.columns]

        if missing_columns:
            print(f"Missing columns in {stock} quarter report: {missing_columns}")
            # Add missing columns to the data with value 0
            for col in missing_columns:
                data[col] = "NaN"
            missing_columns_report.append((stock, missing_columns))

        # Filter rows where "Time" matches the specified value
        filtered_data = data[data['Time'] == 'Quý 1/2024\n01/01-31/03\nCKT/HN'].copy()

        # Fill missing values with 0 and infer data types
        # filtered_data.fillna(0, inplace=True)
        # filtered_data = filtered_data.infer_objects(copy=False)

        # Remove commas from the relevant columns and convert to numeric
        filtered_data['Nợ phải trả'] = filtered_data['Nợ phải trả'].astype(str).str.replace(',', '').astype(float)
        filtered_data['Tài sản ngắn hạn'] = filtered_data['Tài sản ngắn hạn'].astype(str).str.replace(',', '').astype(
            float)
        filtered_data['Nợ ngắn hạn'] = filtered_data['Nợ ngắn hạn'].astype(str).str.replace(',', '').astype(float)
        filtered_data['EPS'] = filtered_data['EPS của 4 quý gần nhất'].astype(str).str.replace(',', '').astype(float)
        filtered_data['BVPS cơ bản'] = filtered_data['BVPS cơ bản'].astype(str).str.replace(',', '').astype(float)
        filtered_data['Tổng tài sản'] = filtered_data['Tổng tài sản'].astype(str).str.replace(',', '').astype(float)
        filtered_data['P/E'] = filtered_data['P/E cơ bản'].astype(str).str.replace(',', '').astype(float)

        # Calculate the ratios
        filtered_data['Nợ phải trả/Tài sản ngắn hạn'] = filtered_data['Nợ phải trả'] / filtered_data['Tài sản ngắn hạn']
        filtered_data['Tài sản ngắn hạn/Nợ ngắn hạn'] = filtered_data['Tài sản ngắn hạn'] / filtered_data['Nợ ngắn hạn']

        # Load the price CSV file
        file_path2 = f'/opt/airflow/dags/Price/{stock}_Price.csv'
        data2 = pd.read_csv(file_path2)

        # Convert the "Date" column to datetime format
        data2['Date'] = pd.to_datetime(data2['Date'])
        data2['Latest Price'] = data2['Close']

        # Sort the DataFrame by date in descending order
        data2 = data2.sort_values(by='Date', ascending=False)

        # Extract the price of the most recent date
        recent_price = data2.iloc[0]['Latest Price']
        filtered_data['P/B'] = recent_price / filtered_data['BVPS cơ bản']
        filtered_data['Latest Price'] = recent_price

        # Add stock ticker to the filtered data
        filtered_data['Stock'] = stock

        # Append the results to the list
        results_list.append(filtered_data[['Stock', 'Nợ phải trả', 'Tổng tài sản', 'Tài sản ngắn hạn', 'Nợ ngắn hạn',
                                           'Nợ phải trả/Tài sản ngắn hạn', 'Tài sản ngắn hạn/Nợ ngắn hạn',
                                           'EPS', 'P/E', 'BVPS cơ bản', 'P/B', 'Latest Price']])
    except FileNotFoundError:
        print(f"File not found for stock: {stock}")
    except KeyError as e:
        print(f"Key error for stock {stock}: {e}")

# Concatenate all results into a single DataFrame
if results_list:
    results = pd.concat(results_list, ignore_index=True)
else:
    results = pd.DataFrame()
results = results.drop_duplicates(subset='Stock')
# Create a DataFrame for the missing columns report
missing_columns_df = pd.DataFrame(missing_columns_report, columns=['Stock', 'Missing Columns'])

# Specify the path where you want to save the CSV file
file_pathsave = r'AnalyzeFindex.csv'

# Save the DataFrame to a CSV file
results.to_csv(file_pathsave, index=False)


filtered_df = results[
    (results['Nợ phải trả/Tài sản ngắn hạn'] < 1.8) &
    (results['Tài sản ngắn hạn/Nợ ngắn hạn'] > 1) &
    (results['P/B'] < 3)]
filtered_df.to_csv("/opt/airflow/dags/ChooseStock/goodfindexstock.csv", index = False, encoding = 'utf-8-sig')