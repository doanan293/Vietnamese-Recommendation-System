import pandas as pd

# List of stock symbols
stocks = [
    "ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG", "MBB", "MSN", "MWG",
    "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB", "TCB", "TPB", "VCB", "VHM", "VIB", "VIC",
    "VJC", "VNM", "VPB", "VRE"
]

# Initialize the clustering dictionary
clustering = {
    'Stock': [],
    'Average_Percentage_Increase': [],
    'Price_Std_dev': []
}
# Loop through each stock and process the data
for stock in stocks:
    file_path = f"/Price/{stock}_Price.csv"
    # Read the CSV file
    df = pd.read_csv(file_path)

    # Convert Date column to datetime

    df['Date'] = pd.to_datetime(df['Date'])

    # Sort by date
    df = df.sort_values(by='Date')

    # Extract year from Date
    df['Year'] = df['Date'].dt.year

    # Calculate yearly average closing prices
    yearly_avg = df.groupby('Year')['Close'].mean().reset_index()

    # Calculate yearly percentage increase in price
    yearly_avg['Price_Percentage_Increase'] = yearly_avg['Close'].pct_change() * 100

    # Calculate average percentage increase excluding NaN values
    if len(yearly_avg) > 1:
        average_percentage_increase = yearly_avg['Price_Percentage_Increase'].iloc[1:].mean()
    else:
        average_percentage_increase = 0

    # Calculate the standard deviation of the closing prices
    price_std_dev = df['Close'].std()

    # Append results to clustering dictionary
    clustering['Stock'].append(stock)
    clustering['Average_Percentage_Increase'].append(average_percentage_increase)
    clustering['Price_Std_dev'].append(price_std_dev)

# Convert clustering dictionary to DataFrame
clustering_df = pd.DataFrame(clustering)
clustering_df.to_csv("/Clustering/Clustering.csv", index=False)
# Print the DataFrame
#print(clustering_df)