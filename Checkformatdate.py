import pandas as pd

# List of stocks
stocks = ["GAS", "POW", "BID"]

for stock in stocks:
    file_path = f"D:\\Study Program\\Project\\Price\\{stock}_Price.csv"
    # Read the CSV file
    df = pd.read_csv(file_path)

    # Convert the Date column from 'dd/mm/yyyy' to 'mm/dd/yyyy'
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    df['Date'] = df['Date'].dt.strftime('%m/%d/%Y')

    # Save the updated dataframe back to the CSV file
    df.to_csv(file_path, index=False)
