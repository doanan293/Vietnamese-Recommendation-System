import pandas as pd

# List of stocks
stocks = ["ACB","BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG", "MBB",
    "MSN", "MWG", "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB","TCB", "TPB",
    "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE"]

for stock in stocks:
    file_path = f"D:\\Study Program\\Project\\Price\\{stock}_Price.csv"
    # Read the CSV file
    df = pd.read_csv(file_path)

    df.rename(columns={"Price": "Close"}, inplace=True)
    # Save the updated dataframe back to the CSV file
    df.to_csv(file_path, index=False)
