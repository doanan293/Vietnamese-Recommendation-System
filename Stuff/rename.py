import os

# List of VN30 companies
vn30_companies = [
    "ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG", "MBB", "MSN",
    "MWG", "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB", "TCB", "TPB", "VCB", "VHM",
    "VIB", "VIC", "VJC", "VNM", "VPB", "VRE"
]

# Base directory
base_dir = r"D:\Study Program\Project\Price"

# Rename files
for stock in vn30_companies:
    original_file_path = os.path.join(base_dir, f"{stock} Historical Data.csv")
    new_file_path = os.path.join(base_dir, f"{stock}_Price.csv")

    # Check if the original file exists before renaming
    if os.path.exists(original_file_path):
        os.rename(original_file_path, new_file_path)
        print(f"Renamed: {original_file_path} to {new_file_path}")
    else:
        print(f"File not found: {original_file_path}")
