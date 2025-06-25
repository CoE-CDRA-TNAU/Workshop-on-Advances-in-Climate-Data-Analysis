import importlib
import subprocess
import sys

def install_if_missing(package):
    try:
        importlib.import_module(package)
    except ImportError:
        print(f"📦 Installing missing package: {package}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Install required packages
for pkg in ["xarray", "pandas", "netCDF4"]:
    install_if_missing(pkg)

print("Created by Dr. S. Mohan Kumar, TNAU")


import xarray as xr
import pandas as pd
import os

# NetCDF filename (Either give full path for the filename or run it in folder where netcdf file is available)
netcdf_path = 'TN_IPED_daily_1991-20.nc'
output_dir = 'output_csvs/'

# Create output folder if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Load dataset
ds = xr.open_dataset(netcdf_path)

# if you varaible name type it manually)
var_name = 'pcp'
# Automatically select the first variable
#var_name = list(ds.data_vars)[0]

data = ds[var_name]

# Ensure 'time', 'lat', 'lon' are in dimensions
if not set(['time', 'lat', 'lon']).issubset(data.dims):
    raise ValueError("NetCDF file must contain 'time', 'lat', and 'lon' dimensions.")

# Iterate over exact lat/lon values
for lat in data.lat.values:
    for lon in data.lon.values:
        # Select the exact lat/lon slice
        point_data = data.sel(lat=lat, lon=lon)

        # Convert to DataFrame
        df = point_data.to_dataframe().reset_index()

        # Keep only time and variable column
        df_out = df[['time', var_name]]

        # Format filename as lat_lon.csv
        lat_str = f"{lat:.1f}"
        lon_str = f"{lon:.1f}"
        filename = f"{lat_str}_{lon_str}.csv"

        # Save CSV
        file_path = os.path.join(output_dir, filename)
        df_out.to_csv(file_path, index=False)
        print(f"Saved: {filename}")

print("✅ All grid points exported.")