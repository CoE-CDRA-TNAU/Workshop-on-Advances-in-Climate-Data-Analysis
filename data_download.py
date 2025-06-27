#This script download and extract data for point from IMD gridded dataset
#Give Name (For file creation), lat and lon of place in decimal degree in input-file.csv
import imdlib as imd
import csv
import pandas as pd
import os
import glob

# Input details
csv_file = 'input-file.csv'
start_yr = int(input("Enter start year: "))
end_yr = int(input("Enter end year (till 2024): ")) 

# Read Name, latitude, and longitude values
locations = []
with open(csv_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        locations.append((row.get('Name'), float(row.get('lat')), float(row.get('lon'))))

# Variables to download
pars = ["rain", "tmax", "tmin"]

# Create a temp directory if not exists
temp_dir = "temp"
os.makedirs(temp_dir, exist_ok=True)

# Process each location
for name, lat, lon in locations:
    for variable in pars:
        for y in range(start_yr, end_yr + 1):
            file_path = os.path.join(variable, f"{y}.GRD")
            csv_output = os.path.join(temp_dir, f"{variable}-{y}.csv")

            if not os.path.exists(file_path):
                print(f"File {file_path} doesn't exist, downloading...")
                data = imd.get_data(variable, y, y, fn_format='yearwise')

            # Open IMD data and directly extract for lat/lon
            data = imd.open_data(variable, y, y, 'yearwise')
            data.to_csv(csv_output, lat, lon)  # Direct extraction for point data

        # Combine all yearly CSV files for the current variable
        file_pattern = os.path.join(temp_dir, f"{variable}-*.csv")
        all_files = glob.glob(file_pattern)
        combined_df = pd.concat([pd.read_csv(f) for f in all_files])

        # Save combined file
        combined_file = os.path.join(temp_dir, f"{variable}_{name}.csv")
        combined_df.to_csv(combined_file, index=False)

        # Cleanup yearly files
        for f in all_files:
            os.remove(f)

    # Read merged data
    df_rain = pd.read_csv(os.path.join(temp_dir, f"rain_{name}.csv"))
    df_tmax = pd.read_csv(os.path.join(temp_dir, f"tmax_{name}.csv"))
    df_tmin = pd.read_csv(os.path.join(temp_dir, f"tmin_{name}.csv"))
    
    merged_df = df_rain.merge(df_tmax, on='DateTime').merge(df_tmin, on='DateTime')

    # Strip spaces and rename first column if necessary
    merged_df.columns = merged_df.columns.str.strip()
    merged_df.rename(columns={'DateTime': 'Date'}, inplace=True)
    merged_df['Date'] = pd.to_datetime(merged_df['Date'])  # Auto-detects YYYY-MM-DD format
    
    # Print Header
    #print(merged_df.head())

    # Convert "Date" column to datetime format
    merged_df['Date'] = pd.to_datetime(merged_df['Date'], format='%d/%m/%Y')

    # Rename columns correctly
    merged_df.columns = ['Date', 'RF', 'TMAX', 'TMIN']

    # Round values to one decimal place
    merged_df[['RF', 'TMAX', 'TMIN']] = merged_df[['RF', 'TMAX', 'TMIN']].round(1)

    # Extract Year and Month
    merged_df['Year'] = merged_df['Date'].dt.year
    merged_df['Month'] = merged_df['Date'].dt.month

    # Define seasons
    def get_season(month):
        if month in [6, 7, 8, 9]:
            return 'SWM'  # South-West Monsoon
        elif month in [10, 11, 12]:
            return 'NEM'  # North-East Monsoon
        elif month in [1, 2, 3]:
            return 'Winter'
        else:
            return 'Summer'

    merged_df['Season'] = merged_df['Month'].apply(get_season)

    # Extract location name for file naming
    output_filename = f"{name}_daily.csv"

    ### SAVE DAILY DATA
    merged_df[['Date', 'RF', 'TMAX', 'TMIN']].to_csv(output_filename, index=False, encoding='utf-8-sig')

    ### ANNUAL SUMMARY
    annual_df1 = merged_df.groupby('Year').agg({'RF': 'sum', 'TMAX': 'mean', 'TMIN': 'mean'}).reset_index()
    annual_df1[['RF', 'TMAX', 'TMIN']] = annual_df1[['RF', 'TMAX', 'TMIN']].round(1)
    annual_df1.to_csv(f"{name}_annual.csv", index=False, encoding='utf-8-sig')

    ### MONTHLY SUMMARY
    monthly_df1 = merged_df.groupby(['Year', 'Month']).agg({'RF': 'sum', 'TMAX': 'mean', 'TMIN': 'mean'}).reset_index()
    monthly_df1[['RF', 'TMAX', 'TMIN']] = monthly_df1[['RF', 'TMAX', 'TMIN']].round(1)
    monthly_df1.to_csv(f"{name}_monthly.csv", index=False, encoding='utf-8-sig')

    ### SEASONAL SUMMARY
    seasonal_df1 = merged_df.groupby(['Year', 'Season']).agg({'RF': 'sum', 'TMAX': 'mean', 'TMIN': 'mean'}).reset_index()
    seasonal_df1[['RF', 'TMAX', 'TMIN']] = seasonal_df1[['RF', 'TMAX', 'TMIN']].round(1)
    seasonal_df1.to_csv(f"{name}_seasonal.csv", index=False, encoding='utf-8-sig')

    print(f"Created files: {name}_daily.csv, {name}_annual.csv, {name}_monthly.csv, {name}_seasonal.csv")