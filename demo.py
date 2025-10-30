from census import Census
import requests
import pandas as pd
import time

# --- USER CONFIG ---
CENSUS_API_KEY = "827d2bde6c4a712da4432fb6f1e392a040ee9c6b"   
c = Census(CENSUS_API_KEY)

# Representative Chinatown addresses - same as demographic script
chinatowns = {
    "Boston": "88 Beach Street, Boston, MA 02111",
    "New York": "70 Bayard Street, New York, NY 10013",
    "Philadelphia": "1001 Race St, Philadelphia, PA 19107",
    "Washington DC": "701 H Street NW, Washington, DC 20001",
    "Cleveland": "2136 Rockwell Ave, Cleveland, OH 44114",
    "Chicago": "2206 S Wentworth Ave, Chicago, IL 60616",
    "Seattle": "668 S King St, Seattle, WA 98104",
    "Portland": "133 NW 4th Avenue, Portland, OR 97209",
    "Oakland": "388 9th St, Oakland, CA 94607",
    "San Francisco": "839 Stockton Street, San Francisco, CA 94108",
    "Fresno": "1001 F St, Fresno, CA 93706",
    "Los Angeles": "727 N Broadway, Los Angeles, CA 90012",
}

# Cities that need 2010 tract substitution
CROSSWALK_CITIES = ["Boston", "New York", "Washington DC", "Portland"]

# --- LOAD CROSSWALK FILE ---
crosswalk = pd.read_csv("crosswalk.txt", sep="|", dtype=str)
crosswalk = crosswalk[["GEOID_TRACT_20", "GEOID_TRACT_10"]].drop_duplicates()

def get_2010_geoid_from_2020(geoid_20):
    """Return the 2010 GEOID that corresponds to the given 2020 GEOID."""
    match = crosswalk.loc[crosswalk["GEOID_TRACT_20"] == geoid_20, "GEOID_TRACT_10"]
    return match.iloc[0] if not match.empty else None

# --- FUNCTIONS ---
def geocode_to_tract(address, max_retries=3):
    """Use Census Geocoder to get tract GEOID for an address (2020 boundaries)."""
    url = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"
    
    for attempt in range(max_retries):
        try:
            params = {
                "address": address,
                "benchmark": "Public_AR_Current",
                "vintage": "Current_Current",
                "format": "json"
            }
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            
            matches = data.get("result", {}).get("addressMatches", [])
            if not matches:
                print(f"    No matches found for: {address}")
                return None, None, None
                
            tract = matches[0]["geographies"]["Census Tracts"][0]
            geoid = tract["GEOID"]
            state = tract["STATE"]
            county = tract["COUNTY"]
            
            return geoid, state, county
            
        except (IndexError, KeyError) as e:
            print(f"    Parse error (attempt {attempt+1}/{max_retries}): {e}")
        except requests.exceptions.RequestException as e:
            print(f"    Request error (attempt {attempt+1}/{max_retries}): {e}")
        
        if attempt < max_retries - 1:
            time.sleep(2)
    
    return None, None, None


def fetch_acs5_housing_econ(year, state_fips, county_fips, tract):
    """Fetch housing and economic variables from ACS5 for a single tract/year."""
    try:
        # Housing and Economic Variables:
        # B25064_001E - Median Gross Rent
        # B25077_001E - Median Home Value
        # B25001_001E - Total Housing Units
        # B19013_001E - Median Household Income
        # B25003_003E - Renter-occupied housing units
        # B17001_002E - Income below poverty level
        # B15003_001E - Total population 25 years and over (for education calc)
        # B15003_022E - Bachelor's degree
        # B15003_023E - Master's degree
        # B15003_024E - Professional school degree
        # B15003_025E - Doctorate degree
        # B23025_005E - Unemployed in civilian labor force
        # B23025_003E - In civilian labor force (for unemployment rate calc)
        
        variables = (
            "B25064_001E",  # Median Gross Rent
            "B25077_001E",  # Median Home Value
            "B25001_001E",  # Total Housing Units
            "B19013_001E",  # Median Household Income
            "B25003_003E",  # Renter-occupied units
            "B17001_002E",  # Below poverty level
            "B15003_001E",  # Total pop 25+ (education base)
            "B15003_022E",  # Bachelor's degree
            "B15003_023E",  # Master's degree
            "B15003_024E",  # Professional degree
            "B15003_025E",  # Doctorate degree
            "B23025_005E",  # Unemployed
            "B23025_003E",  # In labor force
        )
        
        data = c.acs5.state_county_tract(
            variables, state_fips, county_fips, tract, year=year
        )
        
        if data and len(data) > 0:
            row = data[0]
            
            # Extract values, handling -666666666 (N/A) and negative values as None
            def clean_value(val):
                try:
                    num = int(val)
                    return num if num >= 0 else None
                except (ValueError, TypeError):
                    return None
            
            median_rent = clean_value(row.get("B25064_001E"))
            median_value = clean_value(row.get("B25077_001E"))
            total_housing = clean_value(row.get("B25001_001E"))
            median_income = clean_value(row.get("B19013_001E"))
            renter_occupied = clean_value(row.get("B25003_003E"))
            below_poverty = clean_value(row.get("B17001_002E"))
            
            # Education calculations
            edu_total = clean_value(row.get("B15003_001E"))
            bachelors = clean_value(row.get("B15003_022E", 0)) or 0
            masters = clean_value(row.get("B15003_023E", 0)) or 0
            professional = clean_value(row.get("B15003_024E", 0)) or 0
            doctorate = clean_value(row.get("B15003_025E", 0)) or 0
            college_degree = bachelors + masters + professional + doctorate
            
            # Unemployment
            unemployed = clean_value(row.get("B23025_005E"))
            in_labor_force = clean_value(row.get("B23025_003E"))
            
            return {
                "year": year,
                "source": "acs5",
                "median_rent": median_rent,
                "median_home_value": median_value,
                "total_housing_units": total_housing,
                "median_household_income": median_income,
                "renter_occupied_units": renter_occupied,
                "below_poverty": below_poverty,
                "pop_25_over": edu_total,
                "college_degree_or_higher": college_degree,
                "unemployed": unemployed,
                "in_labor_force": in_labor_force,
            }
        else:
            print(f"    No ACS5 data returned for {year}")
            return None
            
    except Exception as e:
        print(f"    Error fetching {year}: {e}")
        return None

# --- MAIN EXECUTION ---
results = []

# Full year range from 2010 to 2023
years = list(range(2010, 2024))

print("="*60)
print("CHINATOWN HOUSING & ECONOMIC DATA - ACS5 (2010-2023)")
print("="*60)

for city, addr in chinatowns.items():
    print(f"\n📍 Processing {city}...")
    
    # Get 2020 tract boundaries
    geoid_2020, state_fips, county_fips = geocode_to_tract(addr)
    
    if not geoid_2020:
        print(f"  ❌ Could not find tract for {city}")
        continue

    tract_2020 = geoid_2020[-6:]
    print(f"  ✓ 2020 GEOID: {geoid_2020}")
    
    # If city needs crosswalk, get 2010 tract
    geoid_2010 = None
    tract_2010 = None
    if city in CROSSWALK_CITIES:
        geoid_2010 = get_2010_geoid_from_2020(geoid_2020)
        if geoid_2010:
            tract_2010 = geoid_2010[-6:]
            print(f"  ✓ 2010 GEOID: {geoid_2010} (via crosswalk)")
        else:
            print(f"  ⚠ Warning: No 2010 tract found in crosswalk")

    # Process each year
    for y in years:
        # Determine which tract to use
        if city in CROSSWALK_CITIES and y <= 2019 and geoid_2010:
            tract_for_year = tract_2010
            geoid_for_year = geoid_2010
        else:
            tract_for_year = tract_2020
            geoid_for_year = geoid_2020
        
        data = fetch_acs5_housing_econ(y, state_fips, county_fips, tract_for_year)
        if data:
            data["city"] = city
            data["tract_geoid"] = geoid_for_year
            results.append(data)
            print(f"    ✓ {y}: Rent=${data['median_rent']}, Income=${data['median_household_income']}")
        
        time.sleep(0.3)

print("\n" + "="*60)
print("COLLECTED DATA")
print("="*60)

df = pd.DataFrame(results)
if not df.empty:
    # Calculate derived variables (percentages/rates)
    df['renter_pct'] = df.apply(
        lambda row: round((row['renter_occupied_units'] / row['total_housing_units'] * 100), 1) 
        if row['total_housing_units'] and row['total_housing_units'] > 0 else None,
        axis=1
    )
    
    df['college_degree_pct'] = df.apply(
        lambda row: round((row['college_degree_or_higher'] / row['pop_25_over'] * 100), 1)
        if row['pop_25_over'] and row['pop_25_over'] > 0 else None,
        axis=1
    )
    
    df['unemployment_rate'] = df.apply(
        lambda row: round((row['unemployed'] / row['in_labor_force'] * 100), 1)
        if row['in_labor_force'] and row['in_labor_force'] > 0 else None,
        axis=1
    )
    
    # Display sample
    print("\nSample of collected data (first 10 rows):")
    display_cols = ['city', 'year', 'median_rent', 'median_home_value', 'median_household_income', 
                    'renter_pct', 'college_degree_pct', 'unemployment_rate']
    print(df[display_cols].head(10).to_string(index=False))
    
    print("\n" + "="*60)
    print("DATA SUMMARY")
    print("="*60)
    print(f"Total records collected: {len(df)}")
    print(f"Cities with data: {df['city'].nunique()}")
    print(f"Year range: {df['year'].min()} - {df['year'].max()}")
    
    # Check for missing values
    print("\nMissing values by variable:")
    missing = df[['median_rent', 'median_home_value', 'median_household_income', 
                  'renter_occupied_units', 'below_poverty']].isnull().sum()
    print(missing)
    
    # Records per city
    print("\nRecords per city:")
    print(df.groupby('city').size().sort_values(ascending=False))
    
    # Save to CSV
    df.to_csv('chinatown_housing_economics.csv', index=False)
    print("\n✅ Data saved to chinatown_housing_economics.csv")
else:
    print("❌ No data collected.")