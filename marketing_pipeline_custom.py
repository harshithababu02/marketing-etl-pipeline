import pandas as pd
import sqlite3
import numpy as np

# -----------------------
# Step 1: Extract (robust read)
# -----------------------
df = pd.read_csv(
    "marketing_data.csv",
    sep=';',
    engine='python',
    on_bad_lines='skip'
)

print("Data Loaded Successfully")

# -----------------------
# Step 2: Clean Column Names
# -----------------------
df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
print("Columns:", df.columns.tolist())

# -----------------------
# Step 3: Fix Numeric Columns
# -----------------------
numeric_cols = [
    'desktop_sessions',
    'app_sessions',
    'desktop_transactions',
    'total_product_detail_views',
    'add_to_wishlist'
]

# Convert messy numeric values like "2,7" → 2.7
for col in numeric_cols:
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(',', '.', regex=False)
            .str.extract(r'(\d+\.?\d*)')[0]  # extract valid number
            .astype(float)
        )

# Fill missing numeric values
df[numeric_cols] = df[numeric_cols].fillna(0)

# -----------------------
# Step 4: Clean Categorical Columns
# -----------------------
if 'location_code' in df.columns:
    df['location_code'] = df['location_code'].fillna('Unknown')

if 'credit_card_info_save' in df.columns:
    df['credit_card_info_save'] = df['credit_card_info_save'].fillna('No')

if 'push_status' in df.columns:
    df['push_status'] = df['push_status'].fillna('Unknown')

# -----------------------
# Step 5: Create Date Column
# -----------------------
if 'date' not in df.columns:
    df['date'] = pd.date_range(start='2026-03-01', periods=len(df), freq='D')

# -----------------------
# Step 6: Feature Engineering
# -----------------------
df['engagement_ratio'] = df['total_product_detail_views'] / (
    df['desktop_sessions'] + df['app_sessions'] + 1
)

# -----------------------
# Step 7: Data Validation
# -----------------------
def validate_data(df):
    assert df.isnull().sum().sum() == 0, "Null values found"
    assert df.duplicated().sum() == 0, "Duplicates found"
    assert (df[numeric_cols] >= 0).all().all(), "Negative values found"
    print("Data validation passed!")

validate_data(df)

# -----------------------
# Step 8: Data Modeling
# -----------------------

# Fact Table
fact_table = df[[
    'user_id',
    'date',
    'desktop_sessions',
    'app_sessions',
    'desktop_transactions',
    'total_product_detail_views',
    'add_to_wishlist',
    'engagement_ratio'
]]

# Users Dimension
dim_users = df[[
    'user_id',
    'location_code',
    'credit_card_info_save'
]].drop_duplicates()

# Campaign Dimension (simulated)
np.random.seed(42)
campaigns = ['campaign_1', 'campaign_2', 'campaign_3']
df['campaign_id'] = np.random.choice(campaigns, size=len(df))

dim_campaigns = df[['campaign_id']].drop_duplicates()
fact_table['campaign_id'] = df['campaign_id']

# -----------------------
# Step 9: Load into SQLite
# -----------------------
conn = sqlite3.connect("marketing.db")

fact_table.to_sql("campaign_performance", conn, if_exists="replace", index=False)
dim_users.to_sql("users", conn, if_exists="replace", index=False)
dim_campaigns.to_sql("campaigns", conn, if_exists="replace", index=False)

conn.close()

# -----------------------
# Step 10: Save CSVs (for GitHub/demo)
# -----------------------
fact_table.to_csv("campaign_performance.csv", index=False)
dim_users.to_csv("users.csv", index=False)
dim_campaigns.to_csv("campaigns.csv", index=False)

print("\n✅ Pipeline completed successfully!")
print("Files created:")
print("- marketing.db")
print("- campaign_performance.csv")
print("- users.csv")
print("- campaigns.csv")
