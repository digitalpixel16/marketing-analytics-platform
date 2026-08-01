"""
Streamlit dashboard for the ETL project.

Shows TOTAL_BUDGET_BURNT by month, with filters for Month and City.

Run with (from inside the backend/ folder, next to database.py):
    streamlit run streamlit_app.py

Requires:
    pip install streamlit pandas plotly sqlalchemy psycopg2-binary
"""

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from database import engine  # reuse the same engine your ETL script / API use

# ============================================
# Configuration — adjust to match your actual table/columns
# ============================================

TABLE_NAME = "sales_data"

DATE_COLUMN = "Month"                # e.g. stores values like "Jun-26"
DATE_FORMAT = "Mon-YY"                # Postgres TO_DATE format matching DATE_COLUMN
CITY_COLUMN = "CITY"                  # change if your city column has a different name
BUDGET_COLUMN = "TOTAL_BUDGET_BURNT"

st.set_page_config(page_title="DigitalPixel", layout="wide")
st.markdown(
    """
    <style>
    .stApp {
        background-color: #f3f4f6;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================
# Data loading
# ============================================

@st.cache_data(ttl=300)
def load_data():
    query = text(f"""
        SELECT
            "{DATE_COLUMN}" AS month_raw,
            TO_CHAR(TO_DATE("{DATE_COLUMN}", '{DATE_FORMAT}'), 'YYYY-MM') AS month,
            "{CITY_COLUMN}" AS city,
            "{BUDGET_COLUMN}" AS total_budget_burnt
        FROM {TABLE_NAME};
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


st.title("Instamart — Monthly Budget Burnt")

try:
    df = load_data()
except Exception as exc:
    st.error(f"Failed to load data from the database: {exc}")
    st.stop()

if df.empty:
    st.warning("No data found in the table.")
    st.stop()

# ============================================
# Sidebar filters
# ============================================

st.sidebar.header("Filters")

all_months = sorted(df["month"].dropna().unique())
selected_months = st.sidebar.multiselect(
    "Month", options=all_months, default=all_months
)

all_cities = sorted(df["city"].dropna().unique())
selected_cities = st.sidebar.multiselect(
    "City", options=all_cities, default=all_cities
)

filtered_df = df[
    df["month"].isin(selected_months) & df["city"].isin(selected_cities)
]

if filtered_df.empty:
    st.warning("No data matches the selected filters.")
    st.stop()

# ============================================
# Chart — month-wise total budget burnt
# ============================================

monthly_totals = (
    filtered_df.groupby("month", as_index=False)["total_budget_burnt"]
    .sum()
    .sort_values("month")
)

col1, col2, col3 = st.columns(3)
col1.metric("Total Budget Burnt", f"{monthly_totals['total_budget_burnt'].sum():,.0f}")
col2.metric("Months Selected", len(selected_months))
col3.metric("Cities Selected", len(selected_cities))

fig = px.bar(
    monthly_totals,
    x="month",
    y="total_budget_burnt",
    labels={"month": "Month", "total_budget_burnt": "Total Budget Burnt"},
    title="Month-wise Total Budget Burnt",
    text_auto=".2s",
    color_discrete_sequence=["#0f172a"]
)
fig.update_layout(xaxis_title="Month", yaxis_title="Total Budget Burnt")
st.plotly_chart(fig, use_container_width=True)
csv_monthly = monthly_totals.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download this chart's data (CSV)",
    data=csv_monthly,
    file_name="monthly_budget_burnt.csv",
    mime="text/csv",
)

# ============================================
# Optional: breakdown by city within selected months
# ============================================

st.subheader("Budget Burnt by City (within selected months)")

city_totals = (
    filtered_df.groupby("city", as_index=False)["total_budget_burnt"]
    .sum()
    .sort_values("total_budget_burnt", ascending=False)
)

fig_city = px.bar(
    city_totals,
    x="city",
    y="total_budget_burnt",
    labels={"city": "City", "total_budget_burnt": "Total Budget Burnt"},
    text_auto=".2s",
    color_discrete_sequence=["#1f2937"]
)
st.plotly_chart(fig_city, use_container_width=True)

# ============================================
# Raw data (optional expandable table)
# ============================================

with st.expander("View raw filtered data"):
    st.dataframe(filtered_df.drop(columns=["month_raw"]), use_container_width=True)