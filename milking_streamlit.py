####
import os
import sqlite3
from datetime import date, timedelta

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="🐄 Milk Yield Dashboard",
    page_icon="🥛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 로고 표시
logo_path = os.path.join(os.path.dirname(__file__), "cj.jpg")
if os.path.exists(logo_path):
    st.image(logo_path, width=200)

DB_PATH = os.path.join(os.path.dirname(__file__), "animals.db")

# ──────────────────────────────────────────────────────────────────────────────
# Database connection (cached so it isn’t pickled)
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_db_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

conn = get_db_conn()

# ──────────────────────────────────────────────────────────────────────────────
# Data loaders
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data
def load_farms():
    df = pd.read_sql(
        "SELECT DISTINCT farm_name FROM animals WHERE farm_name <> '' ORDER BY farm_name",
        conn
    )
    return df["farm_name"].tolist()

@st.cache_data
def load_animals(farm_name: str):
    return pd.read_sql(
        """
        SELECT ear_tag, birth_date
          FROM animals
         WHERE farm_name = ?
         ORDER BY ear_tag
        """,
        conn,
        params=(farm_name,),
    )

@st.cache_data
def farm_yield_summary(farm_name: str):
    total = pd.read_sql(
        "SELECT COALESCE(SUM(m.yield_value),0) AS total_liters "
        "FROM milk_yield m JOIN animals a ON m.ear_tag=a.ear_tag "
        "WHERE a.farm_name=?",
        conn, params=(farm_name,)
    )["total_liters"].iloc[0]

    days = pd.read_sql(
        "SELECT COUNT(DISTINCT record_date) AS lactation_days "
        "FROM milk_yield m JOIN animals a ON m.ear_tag=a.ear_tag "
        "WHERE a.farm_name=?",
        conn, params=(farm_name,)
    )["lactation_days"].iloc[0]

    df_year = pd.read_sql(
        "SELECT record_year AS year, "
        "       SUM(m.yield_value)           AS liters, "
        "       COUNT(DISTINCT m.record_date) AS days, "
        "       COUNT(DISTINCT m.ear_tag)     AS cows "
        "FROM milk_yield m JOIN animals a ON m.ear_tag=a.ear_tag "
        "WHERE a.farm_name=? "
        "GROUP BY record_year ORDER BY record_year",
        conn, params=(farm_name,)
    )
    df_year["avg_daily"] = (df_year["liters"] / df_year["days"]).round(1)

    since = (date.today() - timedelta(days=365)).isoformat()
    df_12mo = pd.read_sql(
        "SELECT strftime('%Y-%m',record_date) AS month, "
        "       SUM(yield_value)               AS liters "
        "FROM milk_yield m JOIN animals a ON m.ear_tag=a.ear_tag "
        "WHERE a.farm_name=? AND record_date>=? "
        "GROUP BY month ORDER BY month",
        conn, params=(farm_name, since)
    )
    df_12mo["liters"] = df_12mo["liters"].round(0).astype(int)

    df_count = pd.read_sql(
        "SELECT strftime('%Y-%m',record_date) AS month, "
        "       COUNT(DISTINCT m.ear_tag)     AS sessions "
        "FROM milk_yield m JOIN animals a ON m.ear_tag=a.ear_tag "
        "WHERE a.farm_name=? AND record_date>=? "
        "GROUP BY month ORDER BY month",
        conn, params=(farm_name, since)
    )

    df_12mo = df_12mo.merge(df_count, on="month", how="left").fillna(0)
    df_12mo["sessions"] = df_12mo["sessions"].astype(int)

    return total, days, df_year, df_12mo

@st.cache_data
def animal_yield_summary(ear_tag: str):
    total = pd.read_sql(
        "SELECT COALESCE(SUM(yield_value),0) AS total_liters "
        "FROM milk_yield WHERE ear_tag=?",
        conn, params=(ear_tag,)
    )["total_liters"].iloc[0]

    days = pd.read_sql(
        "SELECT COUNT(DISTINCT record_date) AS lactation_days "
        "FROM milk_yield WHERE ear_tag=?",
        conn, params=(ear_tag,)
    )["lactation_days"].iloc[0]

    rng = pd.read_sql(
        "SELECT MIN(record_date) AS mn, MAX(record_date) AS mx "
        "FROM milk_yield WHERE ear_tag=?",
        conn, params=(ear_tag,)
    ).iloc[0]
    try:
        d0 = pd.to_datetime(rng["mn"]).date()
        d1 = pd.to_datetime(rng["mx"]).date()
        duration = (d1 - d0).days + 1
    except:
        duration = days

    df_year = pd.read_sql(
        "SELECT record_year AS year, "
        "       SUM(yield_value)             AS liters, "
        "       COUNT(DISTINCT record_date)   AS days "
        "FROM milk_yield WHERE ear_tag=? "
        "GROUP BY record_year ORDER BY record_year",
        conn, params=(ear_tag,)
    )
    df_year["avg_daily"] = (df_year["liters"] / df_year["days"]).round(1)

    since = (date.today() - timedelta(days=365)).isoformat()
    df_12mo = pd.read_sql(
        "SELECT strftime('%Y-%m',record_date) AS month, "
        "       SUM(yield_value)               AS liters "
        "FROM milk_yield WHERE ear_tag=? AND record_date>=? "
        "GROUP BY month ORDER BY month",
        conn, params=(ear_tag, since)
    )
    df_12mo["liters"] = df_12mo["liters"].round(0).astype(int)

    df_sessions = pd.read_sql(
        "SELECT strftime('%Y-%m',record_date) AS month, "
        "       COUNT(*)                      AS sessions "
        "FROM milk_yield WHERE ear_tag=? AND record_date>=? "
        "GROUP BY month ORDER BY month",
        conn, params=(ear_tag, since)
    )
    df_12mo = df_12mo.merge(df_sessions, on="month", how="left").fillna(0)
    df_12mo["sessions"] = df_12mo["sessions"].astype(int)

    stats = pd.read_sql(
        "SELECT ROUND(AVG(yield_value),1) AS avg_daily, "
        "       MAX(yield_value)            AS max_yield, "
        "       MIN(yield_value)            AS min_yield "
        "FROM milk_yield WHERE ear_tag=?",
        conn, params=(ear_tag,)
    ).iloc[0].to_dict()

    return total, days, duration, df_year, df_12mo, stats

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────────────────────────────────────
st.sidebar.title("Configuration")
farm = st.sidebar.selectbox("🐄 Select Farm", ["-- pick a farm --"] + load_farms())
mode = st.sidebar.radio("🔍 View Mode", ["Farm Overview", "Individual Cow"])

if farm == "-- pick a farm --":
    st.sidebar.warning("Please select a farm to proceed.")
    st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# Main content
# ──────────────────────────────────────────────────────────────────────────────
if mode == "Farm Overview":
    st.header(f"📋 Farm Overview: **{farm}**")
    total, days, df_year, df_12mo = farm_yield_summary(farm)

    c1, c2, c3 = st.columns([1,1,2])
    c1.metric("Total Milk (L)", f"{total:,.0f}")
    c2.metric("Total Lactation Days", f"{days}")
    c3.metric("Cows Registered", len(load_animals(farm)))

    with st.expander("📊 Annual Production Breakdown"):
        if not df_year.empty:
            df_disp = df_year.rename(columns={
                "year":"Year",
                "liters":"Total Liters",
                "days":"Days Milked",
                "avg_daily":"Avg Daily (L)"
            }).set_index("Year")
            df_disp["Total Liters"]  = df_disp["Total Liters"].map("{:,.0f}".format)
            df_disp["Days Milked"]   = df_disp["Days Milked"].map("{:,.0f}".format)
            df_disp["Avg Daily (L)"] = df_disp["Avg Daily (L)"].map("{:,.1f}".format)
            st.dataframe(df_disp)
        else:
            st.info("No annual data.")

    with st.expander("📈 Last 12 Months Trend"):
        if not df_12mo.empty:
            fig, ax1 = plt.subplots(figsize=(10,4))
            ax1.bar(df_12mo["month"], df_12mo["liters"], color="tab:blue", alpha=0.6)
            ax1.set_ylabel("Liters")
            ax1.set_xticks(range(len(df_12mo)))
            ax1.set_xticklabels(df_12mo["month"], rotation=45)

            ax2 = ax1.twinx()
            ax2.plot(df_12mo["month"], df_12mo["sessions"],
                     marker="o", color="tab:orange", linewidth=2)
            ax2.set_ylabel("Milking Sessions")

            plt.title("Last 12 Months: Production & Sessions")
            plt.tight_layout()
            st.pyplot(fig)

            tbl = df_12mo.rename(columns={
                "month":"Month",
                "liters":"Liters",
                "sessions":"Milking Sessions"
            }).set_index("Month")
            tbl["Liters"]           = tbl["Liters"].map("{:,.0f}".format)
            tbl["Milking Sessions"] = tbl["Milking Sessions"].map("{:,.0f}".format)
            st.table(tbl)
        else:
            st.info("No data for the past 12 months.")

    # CSV 다운로드
    csv = df_year.copy()
    csv["Avg Daily (L)"] = csv["avg_daily"]
    csv = csv[["year","liters","days","avg_daily"]]
    csv.columns = ["Year","Total Liters","Days Milked","Avg Daily (L)"]
    csv["Total Liters"]   = csv["Total Liters"].map("{:,.0f}".format)
    csv["Days Milked"]    = csv["Days Milked"].map("{:,.0f}".format)
    csv["Avg Daily (L)"]  = csv["Avg Daily (L)"].map("{:,.1f}".format)
    st.download_button(
        "⬇️ Download Farm Annual CSV",
        csv.to_csv(index=False).encode("utf-8"),
        file_name=f"{farm}_annual_report.csv",
        mime="text/csv"
    )

else:
    df_animals = load_animals(farm)
    cow = st.sidebar.selectbox(
        "🐮 Select Cow", ["-- pick a cow --"] + df_animals["ear_tag"].tolist()
    )
    if cow == "-- pick a cow --":
        st.sidebar.warning("Please select a cow.")
        st.stop()

    st.header(f"🐮 Cow Detail: **{cow}**")
    birth = df_animals.set_index("ear_tag").at[cow,"birth_date"]
    age   = ((date.today() - pd.to_datetime(birth).date()).days // 30) if birth else "-"
    col1, col2 = st.columns(2)
    col1.metric("Birth Date", birth or "-")
    col2.metric("Age (months)", f"{age}")

    total, days, duration, df_year, df_12mo, stats = animal_yield_summary(cow)
    m1,m2,m3,m4 = st.columns(4)
    m1.metric("Total Milk (L)", f"{total:,.0f} ({duration} d)")
    m2.metric("Days Milked", f"{days}")
    m3.metric("Avg Daily (L)", f"{stats['avg_daily']:.1f}")
    m4.metric("Max / Min (L)", f"{stats['max_yield']} / {stats['min_yield']}")

    tab1, tab2 = st.tabs(["Yearly Trend","Last 12 Months"])
    with tab1:
        if not df_year.empty:
            df_disp = df_year.rename(columns={
                "year":"Year",
                "liters":"Total Liters",
                "days":"Days Milked",
                "avg_daily":"Avg Daily (L)"
            }).set_index("Year")
            df_disp["Total Liters"]  = df_disp["Total Liters"].map("{:,.0f}".format)
            df_disp["Days Milked"]   = df_disp["Days Milked"].map("{:,.0f}".format)
            df_disp["Avg Daily (L)"] = df_disp["Avg Daily (L)"].map("{:,.1f}".format)
            st.dataframe(df_disp)
        else:
            st.info("No yearly data.")

    with tab2:
        if not df_12mo.empty:
            fig, ax1 = plt.subplots(figsize=(10,4))
            ax1.bar(df_12mo["month"], df_12mo["liters"], color="tab:blue", alpha=0.6)
            ax1.set_ylabel("Liters")
            ax1.set_xticks(range(len(df_12mo)))
            ax1.set_xticklabels(df_12mo["month"], rotation=45)

            ax2 = ax1.twinx()
            ax2.plot(df_12mo["month"], df_12mo["sessions"],
                     marker="o", color="tab:orange", linewidth=2)
            ax2.set_ylabel("Milking Sessions")

            plt.title("Last 12 Months: Production & Sessions")
            plt.tight_layout()
            st.pyplot(fig)

            tbl = df_12mo.rename(columns={
                "month":"Month",
                "liters":"Liters",
                "sessions":"Milking Sessions"
            }).set_index("Month")
            tbl["Liters"]           = tbl["Liters"].map("{:,.0f}".format)
            tbl["Milking Sessions"] = tbl["Milking Sessions"].map("{:,.0f}".format)
            st.table(tbl)
        else:
            st.info("No data for the past 12 months.")

    st.subheader("📋 Last 6 Months Detail")
    df_6mo = pd.read_sql(
        "SELECT record_date AS Date, yield_value AS Liters "
        "FROM milk_yield WHERE ear_tag=? AND record_date>=? ORDER BY record_date",
        conn, params=(cow,(date.today()-timedelta(days=180)).isoformat())
    )
    if not df_6mo.empty:
        df_6mo["Liters"] = df_6mo["Liters"].map("{:,.0f}".format)
        st.dataframe(df_6mo.set_index("Date"))
    else:
        st.info("No data in the last six months.")

    # CSV 다운로드
    csv = df_year.copy()
    csv["Avg Daily (L)"] = csv["avg_daily"]
    csv = csv[["year","liters","days","avg_daily"]]
    csv.columns = ["Year","Total Liters","Days Milked","Avg Daily (L)"]
    csv["Total Liters"]   = csv["Total Liters"].map("{:,.0f}".format)
    csv["Days Milked"]    = csv["Days Milked"].map("{:,.0f}".format)
    csv["Avg Daily (L)"]  = csv["Avg Daily (L)"].map("{:,.1f}".format)
    st.download_button(
        "⬇️ Download Cow Annual CSV",
        csv.to_csv(index=False).encode("utf-8"),
        file_name=f"{cow}_annual_report.csv",
        mime="text/csv"
    )
