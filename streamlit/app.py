import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Nectar IoT Dashboard", layout="wide")

conn = st.connection("postgresql", type="sql", 
                     url="postgresql://admin:admin123@postgres:5432/nectar")

st.title("Nectar IoT Platform Dashboard")
st.sidebar.header("Filters")
site = st.sidebar.selectbox("Select Site", ["site-001", "site-002", "site-003"])

def run_query(query):
    return conn.query(query, ttl=600)

col1, col2, col3 = st.columns(3)
with col1:
    total = run_query(f"""
        SELECT SUM(f.energy_kwh) as total
        FROM fact_energy f
        WHERE f.site_id = '{site}'
    """).iloc[0]['total']
    st.metric("Total Energy (kWh)", f"{total:.2f}")

with col2:
    cnt = run_query(f"""
        SELECT COUNT(DISTINCT f.asset_id) as count
        FROM fact_energy f
        WHERE f.site_id = '{site}'
    """).iloc[0]['count']
    st.metric("Active Assets", cnt)

with col3:
    faults = run_query(f"""
        SELECT COUNT(*) as faults
        FROM fact_event e
        WHERE e.severity = 'High'
          AND e.asset_id IN (SELECT f.asset_id FROM fact_energy f WHERE f.site_id = '{site}')
    """).iloc[0]['faults']
    st.metric("High Severity Faults", faults)

energy_df = run_query(f"""
    SELECT t.date, SUM(f.energy_kwh) as total_energy
    FROM fact_energy f
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.site_id = '{site}'
    GROUP BY t.date
    ORDER BY t.date
""")
if not energy_df.empty:
    fig = px.line(energy_df, x='date', y='total_energy', title='Daily Energy Consumption')
    st.plotly_chart(fig, use_container_width=True)

assets = run_query(f"""
    SELECT f.asset_id, t.temperature, t.humidity, t.power_consumption
    FROM fact_telemetry t
    JOIN fact_energy f ON f.asset_id = t.asset_id
    WHERE f.site_id = '{site}'
    ORDER BY t.ingestion_timestamp DESC
    LIMIT 10
""")
if not assets.empty:
    st.subheader("Latest Asset Readings")
    st.dataframe(assets)

faults = run_query(f"""
    SELECT e.event_type, e.severity, COUNT(*) as count
    FROM fact_event e
    WHERE e.asset_id IN (SELECT f.asset_id FROM fact_energy f WHERE f.site_id = '{site}')
    GROUP BY e.event_type, e.severity
""")
if not faults.empty:
    fig2 = px.bar(faults, x='event_type', y='count', color='severity', title='Fault Distribution')
    st.plotly_chart(fig2, use_container_width=True)
