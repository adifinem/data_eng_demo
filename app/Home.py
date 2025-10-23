
import os
import requests
import pandas as pd
import streamlit as st

API = os.environ.get("API_BASE", "http://localhost:8000")
st.title("Spaceweather")

# col1, col2 = st.columns(2)
# with col1:
#     try:
#         health = requests.get(f"{API}/health", timeout=2).json()
#         st.success(f"API health: {health}")
#     except Exception as e:
#         st.error(f"API not reachable: {e}")

# with col2:
#     st.write("Messages table (via DuckDB)")

# try:
#     data = requests.get(f"{API}/messages?limit=100", timeout=3).json()["rows"]
#     if data:
#         st.table(data)
#     else:
#         st.info("No messages yet — Airflow will trigger ingestion every 5 minutes.")
# except Exception as e:
#     st.warning(f"Messages endpoint not ready: {e}")

st.header("HAPI Datasets")

try:
    ds_resp = requests.get(f"{API}/hapi/datasets", timeout=3).json()["datasets"]
except Exception as e:
    st.error(f"Unable to load dataset metadata: {e}")
    ds_resp = []

if not ds_resp:
    st.info("No HAPI datasets materialized yet. Run the Airflow `hapi_ingest_pipeline` DAG to populate data.")
else:
    dataset_options = {f"{d['dataset_id']} ({d['scope']})": d for d in ds_resp}
    selected_label = st.selectbox("Dataset", list(dataset_options.keys()))
    selected = dataset_options[selected_label]
    parameters = selected.get("parameters") or []
    param = st.selectbox("Parameter", parameters) if parameters else None
    limit = st.slider("Points", min_value=100, max_value=2000, step=100, value=500)

    if param:
        try:
            rows = requests.get(
                f"{API}/hapi/data",
                params={"dataset_id": selected["dataset_id"], "parameter": param, "limit": limit},
                timeout=5,
            ).json()["rows"]
            if rows:
                df = pd.DataFrame(rows)
                df['ts'] = pd.to_datetime(df['ts'])
                df = df.sort_values('ts')
                st.line_chart(df.set_index('ts')['value'], height=250)
                st.dataframe(df[['ts', 'value', 'value_raw', 'ingest_ts']].tail(50))
            else:
                st.info("No records for the selected parameter in the requested window.")
        except Exception as e:
            st.error(f"Unable to load HAPI data: {e}")
    else:
        st.warning("Selected dataset has no parameters listed.")
