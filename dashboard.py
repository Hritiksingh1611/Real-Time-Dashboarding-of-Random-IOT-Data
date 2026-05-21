from __future__ import annotations

import os
from datetime import timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import streamlit as st
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.io.twistedreactor import TwistedConnection

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "19042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "iot_data")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "sensor_data")
DEFAULT_REFRESH_SECONDS = 2
STREAM_LIMIT = 10000
IST = ZoneInfo("Asia/Kolkata")

st.set_page_config(page_title="IoT Pipeline Command Center", layout="wide")


@st.cache_resource(show_spinner=False)
def get_session():
    auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        auth_provider=auth_provider,
        connection_class=TwistedConnection,
    )
    return cluster.connect()


def ensure_schema(session) -> None:
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH REPLICATION = {{
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }}
        """
    )
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} (
            device_id TEXT,
            timestamp TIMESTAMP,
            temperature DOUBLE,
            humidity DOUBLE,
            PRIMARY KEY (device_id, timestamp)
        )
        """
    )


def load_stream_data() -> pd.DataFrame:
    session = get_session()
    ensure_schema(session)
    query = f"""
    SELECT device_id, timestamp, temperature, humidity
    FROM {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}
    LIMIT %s
    """
    rows = session.execute(query, (STREAM_LIMIT,))
    frame = pd.DataFrame(rows.current_rows)
    if frame.empty:
        return frame

    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["timestamp_ist"] = frame["timestamp_utc"].dt.tz_convert(IST)
    frame = frame.sort_values("timestamp_utc").reset_index(drop=True)
    frame["timestamp_ist_label"] = frame["timestamp_ist"].dt.strftime("%d %b %Y, %I:%M:%S %p IST")
    frame["minute_bucket"] = frame["timestamp_ist"].dt.floor("min")
    return frame


def freshness_label(seconds: float) -> str:
    if seconds <= 5:
        return "Live"
    if seconds <= 20:
        return "Warm"
    return "Lagging"


st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(circle at top left, rgba(247, 200, 97, 0.08), transparent 28%),
            radial-gradient(circle at top right, rgba(74, 222, 128, 0.08), transparent 22%),
            linear-gradient(180deg, #07111f 0%, #0c1626 52%, #101827 100%);
    }
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1500px;
    }
    .hero-card {
        padding: 1.4rem 1.6rem;
        border: 1px solid rgba(255,255,255,0.09);
        border-radius: 24px;
        background: linear-gradient(135deg, rgba(13, 24, 40, 0.94), rgba(10, 17, 30, 0.82));
        box-shadow: 0 18px 50px rgba(0,0,0,0.22);
        margin-bottom: 1rem;
    }
    .eyebrow {
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #7dd3fc;
        font-size: 0.78rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }
    .hero-title {
        font-size: 3rem;
        font-weight: 800;
        line-height: 1.05;
        color: #f8fafc;
        margin: 0 0 0.5rem 0;
    }
    .hero-copy {
        color: #cbd5e1;
        font-size: 1rem;
        margin: 0;
    }
    .status-strip {
        display: flex;
        gap: 0.75rem;
        flex-wrap: wrap;
        margin-top: 1rem;
    }
    .status-pill {
        padding: 0.55rem 0.9rem;
        border-radius: 999px;
        font-size: 0.92rem;
        font-weight: 700;
        border: 1px solid rgba(255,255,255,0.08);
        background: rgba(255,255,255,0.04);
        color: #e2e8f0;
    }
    .section-label {
        font-size: 0.85rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #94a3b8;
        margin-bottom: 0.6rem;
        font-weight: 700;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if "live_sync" not in st.session_state:
    st.session_state.live_sync = True
if "refresh_seconds" not in st.session_state:
    st.session_state.refresh_seconds = DEFAULT_REFRESH_SECONDS

with st.sidebar:
    st.subheader("Sync Controls")
    st.session_state.live_sync = st.toggle("Live sync", value=st.session_state.live_sync)
    st.session_state.refresh_seconds = st.select_slider(
        "Sync interval",
        options=[2, 5, 10, 15],
        value=st.session_state.refresh_seconds,
    )
    if st.button("Refresh now", use_container_width=True):
        st.rerun()
    st.caption("Use live sync for automatic updates, or turn it off and refresh only when you want.")


def render_dashboard():
    try:
        df = load_stream_data()
    except Exception as exc:
        st.error(f"Could not load data from Cassandra: {exc}")
        return

    if df.empty:
        st.warning("No rows found in Cassandra yet. Keep the producer and consumer running and press refresh.")
        return

    latest = df.iloc[-1]
    now_ist = pd.Timestamp.now(tz=IST)
    freshness_seconds = max((now_ist - latest["timestamp_ist"]).total_seconds(), 0)
    recent_cutoff = now_ist - timedelta(minutes=1)
    rows_last_minute = int((df["timestamp_ist"] >= recent_cutoff).sum())
    devices_seen = df["device_id"].nunique()
    latest_per_device = (
        df.sort_values("timestamp_utc")
        .groupby("device_id", as_index=False)
        .tail(1)
        .sort_values("timestamp_utc", ascending=False)
    )

    verify_df = latest_per_device.copy()
    verify_df["age_seconds"] = (now_ist - verify_df["timestamp_ist"]).dt.total_seconds().round(1)
    verify_df["stream_status"] = verify_df["age_seconds"].apply(freshness_label)
    verify_df["temperature_band"] = pd.cut(
        verify_df["temperature"],
        bins=[0, 22, 27, 100],
        labels=["Cool", "Normal", "High"],
        include_lowest=True,
    )

    temp_chart = px.line(
        df,
        x="timestamp_ist",
        y="temperature",
        color="device_id",
        markers=True,
        template="plotly_dark",
        title="Temperature Trend",
        color_discrete_sequence=["#38bdf8", "#f59e0b", "#4ade80"],
    )
    temp_chart.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(15,23,42,0.58)",
        legend_title_text="Device",
        margin=dict(l=10, r=10, t=50, b=10),
    )
    temp_chart.update_xaxes(title="IST Time")
    temp_chart.update_yaxes(title="Temperature (C)")

    humidity_chart = px.area(
        df,
        x="timestamp_ist",
        y="humidity",
        color="device_id",
        template="plotly_dark",
        title="Humidity Trend",
        color_discrete_sequence=["#22d3ee", "#a78bfa", "#f472b6"],
    )
    humidity_chart.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(15,23,42,0.58)",
        legend_title_text="Device",
        margin=dict(l=10, r=10, t=50, b=10),
    )
    humidity_chart.update_xaxes(title="IST Time")
    humidity_chart.update_yaxes(title="Humidity (%)")

    throughput_df = (
        df.groupby(["minute_bucket", "device_id"], as_index=False)
        .size()
        .rename(columns={"size": "events"})
    )
    throughput_chart = px.bar(
        throughput_df,
        x="minute_bucket",
        y="events",
        color="device_id",
        barmode="group",
        template="plotly_dark",
        title="Events Per Minute",
        color_discrete_sequence=["#60a5fa", "#fb7185", "#facc15"],
    )
    throughput_chart.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(15,23,42,0.58)",
        legend_title_text="Device",
        margin=dict(l=10, r=10, t=50, b=10),
    )
    throughput_chart.update_xaxes(title="IST Minute")
    throughput_chart.update_yaxes(title="Events")

    sync_mode = f"every {st.session_state.refresh_seconds}s" if st.session_state.live_sync else "manual"

    st.markdown(
        f"""
        <div class="hero-card">
          <div class="eyebrow">Cassandra Verification Dashboard</div>
          <div class="hero-title">IoT Pipeline Command Center</div>
          <p class="hero-copy">
            This view shows persisted sensor records from Cassandra in India Standard Time so you can verify freshness,
            device activity, and incoming event continuity without reading raw logs.
          </p>
          <div class="status-strip">
            <div class="status-pill">Timezone: IST</div>
            <div class="status-pill">Sync: {sync_mode}</div>
            <div class="status-pill">Freshness: {freshness_label(freshness_seconds)}</div>
            <div class="status-pill">Latest persisted row: {latest["timestamp_ist_label"]}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    metric1, metric2, metric3, metric4 = st.columns(4)
    metric1.metric("Persisted rows in view", f"{len(df):,}")
    metric2.metric("Rows in last 1 min", f"{rows_last_minute:,}")
    metric3.metric("Devices active", devices_seen)
    metric4.metric("Latest row age", f"{freshness_seconds:.1f}s")

    tab1, tab2, tab3 = st.tabs(["Live Trends", "Verification", "Raw Feed"])

    with tab1:
        chart_left, chart_right = st.columns(2)
        chart_left.plotly_chart(temp_chart, use_container_width=True)
        chart_right.plotly_chart(humidity_chart, use_container_width=True)
        st.plotly_chart(throughput_chart, use_container_width=True)

    with tab2:
        st.markdown('<div class="section-label">Latest persisted state by device</div>', unsafe_allow_html=True)
        st.dataframe(
            verify_df[
                [
                    "device_id",
                    "timestamp_ist_label",
                    "age_seconds",
                    "stream_status",
                    "temperature",
                    "temperature_band",
                    "humidity",
                ]
            ].rename(columns={"timestamp_ist_label": "timestamp_ist"}),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown('<div class="section-label">Verification notes</div>', unsafe_allow_html=True)
        st.info(
            "This dashboard is reading data already stored in Cassandra. If rows update here, the Kafka producer, "
            "consumer, and Cassandra insert path are all working end-to-end."
        )

    with tab3:
        st.markdown('<div class="section-label">Newest persisted events</div>', unsafe_allow_html=True)
        st.dataframe(
            df[
                ["device_id", "timestamp_ist_label", "temperature", "humidity"]
            ]
            .rename(columns={"timestamp_ist_label": "timestamp_ist"})
            .sort_values("timestamp_ist", ascending=False),
            use_container_width=True,
            hide_index=True,
        )


if st.session_state.live_sync:
    @st.fragment(run_every=f"{st.session_state.refresh_seconds}s")
    def live_dashboard_fragment():
        render_dashboard()

    live_dashboard_fragment()
else:
    render_dashboard()
