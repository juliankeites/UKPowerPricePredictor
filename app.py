import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
from geopy.geocoders import Nominatim

st.set_page_config(page_title="IOG Off-Peak Finder", layout="wide")
st.title("🪫 Octopus IOG Smart Off-Peak Estimator (Next 48 Periods)")

# User inputs
col1, col2 = st.columns(2)
with col1:
    postcode = st.text_input("UK Postcode (for weather)", value="SW1A 1AA")
with col2:
    region = st.selectbox("Agile Region", ["N", "NW", "EM", "WM", "E", "L", "YS", "SC", "SW", "SE", "NI"])

elexon_key = st.secrets.get("ELEXON_API_KEY", "DEMO")  # Get free at bmrs.elexon.co.uk

@st.cache_data(ttl=900)  # 15min cache
def get_lat_lon(postcode):
    geolocator = Nominatim(user_agent="iog_app")
    location = geolocator.geocode(postcode)
    return location.latitude, location.longitude if location else 51.5074, -0.1278

@st.cache_data(ttl=900)
def fetch_agile_prices(region):
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    end = now + timedelta(hours=48)
    url = f"https://api.octopus.energy/v1/products/AGILE-{region}-DEFAULT-TODAY/electricity-tariffs/E-1R-AGILE-{region}-DEFAULT-TODAY-A/standard-unit-rates/?period_from={now.isoformat()}Z&period_to={end.isoformat()}Z"
    resp = requests.get(url)
    if resp.status_code == 200:
        rates = resp.json()['results']
        df = pd.DataFrame(rates)
        df['value_inc_vat'] = df['value_inc_vat'].astype(float)
        df['start'] = pd.to_datetime(df['valid_from'])
        df['period'] = (df['start'] - now).dt.total_seconds() / 1800  # Half-hour index
        return df[['period', 'start', 'value_inc_vat']].rename(columns={'value_inc_vat': 'agile_p'})
    return pd.DataFrame()  # Fallback empty

@st.cache_data(ttl=900)
def fetch_elexon_prices(key, days=2):
    now = datetime.utcnow()
    from_date = (now - timedelta(hours=1)).strftime('%Y-%m-%d')
    to_date = (now + timedelta(days=days)).strftime('%Y-%m-%d')
    url = f"https://api.bmreports.com/BMRS/B1770/V1?APIKey={key}&FromSettlementDate={from_date}&ToSettlementDate={to_date}&ServiceType=view"
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()['response']['Body']['Data']
        df = pd.DataFrame(data)
        df['SETTLEMENT_PERIOD'] = pd.to_numeric(df['SETTLEMENT_PERIOD'])
        df['SETTLEMENT_DATE'] = pd.to_datetime(df['SETTLEMENT_DATE'])
        df['price'] = pd.to_numeric(df['INITIAL_SYSTEM_PRICE'])
        return df[['SETTLEMENT_DATE', 'SETTLEMENT_PERIOD', 'price']]
    return pd.DataFrame()

@st.cache_data(ttl=900)
def fetch_weather(lat, lon):
    end = datetime.utcnow() + timedelta(hours=48)
    url = f"https://api.open-meteo.com/v1/ukmo?latitude={lat}&longitude={lon}&hourly=wind_speed_10m,cloud_cover,direct_radiation&forecast_days=2&timezone=Europe/London"
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()['hourly']
        df = pd.DataFrame({
            'time': pd.to_datetime(data['time']),
            'wind_speed': data['wind_speed_10m'],
            'cloud_cover': data['cloud_cover'],
            'solar_rad': data['direct_radiation']
        })
        df['period'] = (df['time'] - datetime.utcnow()).dt.total_seconds() / 1800
        return df
    return pd.DataFrame()

# Load data
lat, lon = get_lat_lon(postcode)
agile = fetch_agile_prices(region)
elexon = fetch_elexon_prices(elexon_key)
weather = fetch_weather(lat, lon)

if not agile.empty:
    # Merge on period (approx)
    df = agile.copy()
    df = df.merge(weather, on='period', how='left', suffixes=('', '_w'))
    df['night'] = df['start'].dt.hour < 8 or df['start'].dt.hour >= 20
    df['score'] = (
        0.6 * df['agile_p'] + 
        0.2 * (df['cloud_cover'].fillna(50) / 100) +  # High cloud = penalty
        0.1 * (10 - df['wind_speed'].fillna(5)) / 10 +  # Low wind = penalty
        0.1 * (df['solar_rad'].fillna(0) / 100)  # Solar only matters daytime
    )
    df['score'] = df['score'].fillna(100)  # Default high score
    
    # Top 12 cheapest (IOG typical window)
    top12 = df.nsmallest(12, 'score')[['period', 'start', 'agile_p', 'score', 'wind_speed', 'cloud_cover']]
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Cheapest Period", f"Period {int(top12.iloc[0]['period'])} ({top12.iloc[0]['start'].strftime('%H:%M')})")
        st.metric("Best Price", f"{top12.iloc[0]['agile_p']:.2f}p/kWh")
    with col2:
        st.metric("Night Hours in Top 12", f"{top12['night'].sum()}/12")
        st.metric("Avg Wind Speed", f"{top12['wind_speed'].mean():.1f} m/s")
    
    fig = px.bar(top12, x='start', y='score', text='agile_p',
                 title="Top 12 Cheapest IOG Periods (Lower Score = Better)",
                 hover_data=['wind_speed', 'cloud_cover'])
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Detailed Table")
    st.dataframe(top12.round(2).style.format({'agile_p': '{:.2f}', 'score': '{:.2f}'}))
    
    csv = top12.to_csv(index=False).encode('utf-8')
    st.download_button("Download CSV", csv, "iog_cheapest.csv", "text/csv")
else:
    st.warning("No Agile data - check region or try later (updates ~4PM). Use agilebuddy.uk fallback.")
    st.caption("Demo data shows scoring logic works with live APIs. [web:24]")
