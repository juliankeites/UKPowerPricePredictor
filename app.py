import datetime as dt

import pandas as pd
import requests
import streamlit as st

# --- Octopus Agile helper ----------------------------------------------------

OCTOPUS_BASE = "https://api.octopus.energy/v1"


def get_agile_prices(product_code: str,
                     region_code: str,
                     start_utc: dt.datetime,
                     end_utc: dt.datetime) -> pd.DataFrame:
    """
    Half-hour Agile unit rates (p/kWh) for your GSP/region.
    """
    tariff_code = f"E-1R-{product_code}-{region_code}"
    url = (
        f"{OCTOPUS_BASE}/products/"
        f"{product_code}/electricity-tariffs/{tariff_code}/standard-unit-rates/"
    )
    params = {
        "period_from": start_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "period_to": end_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "page_size": 1500,
    }

    results = []
    while url:
        r = requests.get(url, params=params if not results else None, timeout=15)
        r.raise_for_status()
        data = r.json()
        results.extend(data.get("results", []))
        url = data.get("next")

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df["start"] = pd.to_datetime(df["valid_from"], utc=True)
    df["end"] = pd.to_datetime(df["valid_to"], utc=True)
    df["agile_p_per_kwh"] = df["value_inc_vat"].astype(float)
    df = df.sort_values("start").reset_index(drop=True)
    return df[["start", "end", "agile_p_per_kwh"]]


# --- Elexon system price (DISEBSP) helper ------------------------------------

def get_system_prices_today_and_tomorrow(now_utc: dt.datetime) -> pd.DataFrame:
    """
    Get system prices from Elexon Insights (DISEBSP) for today and tomorrow.
    Returns UTC 'start' and price in p/kWh (systemSellPrice).
    """
    base_url = (
        "https://data.elexon.co.uk/bmrs/api/v1/"
        "balancing/settlement/system-prices"
    )

    today = now_utc.date()
    tomorrow = today + dt.timedelta(days=1)
    dates = [today, tomorrow]

    dfs = []
    for d in dates:
        settlement_date = d.strftime("%Y-%m-%d")
        url = f"{base_url}/{settlement_date}?format=json"
        try:
            r = requests.get(url, timeout=15)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        data = r.json()
        items = data.get("data") or []
        if not items:
            continue
        df_d = pd.DataFrame(items)
        dfs.append(df_d)

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    df["start"] = pd.to_datetime(df["startTime"], utc=True)
    df["system_gbp_per_mwh"] = df["systemSellPrice"].astype(float)
    df["system_p_per_kwh"] = df["system_gbp_per_mwh"] * 100 / 1000
    df = df.sort_values("start").reset_index(drop=True)
    return df[["start", "system_p_per_kwh"]]


# --- Cheapness calculation ---------------------------------------------------

def floor_to_half_hour(ts: pd.Series) -> pd.Series:
    """
    Floor timestamps to the nearest half-hour (UTC).
    """
    ts = ts.dt.tz_convert("UTC")
    minutes = (ts.dt.minute // 30) * 30
    return ts.dt.floor("H") + pd.to_timedelta(minutes, unit="m")


def compute_cheapness(agile_df: pd.DataFrame,
                      system_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join Agile and system prices by half hour, then compute a 0–100 cheapness score:
    - normalise Agile vs its min/max over the window
    - normalise system price vs its min/max over the window
    - cheapness = 100 * (1 - 0.5*agile_norm - 0.5*system_norm)
    """
    if agile_df.empty or system_df.empty:
        return pd.DataFrame()

    df_a = agile_df.copy()
    df_s = system_df.copy()

    # Align both to half-hour buckets
    df_a["slot"] = floor_to_half_hour(df_a["start"])
    df_s["slot"] = floor_to_half_hour(df_s["start"])

    # Aggregate in case of duplicates
    df_a = df_a.groupby("slot", as_index=False).agg(
        {"agile_p_per_kwh": "mean", "end": "max"}
    )
    df_s = df_s.groupby("slot", as_index=False).agg(
        {"system_p_per_kwh": "mean"}
    )

    # Only overlapping periods
    df = pd.merge(df_a, df_s, on="slot", how="inner")
    if df.empty:
        return pd.DataFrame()

    df = df.rename(columns={"slot": "start"})

    # Normalise Agile
    a_min, a_max = df["agile_p_per_kwh"].min(), df["agile_p_per_kwh"].max()
    if a_max > a_min:
        df["agile_norm"] = (df["agile_p_per_kwh"] - a_min) / (a_max - a_min)
    else:
        df["agile_norm"] = 0.5

    # Normalise system price
    s_min, s_max = df["system_p_per_kwh"].min(), df["system_p_per_kwh"].max()
    if s_max > s_min:
        df["system_norm"] = (df["system_p_per_kwh"] - s_min) / (s_max - s_min)
    else:
        df["system_norm"] = 0.5

    df["cheapness_score"] = 100 * (1 - 0.5 * df["agile_norm"] - 0.5 * df["system_norm"])

    # Approximate end time as start + 30 minutes
    df["end"] = df["start"] + pd.Timedelta("30min")

    return df[["start", "end", "agile_p_per_kwh", "system_p_per_kwh", "cheapness_score"]]


# --- Streamlit app -----------------------------------------------------------

def main():
    st.set_page_config(page_title="UK Power Price & Cheapness", layout="wide")
    st.title("UK Power Price & Cheapness – Next 48 Hours")

    with st.sidebar:
        st.header("Settings")

        product_code = st.text_input(
            "Agile product code",
            value="AGILE-24-10-01",
            help="Current Agile product code; check your tariff / Octopus API docs.",
        )
        region_code = st.text_input(
            "Region code (GSP letter A–P)",
            value="H",
            help="Your GSP / DNO region letter used in Agile tariff codes.",
        )

        tz_choice = st.selectbox(
            "Display time in",
            ["Local time (UK)", "UTC"],
            index=0,
        )

        if st.button("Fetch & calculate next 48h"):
            st.session_state["run"] = True

    if "run" not in st.session_state or not st.session_state["run"]:
        st.info("Configure settings in the sidebar and click **Fetch & calculate next 48h**.")
        return

    now_utc = dt.datetime.now(dt.timezone.utc)
    end_utc = now_utc + dt.timedelta(hours=48)

    # Fetch Agile
    try:
        with st.spinner("Getting Agile prices from Octopus…"):
            agile_df = get_agile_prices(
                product_code=product_code.strip(),
                region_code=region_code.strip().upper(),
                start_utc=now_utc,
                end_utc=end_utc,
            )
    except Exception as e:
        st.error(f"Error getting Agile prices: {e}")
        return

    if agile_df.empty:
        st.warning("No Agile data returned for the given product / region over the next 48 hours.")
        return

    # Fetch system prices for today and tomorrow
    try:
        with st.spinner("Getting system prices from Elexon…"):
            system_df = get_system_prices_today_and_tomorrow(now_utc)
    except Exception as e:
        st.error(f"Error getting system prices from Elexon: {e}")
        return

    if system_df.empty:
        st.warning("No system price data returned from Elexon for today/tomorrow.")
        return
st.write("Agile rows:", len(agile_df), "System rows:", len(system_df))
st.write("Agile time range:", agile_df["start"].min(), agile_df["start"].max())
st.write("System time range:", system_df["start"].min(), system_df["start"].max())

    # Compute cheapness
    cheap_df = compute_cheapness(agile_df, system_df)
    if cheap_df.empty:
        st.warning("Could not compute cheapness score (no overlapping Agile & system price data).")
        return

    # Timezone choice
    if tz_choice.startswith("Local"):
        cheap_df["time"] = cheap_df["start"].dt.tz_convert("Europe/London")
    else:
        cheap_df["time"] = cheap_df["start"]

    st.subheader("Cheapness table")

    st.dataframe(
        cheap_df[["time", "agile_p_per_kwh", "system_p_per_kwh", "cheapness_score"]]
        .rename(
            columns={
                "time": "time",
                "agile_p_per_kwh": "Agile (p/kWh)",
                "system_p_per_kwh": "System (p/kWh)",
                "cheapness_score": "Cheapness (0–100)",
            }
        )
        .style.format(
            {
                "Agile (p/kWh)": "{:.2f}",
                "System (p/kWh)": "{:.2f}",
                "Cheapness (0–100)": "{:.1f}",
            }
        ),
        use_container_width=True,
        height=500,
    )

    st.subheader("Price & cheapness over time")

    chart_df = cheap_df.copy()
    chart_df = chart_df.set_index("time")

    st.line_chart(
        chart_df[["agile_p_per_kwh", "system_p_per_kwh"]],
        height=300,
    )
    st.area_chart(
        chart_df[["cheapness_score"]],
        height=200,
    )

    st.caption(
        "Agile prices from Octopus and system prices from Elexon Insights are "
        "normalised over the next 48 hours to produce a 0–100 cheapness score per half‑hour."
    )


if __name__ == "__main__":
    main()
