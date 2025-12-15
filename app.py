import datetime as dt
import math
import requests
import pandas as pd
from elexonpy.api_client import ApiClient
from elexonpy.api.indicative_imbalance_settlement_api import IndicativeImbalanceSettlementApi

OCTOPUS_BASE = "https://api.octopus.energy/v1"

def get_agile_prices(product_code: str, region_code: str,
                     start_utc: dt.datetime, end_utc: dt.datetime) -> pd.DataFrame:
    """Half-hour Agile unit rates (p/kWh) for your GSP/region."""
    tariff_code = f"E-1R-{product_code}-{region_code}"
    url = f"{OCTOPUS_BASE}/products/{product_code}/electricity-tariffs/{tariff_code}/standard-unit-rates/"
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

def get_system_prices(today: dt.date) -> pd.DataFrame:
    """
    Pull settlement system prices for today and tomorrow via Elexon.
    Assumes Elexon key is configured for elexonpy as per its docs.
    """
    api_client = ApiClient()
    imbalance_api = IndicativeImbalanceSettlementApi(api_client)

    dfs = []
    for d in [today, today + dt.timedelta(days=1)]:
        settlement_date = d.strftime("%Y-%m-%d")
        df_d = imbalance_api.balancing_settlement_system_prices_settlement_date_get(
            settlement_date=settlement_date,
            format="dataframe"
        )
        dfs.append(df_d)
    df = pd.concat(dfs, ignore_index=True)

    # elexonpy returns timestamps and prices; standardise column names
    # You may need to adjust to the exact column names from the API.
    # Example: 'settlement_period_start' in UTC and price in GBP/MWh.
    df["start"] = pd.to_datetime(df["settlement_period_start"], utc=True)
    df["system_gbp_per_mwh"] = df["system_price"].astype(float)
    # Convert to p/kWh to align with Agile
    df["system_p_per_kwh"] = df["system_gbp_per_mwh"] * 100 / 1000
    df = df.sort_values("start").reset_index(drop=True)
    return df[["start", "system_p_per_kwh"]]

def compute_cheapness(agile_df: pd.DataFrame,
                      system_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join Agile and system prices by half hour, then compute a 0–100 cheapness score:
    - normalise Agile vs its min/max over the window
    - normalise system price vs its min/max over the window
    - cheapness = 100 * (1 - 0.5*agile_norm - 0.5*system_norm)
    """
    df = pd.merge_asof(
        agile_df.sort_values("start"),
        system_df.sort_values("start"),
        on="start",
        direction="nearest",
        tolerance=pd.Timedelta("15min")
    )

    # Drop rows without system price
    df = df.dropna(subset=["system_p_per_kwh"]).copy()

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

    # 0 = very expensive, 100 = very cheap
    df["cheapness_score"] = 100 * (1 - 0.5 * df["agile_norm"] - 0.5 * df["system_norm"])
    return df[["start", "end", "agile_p_per_kwh", "system_p_per_kwh", "cheapness_score"]]

def main():
    # Window: today + tomorrow UTC
    today = dt.datetime.now(dt.timezone.utc).date()
    start_utc = dt.datetime.combine(today, dt.time(0, 0), tzinfo=dt.timezone.utc)
    end_utc = start_utc + dt.timedelta(days=2)

    # Fill in your Agile product / region
    product_code = "AGILE-24-10-01"  # example; use your live product code
    region_code = "H"                # your GSP/region letter

    agile_df = get_agile_prices(product_code, region_code, start_utc, end_utc)
    if agile_df.empty:
        raise SystemExit("No Agile data returned for this product/region.")

    system_df = get_system_prices(today)
    cheap_df = compute_cheapness(agile_df, system_df)

    # Show rolling cheapness for today/tomorrow
    print(cheap_df.to_string(index=False, formatters={
        "agile_p_per_kwh": "{:.2f}".format,
        "system_p_per_kwh": "{:.2f}".format,
        "cheapness_score": "{:.1f}".format,
    }))

if __name__ == "__main__":
    main()
