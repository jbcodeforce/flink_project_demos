"""
Export view contents to parquet for dashboard use.

Option A (default): Generates schema-aligned sample data matching the Flink view DDLs
and writes parquet/customer_analytics_c360.parquet and parquet/sdp_fulfillment_analytics.parquet.

Option B: Documented in dashboard_data/README.md for exporting from live Kafka topics
(Avro + Schema Registry) when pipelines are deployed.

Run from dashboard_data/ so output goes to parquet/; or pass -o for another directory.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
import uuid

import pandas as pd
import numpy as np

try:
    import pyarrow
    _PARQUET_ENGINE = "pyarrow"
except ImportError:
    _PARQUET_ENGINE = None


def _default_output_dir() -> Path:
    """Default parquet output: cwd/parquet so running from dashboard_data/ works."""
    return Path.cwd() / "parquet"


def generate_customer_analytics_c360(num_rows: int = 50) -> pd.DataFrame:
    """Generate sample rows matching ddl.customer_analytics_c360.sql schema."""
    np.random.seed(42)
    base = datetime.now().replace(microsecond=0)
    first_names = ["John", "Jane", "Michael", "Emily", "David", "Sarah", "James", "Emma"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
    states = ["NY", "CA", "IL", "TX", "AZ"]
    segments = ["Premium", "Standard", "Value"]
    channels = ["online", "mobile", "store", "phone"]
    tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    statuses = ["Active", "At Risk", "Churned", "New"]
    tenure_segments = ["New (0-30 days)", "Recent (31-90 days)", "Established (3-12 months)", "Veteran (1+ years)"]
    support_levels = ["No Contact", "Highly Satisfied", "Satisfied", "Needs Attention"]

    rows = []
    for i in range(num_rows):
        cid = str(uuid.uuid4())
        fn = np.random.choice(first_names)
        ln = np.random.choice(last_names)
        days_reg = int(np.random.choice([15, 60, 200, 400]))
        rows.append({
            "customer_id": cid,
            "first_name": fn,
            "last_name": ln,
            "email": f"{fn.lower()}.{ln.lower()}{i}@example.com",
            "customer_segment": np.random.choice(segments),
            "preferred_channel": np.random.choice(channels),
            "generation_segment": np.random.choice(["Gen Z", "Millennial", "Gen X", "Boomer"]),
            "age_years": int(np.clip(np.random.normal(40, 15), 18, 80)),
            "city": np.random.choice(cities),
            "state": np.random.choice(states),
            "country": "USA",
            "days_since_registration": days_reg,
            "customer_tenure_segment": np.random.choice(tenure_segments),
            "loyalty_tier": np.random.choice(tiers, p=[0.35, 0.35, 0.2, 0.1]),
            "points_balance": int(np.random.exponential(2000)),
            "lifetime_value": round(float(np.random.exponential(1500)), 2),
            "value_segment": np.random.choice(["High", "Medium", "Low"]),
            "redemption_rate": round(np.random.uniform(0, 0.5), 2),
            "total_transactions": int(np.random.poisson(25)),
            "total_spent": round(np.random.exponential(1200), 2),
            "avg_order_value": round(np.random.uniform(30, 150), 2),
            "first_purchase_date": base - timedelta(days=np.random.randint(30, 500)),
            "last_purchase_date": base - timedelta(days=np.random.randint(0, 90)),
            "channels_used": int(np.random.randint(1, 4)),
            "shopping_days": int(np.random.poisson(15)),
            "transactions_last_90d": int(np.random.poisson(5)),
            "spent_last_90d": round(np.random.uniform(0, 800), 2),
            "customer_status": np.random.choice(statuses, p=[0.7, 0.15, 0.05, 0.1]),
            "recency_score": int(np.random.randint(1, 5)),
            "frequency_score": int(np.random.randint(1, 5)),
            "monetary_score": int(np.random.randint(1, 5)),
            "customer_health_score": round(np.random.uniform(1.5, 5.0), 2),
            "total_app_sessions": int(np.random.poisson(20)),
            "total_session_minutes": int(np.random.exponential(120)),
            "avg_session_duration": int(np.random.exponential(8)),
            "app_engagement_score": round(np.random.uniform(0, 5), 2),
            "last_app_use_date": (base - timedelta(days=np.random.randint(0, 30))).date(),
            "unique_device_types": int(np.random.randint(1, 3)),
            "total_support_tickets": int(np.random.poisson(0.8)),
            "resolved_support_tickets": int(np.random.poisson(0.6)),
            "avg_satisfaction": round(np.random.uniform(2.5, 5.0), 2) if np.random.random() > 0.3 else None,
            "last_ticket_date": base - timedelta(days=np.random.randint(10, 200)) if np.random.random() > 0.6 else None,
            "urgent_support_tickets": int(np.random.choice([0, 0, 1])),
            "support_satisfaction_level": np.random.choice(support_levels),
            "is_app_user": int(np.random.random() > 0.3),
            "is_digital_native": int(np.random.random() > 0.4),
            "has_urgent_issues": 0,
            "churn_risk_flag": int(np.random.random() > 0.85),
            "satisfaction_risk_flag": int(np.random.random() > 0.9),
            "channel_expansion_opportunity": int(np.random.random() > 0.7),
            "tier_upgrade_opportunity": int(np.random.random() > 0.8),
            "app_adoption_opportunity": int(np.random.random() > 0.75),
            "profile_created_at": base - timedelta(days=1),
            "view_created_at": base,
        })
    df = pd.DataFrame(rows)
    df["points_balance"] = df["points_balance"].astype("Int64")
    df["recency_score"] = df["recency_score"].astype("Int64")
    df["frequency_score"] = df["frequency_score"].astype("Int64")
    df["monetary_score"] = df["monetary_score"].astype("Int64")
    df["total_session_minutes"] = df["total_session_minutes"].astype("Int64")
    df["avg_session_duration"] = df["avg_session_duration"].astype("Int64")
    df["total_support_tickets"] = df["total_support_tickets"].astype("Int64")
    df["resolved_support_tickets"] = df["resolved_support_tickets"].astype("Int64")
    df["urgent_support_tickets"] = df["urgent_support_tickets"].astype("Int64")
    for col in ["age_years", "days_since_registration", "total_transactions", "channels_used",
                "shopping_days", "transactions_last_90d", "total_app_sessions", "unique_device_types",
                "is_app_user", "is_digital_native", "has_urgent_issues", "churn_risk_flag",
                "satisfaction_risk_flag", "channel_expansion_opportunity", "tier_upgrade_opportunity",
                "app_adoption_opportunity"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def generate_sdp_fulfillment_analytics(num_rows: int = 80) -> pd.DataFrame:
    """Generate sample rows matching ddl.sdp_fulfillment_analytics.sql schema."""
    np.random.seed(43)
    base = datetime.now().replace(microsecond=0)
    carriers = ["UPS", "FedEx", "USPS", "DHL"]
    service_levels = ["ground", "express", "overnight"]
    statuses = ["delivered", "in_transit", "pending"]
    sla_statuses = ["On Time", "Late", "In Transit", "Unknown"]
    segments = ["Premium", "Standard", "Value"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
    states = ["NY", "CA", "IL", "TX", "AZ"]
    channels = ["online", "mobile", "store"]
    channel_groups = ["Digital", "Retail"]

    rows = []
    for i in range(num_rows):
        sid = str(uuid.uuid4())
        tid = str(uuid.uuid4())
        cid = str(uuid.uuid4())
        ship_d = (base - timedelta(days=np.random.randint(1, 60))).date()
        est_d = ship_d + timedelta(days=int(np.random.choice([2, 3, 5, 7])))
        actual_d = est_d + timedelta(days=int(np.random.choice([0, 0, 0, 1, 2, -1])))
        sla_met = 1 if actual_d <= est_d else 0
        delay = (actual_d - est_d).days if actual_d > est_d else 0
        rows.append({
            "shipment_id": sid,
            "transaction_id": tid,
            "customer_id": cid,
            "transaction_date": base - timedelta(days=np.random.randint(1, 90)),
            "channel": np.random.choice(channels),
            "channel_group": np.random.choice(channel_groups),
            "total_amount": round(np.random.uniform(25, 500), 2),
            "tracking_number": f"1Z{np.random.randint(1000000000000000, 9999999999999999)}",
            "carrier": np.random.choice(carriers),
            "service_level": np.random.choice(service_levels),
            "origin_location": "Warehouse A",
            "destination_region": np.random.choice(["Northeast", "South", "Midwest", "West"]),
            "ship_date": ship_d,
            "estimated_delivery": est_d,
            "actual_delivery": actual_d,
            "delivery_status": np.random.choice(statuses, p=[0.85, 0.1, 0.05]),
            "shipping_cost": round(np.random.uniform(5, 25), 2),
            "weight_kg": round(np.random.uniform(0.5, 15), 3),
            "sla_met": sla_met,
            "delay_days": delay,
            "sla_status": sla_statuses[0] if sla_met else (sla_statuses[1] if delay > 0 else sla_statuses[3]),
            "customer_segment": np.random.choice(segments),
            "city": np.random.choice(cities),
            "state": np.random.choice(states),
            "country": "USA",
            "snapshot_at": base - timedelta(days=1),
            "view_created_at": base,
        })
    df = pd.DataFrame(rows)
    df["sla_met"] = df["sla_met"].astype("int32")
    df["delay_days"] = pd.to_numeric(df["delay_days"], errors="coerce").astype("Int64")
    return df


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write DataFrame to parquet using pyarrow if available, else DuckDB."""
    if _PARQUET_ENGINE:
        df.to_parquet(path, index=False, engine=_PARQUET_ENGINE)
        return
    try:
        import duckdb
        conn = duckdb.connect(":memory:")
        conn.register("df", df)
        conn.execute(f"COPY (SELECT * FROM df) TO '{path}' (FORMAT PARQUET)")
        conn.close()
    except ImportError as e:
        raise RuntimeError(
            "Need pyarrow or duckdb to write parquet. Install with: uv add pyarrow"
        ) from e


def main() -> None:
    parser = argparse.ArgumentParser(description="Export view contents to parquet (sample data).")
    parser.add_argument("--customer-rows", type=int, default=50, help="Number of sample rows for customer_analytics_c360")
    parser.add_argument("--fulfillment-rows", type=int, default=80, help="Number of sample rows for sdp_fulfillment_analytics")
    parser.add_argument("-o", "--output-dir", type=Path, default=None, help="Output directory (default: ./parquet)")
    args = parser.parse_args()
    out = args.output_dir or _default_output_dir()
    out.mkdir(parents=True, exist_ok=True)

    df_c360 = generate_customer_analytics_c360(num_rows=args.customer_rows)
    path_c360 = out / "customer_analytics_c360.parquet"
    _write_parquet(df_c360, path_c360)
    print(f"Wrote {path_c360} ({len(df_c360)} rows)")

    df_sdp = generate_sdp_fulfillment_analytics(num_rows=args.fulfillment_rows)
    path_sdp = out / "sdp_fulfillment_analytics.parquet"
    _write_parquet(df_sdp, path_sdp)
    print(f"Wrote {path_sdp} ({len(df_sdp)} rows)")
