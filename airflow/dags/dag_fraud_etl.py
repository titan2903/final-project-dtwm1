"""
Operation FRAUD BUSTER - Complete ETL Pipeline Solution
PT Nusantara Digital Commerce - NDC

Data Sources: PostgreSQL OLTP + CSV Files
Extract -> Transform -> Load to Star Schema

Uses intermediate pickle files for large data transfers
between tasks (avoids XCom size limits).

INSTRUCTOR ONLY - DO NOT DISTRIBUTE TO STUDENTS
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
import pickle

OLTP_CONN_ID = "ndc_oltp"
OLAP_CONN_ID = "ndc_olap"
DATA_DIR = "/opt/airflow/data"
STAGING_DIR = "/opt/airflow/data/staging"


def get_oltp():
    return PostgresHook(postgres_conn_id=OLTP_CONN_ID).get_conn()


def get_olap():
    return PostgresHook(postgres_conn_id=OLAP_CONN_ID).get_conn()


def _save(df, name):
    os.makedirs(STAGING_DIR, exist_ok=True)
    path = os.path.join(STAGING_DIR, f"{name}.pkl")
    df.to_pickle(path)
    return path


def _load(path):
    return pd.read_pickle(path)


def batch_insert_df(df, table, conn, batch_size=5000):
    """Batch INSERT rows into a table. Tables should be empty (truncated first)."""
    if df.empty:
        return 0
    cur = conn.cursor()
    # Convert to object dtype so NaN becomes proper Python None, not numpy.float64('nan')
    df = df.astype(object).where(df.notna(), None)
    cols = list(df.columns)
    ph = ", ".join(["%s"] * len(cols))
    cn = ", ".join(cols)
    sql = f"INSERT INTO {table} ({cn}) VALUES ({ph})"
    total = 0
    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start + batch_size]
        for _, row in batch.iterrows():
            cur.execute(sql, tuple(row))
            total += 1
        conn.commit()
    cur.close()
    return total


@dag(
    dag_id="fraud_buster_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "fraud_buster", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["fraud", "ndc", "etl"],
)
def fraud_buster_etl():

    # ── EXTRACT ──────────────────────────────────────────

    @task
    def extract_all_customers():
        conn = get_oltp()
        df = pd.read_sql("SELECT customer_id, full_name, city, province, account_type, registered_at FROM ndc_customers", conn)
        conn.close()
        for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            df[col] = df[col].astype(str)
        path = _save(df, "all_customers")
        return {"path": path, "rows": len(df)}

    @task
    def extract_transactions():
        conn = get_oltp()
        query = """
            SELECT
                o.order_id, o.customer_id, o.order_date, o.status,
                o.total_amount, o.discount_amount, o.shipping_cost, o.net_amount,
                o.promo_id, o.shipping_city, o.shipping_province,
                o.shipping_lat, o.shipping_lng, o.shipping_method,
                o.device_fingerprint, o.ip_address AS order_ip, o.is_flagged,
                oi.order_item_id, oi.product_id, oi.quantity, oi.unit_price,
                oi.subtotal, oi.discount_per_item,
                c.full_name, c.city AS customer_city, c.province AS customer_province,
                c.account_type, c.registered_at,
                p.product_name, p.base_price, p.cost_price,
                cat.category_name,
                COALESCE(pc.category_name, cat.category_name) AS parent_category,
                pm.payment_method, pm.payment_provider, pm.amount AS payment_amount,
                pm.source_account,
                s.courier, s.shipping_status, s.recipient_name
            FROM ndc_orders o
            LEFT JOIN ndc_order_items oi ON o.order_id = oi.order_id
            LEFT JOIN ndc_customers c ON o.customer_id = c.customer_id
            LEFT JOIN ndc_products p ON oi.product_id = p.product_id
            LEFT JOIN ndc_categories cat ON p.category_id = cat.category_id
            LEFT JOIN ndc_categories pc ON cat.parent_category_id = pc.category_id
            LEFT JOIN ndc_payments pm ON o.order_id = pm.order_id
            LEFT JOIN ndc_shipments s ON o.order_id = s.order_id
            ORDER BY o.order_date
        """
        df = pd.read_sql(query, conn)
        conn.close()
        # Convert datetime to string for safe storage
        for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            df[col] = df[col].astype(str)
        path = _save(df, "transactions")
        return {"path": path, "rows": len(df)}

    @task
    def extract_reviews():
        conn = get_oltp()
        query = """
            SELECT r.*
            FROM ndc_product_reviews r
            ORDER BY r.reviewed_at
        """
        df = pd.read_sql(query, conn)
        conn.close()
        for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            df[col] = df[col].astype(str)
        path = _save(df, "reviews")
        return {"path": path, "rows": len(df)}

    @task
    def extract_refunds():
        conn = get_oltp()
        query = """
            SELECT r.*,
                   pm.payment_method, pm.payment_provider,
                   c.bank_account AS customer_bank_account
            FROM ndc_refund_records r
            LEFT JOIN ndc_orders o ON r.order_id = o.order_id
            LEFT JOIN ndc_payments pm ON o.order_id = pm.order_id
            LEFT JOIN ndc_customers c ON r.customer_id = c.customer_id
            ORDER BY r.processed_at
        """
        df = pd.read_sql(query, conn)
        conn.close()
        for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            df[col] = df[col].astype(str)
        path = _save(df, "refunds")
        return {"path": path, "rows": len(df)}

    @task
    def extract_csv_files():
        files = {}
        for fn in ["customer_complaints.csv", "ip_device_log.csv", "promo_codes.csv"]:
            path = os.path.join(DATA_DIR, fn)
            if os.path.exists(path):
                name = fn.replace(".csv", "")
                df = pd.read_csv(path)
                _save(df, name)
                files[name] = len(df)
        return files

    @task
    def extract_devices():
        conn = get_oltp()
        df = pd.read_sql(
            "SELECT device_fingerprint, device_type, os, browser, ip_address, is_flagged "
            "FROM ndc_customer_devices",
            conn,
        )
        conn.close()
        for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            df[col] = df[col].astype(str)
        path = _save(df, "devices")
        return {"path": path, "rows": len(df)}

    # ── TRANSFORM ────────────────────────────────────────

    @task
    def generate_date_dimension():
        dates = []
        start = datetime(2025, 1, 1)
        end = datetime(2026, 12, 31)
        current = start
        holidays = {
            20250101: "Tahun Baru", 20250129: "Imlek",
            20250331: "Idul Fitri", 20250401: "Idul Fitri",
            20250501: "Hari Buruh", 20250529: "Waisak",
            20250601: "Pancasila",
        }
        day_names = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"]
        month_names = ["", "Januari", "Februari", "Maret", "April", "Mei", "Juni",
                       "Juli", "Agustus", "September", "Oktober", "November", "Desember"]

        while current <= end:
            date_id = int(current.strftime("%Y%m%d"))
            dow = current.weekday()
            dates.append({
                "date_id": date_id,
                "full_date": current.strftime("%Y-%m-%d"),
                "day_of_week": dow + 1,
                "day_name": day_names[dow],
                "month": current.month,
                "month_name": month_names[current.month],
                "quarter": (current.month - 1) // 3 + 1,
                "year": current.year,
                "is_weekend": dow >= 5,
                "is_holiday": date_id in holidays,
            })
            current += timedelta(days=1)
        return dates

    @task
    def transform_to_star_schema(tx_info, cust_info, rv_info, rf_info, csv_info, dev_info):
        tx_df = _load(tx_info["path"])
        all_cust_df = _load(cust_info["path"])
        rv_df = _load(rv_info["path"])
        rf_df = _load(rf_info["path"])
        dev_raw_df = _load(dev_info["path"])

        dims = {}
        facts = {}

        # ── dim_customer (schema: source_customer_id, full_name, city, province, account_type, is_active, registered_date)
        # Use ALL customers from ndc_customers, not just those with orders
        if not all_cust_df.empty:
            cust = all_cust_df.rename(columns={
                "customer_id": "source_customer_id",
                "city": "city",
                "province": "province",
                "registered_at": "registered_date",
            })[["source_customer_id", "full_name", "city", "province", "account_type", "registered_date"]].copy()
            cust["is_active"] = True
            dims["dim_customer"] = cust

        # ── dim_product (schema: source_product_id, product_name, category_name, subcategory_name, base_price, is_active)
        if not tx_df.empty and "product_id" in tx_df.columns:
            prod = tx_df[tx_df["product_id"].notna()].drop_duplicates("product_id")[
                ["product_id", "product_name", "category_name", "parent_category", "base_price"]
            ].copy()
            prod.columns = ["source_product_id", "product_name", "category_name",
                            "subcategory_name", "base_price"]
            prod["is_active"] = True
            dims["dim_product"] = prod

        # ── dim_location ──
        if not tx_df.empty and "shipping_city" in tx_df.columns:
            locs = tx_df[tx_df["shipping_city"].notna()].drop_duplicates(
                ["shipping_city", "shipping_province"]
            )[["shipping_city", "shipping_province", "shipping_lat", "shipping_lng"]].copy()
            locs.columns = ["city", "province", "latitude", "longitude"]
            locs["latitude"] = pd.to_numeric(locs["latitude"], errors="coerce")
            locs["longitude"] = pd.to_numeric(locs["longitude"], errors="coerce")
            locs["postal_code"] = None
            locs["is_valid_coordinates"] = (
                locs["latitude"].notna() & locs["longitude"].notna() &
                (locs["latitude"].abs() > 0.1)
            )
            locs = locs[["city", "province", "postal_code", "latitude", "longitude", "is_valid_coordinates"]]
            dims["dim_location"] = locs

        # ── dim_payment_method ──
        if not tx_df.empty and "payment_method" in tx_df.columns:
            pm = tx_df[tx_df["payment_method"].notna()].drop_duplicates(
                ["payment_method", "payment_provider"]
            )[["payment_method", "payment_provider"]].copy()
            pm["payment_type_category"] = pm["payment_method"].map({
                "credit_card": "card", "bank_transfer": "transfer",
                "e_wallet": "digital", "virtual_account": "transfer", "cod": "cash",
            })
            dims["dim_payment_method"] = pm

        # ── dim_device — build from ndc_customer_devices + orders + reviews ──
        all_fps = []
        # Start with ndc_customer_devices (has device_type, os, browser, ip_address, is_flagged)
        if not dev_raw_df.empty:
            dev_base = dev_raw_df.rename(columns={"ip_address": "ip"})[[
                "device_fingerprint", "device_type", "os", "browser", "ip", "is_flagged"
            ]].copy()
            all_fps.append(dev_base)
        # Add unique fingerprints from orders (ip only)
        if not tx_df.empty and "device_fingerprint" in tx_df.columns:
            order_fps = tx_df[tx_df["device_fingerprint"].notna()].drop_duplicates("device_fingerprint")[
                ["device_fingerprint", "order_ip"]
            ].copy()
            order_fps.columns = ["device_fingerprint", "ip"]
            order_fps["device_type"] = None
            order_fps["os"] = None
            order_fps["browser"] = None
            order_fps["is_flagged"] = False
            all_fps.append(order_fps)
        # Add unique fingerprints from reviews
        if not rv_df.empty and "device_fingerprint" in rv_df.columns:
            review_fps = rv_df[rv_df["device_fingerprint"].notna()].drop_duplicates("device_fingerprint")[
                ["device_fingerprint"]
            ].copy()
            review_fps["ip"] = None
            review_fps["device_type"] = None
            review_fps["os"] = None
            review_fps["browser"] = None
            review_fps["is_flagged"] = False
            all_fps.append(review_fps)

        if all_fps:
            combined = pd.concat(all_fps, ignore_index=True)
            # Prioritize rows with richer data (from ndc_customer_devices)
            combined["_priority"] = combined["device_type"].notna().astype(int)
            combined = combined.sort_values("_priority", ascending=False)
            dev = combined.drop_duplicates("device_fingerprint").drop(columns=["_priority"])
            dev = dev.rename(columns={"ip": "ip_address"})
            dev["ip_prefix"] = dev["ip_address"].apply(
                lambda x: ".".join(str(x).split(".")[:3]) if pd.notna(x) and str(x) != "nan" else None
            )
            dims["dim_device"] = dev

        # ── Build surrogate key maps ──
        cust_map = {}
        prod_map = {}

        # Build cust_map from dim_customer (ALL customers, not just those with orders)
        cust_df = dims.get("dim_customer")
        if cust_df is not None:
            cust_map = dict(zip(cust_df["source_customer_id"], range(1, len(cust_df) + 1)))

        # ── fact_transactions ──
        if not tx_df.empty:
            # Build dimension maps from DataFrames (before adding surrogate keys)
            prod_df = dims.get("dim_product")
            loc_df = dims.get("dim_location")
            pm_df = dims.get("dim_payment_method")
            dev_df = dims.get("dim_device")

            if prod_df is not None:
                prod_map = dict(zip(prod_df["source_product_id"], range(1, len(prod_df) + 1)))

            loc_map = {}
            if loc_df is not None:
                for i, row in loc_df.iterrows():
                    key = (str(row["city"]), str(row["province"]))
                    loc_map[key] = len(loc_map) + 1

            pm_map = {}
            if pm_df is not None:
                for i, row in pm_df.iterrows():
                    key = (str(row["payment_method"]), str(row["payment_provider"]))
                    pm_map[key] = len(pm_map) + 1

            dev_map = {}
            if dev_df is not None:
                for i, row in dev_df.iterrows():
                    dev_map[str(row["device_fingerprint"])] = len(dev_map) + 1

            # Separate ghost transactions (no order_items) from regular transactions
            ghost = tx_df[tx_df["order_item_id"].isna()].copy()
            regular = tx_df[tx_df["order_item_id"].notna()].copy()

            # Process regular transactions (with order items)
            ft = regular[[
                "order_id", "customer_id", "product_id", "order_date",
                "total_amount", "discount_amount", "shipping_cost", "net_amount",
                "quantity", "unit_price", "is_flagged", "promo_id",
                "shipping_city", "shipping_province", "payment_method", "payment_provider",
                "device_fingerprint",
            ]].copy()

            # Process ghost transactions (orders without items)
            if not ghost.empty:
                ghost_ft = ghost[[
                    "order_id", "customer_id", "order_date",
                    "total_amount", "discount_amount", "shipping_cost", "net_amount",
                    "is_flagged", "promo_id",
                    "shipping_city", "shipping_province", "payment_method", "payment_provider",
                    "device_fingerprint",
                ]].copy()
                ghost_ft["product_id"] = None
                ghost_ft["quantity"] = 1
                ghost_ft["unit_price"] = ghost_ft["total_amount"]
                ft = pd.concat([ft, ghost_ft], ignore_index=True)

            ft["customer_key"] = ft["customer_id"].map(cust_map)
            ft["product_key"] = ft["product_id"].map(prod_map)
            ft["date_key"] = pd.to_datetime(ft["order_date"]).dt.strftime("%Y%m%d").astype(int)
            ft["has_promo"] = ft["promo_id"].notna()

            ft["location_key"] = ft.apply(
                lambda r: loc_map.get((str(r.get("shipping_city")), str(r.get("shipping_province")))), axis=1
            )
            ft["payment_key"] = ft.apply(
                lambda r: pm_map.get((str(r.get("payment_method")), str(r.get("payment_provider")))), axis=1
            )
            ft["device_key"] = ft["device_fingerprint"].map(dev_map)

            keep = ["order_id", "customer_key", "product_key",
                    "date_key", "location_key", "payment_key", "device_key",
                    "quantity", "unit_price", "total_amount",
                    "discount_amount", "shipping_cost", "net_amount",
                    "has_promo", "promo_id", "is_flagged"]
            ft = ft[keep]
            facts["fact_transactions"] = ft

        # ── fact_reviews ──
        if not rv_df.empty:
            fr = rv_df.copy()
            fr["date_key"] = pd.to_datetime(fr["reviewed_at"]).dt.strftime("%Y%m%d").astype(int)
            fr["customer_key"] = fr["customer_id"].map(cust_map)
            fr["product_key"] = fr["product_id"].map(prod_map)
            fr["device_key"] = fr["device_fingerprint"].map(dev_map) if "device_fingerprint" in fr.columns else None
            fr = fr.dropna(subset=["customer_key", "product_key"])
            keep = ["review_id", "product_key", "customer_key", "date_key", "device_key",
                    "rating", "review_source", "is_verified", "is_flagged"]
            available = [c for c in keep if c in fr.columns]
            facts["fact_reviews"] = fr[available]

        # ── fact_refunds ──
        if not rf_df.empty:
            frf = rf_df.copy()
            frf["date_key"] = pd.to_datetime(frf["processed_at"]).dt.strftime("%Y%m%d").astype(int)
            frf["customer_key"] = frf["customer_id"].map(cust_map)
            # Map payment_method from joined data
            frf["payment_key"] = frf.apply(
                lambda r: pm_map.get((str(r.get("payment_method")), str(r.get("payment_provider")))), axis=1
            )
            # Compare refund_to_account with customer's own bank account
            frf["refund_to_different_account"] = (
                frf["refund_to_account"].notna() &
                frf["customer_bank_account"].notna() &
                (frf["refund_to_account"] != frf["customer_bank_account"])
            )
            frf = frf.dropna(subset=["customer_key"])
            keep = ["refund_id", "order_id", "customer_key", "date_key", "payment_key",
                    "refund_amount", "refund_category", "refund_reason",
                    "refund_to_different_account", "is_flagged"]
            available = [c for c in keep if c in frf.columns]
            facts["fact_refunds"] = frf[available]

        # Save all dimensions and facts to staging files
        result = {"dimensions": {}, "facts": {}}
        for name, df in dims.items():
            path = _save(df, name)
            pk_col = [c for c in df.columns if c.endswith("_id") or c == "source_customer_id" or c == "source_product_id"]
            if not pk_col:
                pk_col = [df.columns[0]]
            result["dimensions"][name] = {"path": path, "rows": len(df), "pk": pk_col[:1]}

        for name, df in facts.items():
            path = _save(df, name)
            result["facts"][name] = {"path": path, "rows": len(df)}

        return result

    # ── LOAD ─────────────────────────────────────────────

    @task
    def load_date_dimension(dates):
        conn = get_olap()
        df = pd.DataFrame(dates)
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE dim_date RESTART IDENTITY CASCADE")
        conn.commit()
        cur.close()
        n = batch_insert_df(df, "dim_date", conn)
        conn.close()
        return {"dim_date": n}

    @task
    def load_dimensions(star_data):
        conn = get_olap()
        dims = star_data["dimensions"]
        results = {}
        for table, info in dims.items():
            df = _load(info["path"])
            cur = conn.cursor()
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
            conn.commit()
            cur.close()
            results[table] = batch_insert_df(df, table, conn, batch_size=2000)
        conn.close()
        return results

    @task
    def load_facts(star_data, dim_results):
        conn = get_olap()
        facts = star_data["facts"]
        results = {}
        for table, info in facts.items():
            df = _load(info["path"])
            cur = conn.cursor()
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
            conn.commit()
            cur.close()
            results[table] = batch_insert_df(df, table, conn, batch_size=5000)
        conn.close()
        return results

    @task
    def verify_data_quality(dim_results, fact_results):
        conn = get_olap()
        cur = conn.cursor()
        checks = {}

        for table in ["dim_customer", "dim_product", "dim_date", "dim_location",
                       "dim_payment_method", "dim_device", "fact_transactions",
                       "fact_reviews", "fact_refunds", "agg_daily_fraud_summary"]:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                checks[table] = cur.fetchone()[0]
            except Exception as e:
                checks[table] = f"ERROR: {e}"

        cur.execute("SELECT COUNT(*) FROM fact_transactions WHERE is_flagged = TRUE")
        checks["flagged_transactions"] = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM fact_reviews WHERE is_flagged = TRUE")
        checks["flagged_reviews"] = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM fact_reviews WHERE device_key IS NOT NULL")
        checks["reviews_with_device"] = cur.fetchone()[0]

        cur.execute("SELECT MIN(date_key), MAX(date_key) FROM fact_refunds")
        row = cur.fetchone()
        checks["refund_date_range"] = f"{row[0]} to {row[1]}"

        cur.execute("SELECT COUNT(*) FROM fact_refunds WHERE refund_to_different_account = TRUE")
        checks["refunds_to_diff_account"] = cur.fetchone()[0]

        cur.execute("SELECT SUM(net_amount) FROM fact_transactions")
        val = cur.fetchone()[0]
        checks["total_revenue"] = float(val) if val else 0

        cur.close()
        conn.close()
        return checks

    @task
    def populate_aggregate(fact_results):
        conn = get_olap()
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE agg_daily_fraud_summary")
        conn.commit()

        # Transaction fraud aggregates
        cur.execute("""
            INSERT INTO agg_daily_fraud_summary (date_key, city, fraud_type, total_transactions, flagged_transactions, total_amount, fraud_amount, fraud_rate)
            SELECT
                ft.date_key,
                COALESCE(loc.city, 'Unknown'),
                'transaction_fraud',
                COUNT(*),
                COUNT(*) FILTER (WHERE ft.is_flagged),
                SUM(ft.net_amount),
                COALESCE(SUM(ft.net_amount) FILTER (WHERE ft.is_flagged), 0),
                CASE WHEN COUNT(*) > 0
                     THEN ROUND(CAST(COUNT(*) FILTER (WHERE ft.is_flagged) AS NUMERIC) / COUNT(*), 4)
                     ELSE 0 END
            FROM fact_transactions ft
            JOIN dim_location loc ON ft.location_key = loc.location_id
            GROUP BY ft.date_key, loc.city
        """)
        conn.commit()

        # Fake review aggregates
        cur.execute("""
            INSERT INTO agg_daily_fraud_summary (date_key, city, fraud_type, total_transactions, flagged_transactions, total_amount, fraud_amount, fraud_rate)
            SELECT
                fr.date_key,
                'ALL',
                'fake_review',
                COUNT(*),
                COUNT(*) FILTER (WHERE fr.is_flagged),
                0,
                0,
                CASE WHEN COUNT(*) > 0
                     THEN ROUND(CAST(COUNT(*) FILTER (WHERE fr.is_flagged) AS NUMERIC) / COUNT(*), 4)
                     ELSE 0 END
            FROM fact_reviews fr
            GROUP BY fr.date_key
        """)
        conn.commit()

        # Refund abuse aggregates
        cur.execute("""
            INSERT INTO agg_daily_fraud_summary (date_key, city, fraud_type, total_transactions, flagged_transactions, total_amount, fraud_amount, fraud_rate)
            SELECT
                frf.date_key,
                COALESCE(cust.city, 'Unknown'),
                'refund_abuse',
                COUNT(*),
                COUNT(*) FILTER (WHERE frf.is_flagged),
                SUM(frf.refund_amount),
                COALESCE(SUM(frf.refund_amount) FILTER (WHERE frf.is_flagged), 0),
                CASE WHEN COUNT(*) > 0
                     THEN ROUND(CAST(COUNT(*) FILTER (WHERE frf.is_flagged) AS NUMERIC) / COUNT(*), 4)
                     ELSE 0 END
            FROM fact_refunds frf
            JOIN dim_customer cust ON frf.customer_key = cust.customer_id
            GROUP BY frf.date_key, cust.city
        """)
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM agg_daily_fraud_summary")
        agg_count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {"agg_rows": agg_count}

    # ── DEPENDENCIES ─────────────────────────────────────

    tx = extract_transactions()
    all_cust = extract_all_customers()
    rv = extract_reviews()
    rf = extract_refunds()
    dev = extract_devices()
    csv = extract_csv_files()
    dates = generate_date_dimension()

    star = transform_to_star_schema(tx, all_cust, rv, rf, csv, dev)

    dim_loaded = load_dimensions(star)
    date_loaded = load_date_dimension(dates)
    fact_loaded = load_facts(star, dim_loaded)
    agg_loaded = populate_aggregate(fact_loaded)

    verify_data_quality(dim_loaded, fact_loaded)


fraud_buster_etl_dag = fraud_buster_etl()
