import os
import sqlite3
import pandas as pd
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import datetime

downloads = os.path.join(os.path.expanduser("~"), "Downloads")
hnau_csv = os.path.join(downloads, "hnau_production_skus_29_08_2025.csv")
vs_csv   = os.path.join(downloads, "vs_products_snapshot_29_08_2025.csv")
db_path  = os.path.join(downloads, "inventory.db")

date_tag = datetime.today().strftime('%d_%m_%Y')
export_mismatch = os.path.join(downloads, f"stock_mismatches_{date_tag}.csv")
export_only_hnau = os.path.join(downloads, f"only_in_hnau_{date_tag}.csv")
export_only_vs   = os.path.join(downloads, f"only_in_vs_{date_tag}.csv")

def clean_sku(x: str) -> str:
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s == "":
        return None
    return s.upper()

def parse_qty_to_int(x) -> int:
    """
    Robust integer parsing for quantities:
    - Accepts '', None => 0
    - Strips commas/spaces
    - Supports '(123)' as -123
    - Supports decimals like '41820.0' => rounds half up to int
    - If non-numeric, returns 0
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0
    s = str(x).strip()
    if s == "" or s.upper() in {"NULL", "NAN"}:
        return 0
    is_paren_negative = s.startswith("(") and s.endswith(")")
    if is_paren_negative:
        s = s[1:-1]
    s = s.replace(",", "")
    try:
        d = Decimal(s)
        i = int(d.to_integral_value(rounding=ROUND_HALF_UP))
        return -i if is_paren_negative else i
    except (InvalidOperation, ValueError):
        try:
            i = int(float(s))
            return -i if is_paren_negative else i
        except Exception:
            return 0

def to_nullable_int_series(series: pd.Series) -> pd.Series:
    return series.map(parse_qty_to_int).astype("Int64")

def drop_object(conn, name: str):
    row = conn.execute(
        "SELECT type FROM sqlite_master WHERE name = ?", (name,)
    ).fetchone()
    if not row:
        return
    obj_type = row[0]
    if obj_type == "view":
        conn.execute(f"DROP VIEW IF EXISTS {name}")
    elif obj_type == "table":
        conn.execute(f"DROP TABLE IF EXISTS {name}")

print("Loading CSVs...")
hnau_df = pd.read_csv(
    hnau_csv,
    dtype={
        "sku_oms_details_sku": "string",
        "online_salable_qty_quantity": "string",
        "sku_oms_details_sap_supplier_id": "string",
    }
)

vs_df = pd.read_csv(
    vs_csv,
    dtype={
        "account": "string",
        "supplier_sku": "string",
        "free_stock": "string",
    }
)

print("Normalizing data (SKU + qty types)...")
hnau_norm = (
    hnau_df.assign(
        sku=hnau_df["sku_oms_details_sku"].map(clean_sku),
        qty=to_nullable_int_series(hnau_df["online_salable_qty_quantity"]),
        supplier_id=hnau_df["sku_oms_details_sap_supplier_id"].astype("string").str.strip()
    )
    .dropna(subset=["sku"])
    .groupby("sku", as_index=False)
    .agg(qty=("qty", "sum"), supplier_id=("supplier_id", "min"))
)

vs_norm = (
    vs_df.assign(
        sku=vs_df["supplier_sku"].map(clean_sku),
        qty=to_nullable_int_series(vs_df["free_stock"]),
        account=vs_df["account"].astype("string").str.strip()
    )
    .dropna(subset=["sku"])
    .groupby("sku", as_index=False)
    .agg(qty=("qty", "sum"), account=("account", "min"))
)

hnau_norm["qty"] = hnau_norm["qty"].astype("Int64")
vs_norm["qty"]   = vs_norm["qty"].astype("Int64")

print("Writing normalized tables to SQLite...")
conn = sqlite3.connect(db_path)
cur = conn.cursor()

for name in ("hnau_norm", "vs_norm"):
    drop_object(conn, name)

cur.execute("""
CREATE TABLE hnau_norm (
    sku         TEXT PRIMARY KEY,
    qty         INTEGER,
    supplier_id TEXT
)
""")
cur.execute("""
CREATE TABLE vs_norm (
    sku     TEXT PRIMARY KEY,
    qty     INTEGER,
    account TEXT
)
""")

hnau_norm.to_sql("hnau_norm", conn, if_exists="append", index=False)
vs_norm.to_sql("vs_norm", conn, if_exists="append", index=False)

base_join = """
WITH joined AS (
  SELECT
    h.sku AS sku,
    CAST(h.qty AS INTEGER) AS hnau_qty,
    CAST(v.qty AS INTEGER) AS vs_qty,
    CAST((COALESCE(v.qty,0) - COALESCE(h.qty,0)) AS INTEGER) AS qty_diff,
    h.supplier_id AS supplier_id,
    v.account     AS account,
    CASE
      WHEN v.sku IS NULL THEN 'ONLY_IN_HNAU'
      WHEN COALESCE(h.qty,0) = COALESCE(v.qty,0) THEN 'MATCH'
      ELSE 'QTY_MISMATCH'
    END AS status
  FROM hnau_norm h
  LEFT JOIN vs_norm v ON v.sku = h.sku

  UNION ALL

  SELECT
    v.sku AS sku,
    CAST(h.qty AS INTEGER) AS hnau_qty,
    CAST(v.qty AS INTEGER) AS vs_qty,
    CAST((COALESCE(v.qty,0) - COALESCE(h.qty,0)) AS INTEGER) AS qty_diff,
    h.supplier_id AS supplier_id,
    v.account     AS account,
    'ONLY_IN_VS'  AS status
  FROM vs_norm v
  LEFT JOIN hnau_norm h ON h.sku = v.sku
  WHERE h.sku IS NULL
)
"""

stats_sql = base_join + """
SELECT
  (SELECT COUNT(*) FROM hnau_norm) AS hnau_rows,
  (SELECT COUNT(*) FROM vs_norm)   AS vs_rows,
  SUM(CASE WHEN status = 'MATCH'        THEN 1 ELSE 0 END) AS matches,
  SUM(CASE WHEN status = 'QTY_MISMATCH' THEN 1 ELSE 0 END) AS qty_mismatches,
  SUM(CASE WHEN status = 'ONLY_IN_HNAU' THEN 1 ELSE 0 END) AS only_in_hnau,
  SUM(CASE WHEN status = 'ONLY_IN_VS'   THEN 1 ELSE 0 END) AS only_in_vs,
  SUM(COALESCE(hnau_qty,0)) AS total_hnau_qty,
  SUM(COALESCE(vs_qty,0))   AS total_vs_qty,
  SUM(CASE WHEN status = 'QTY_MISMATCH' THEN ABS(qty_diff) ELSE 0 END) AS sum_abs_qty_diff,
  AVG(CASE WHEN status = 'QTY_MISMATCH' THEN ABS(qty_diff) END)        AS avg_abs_qty_diff
FROM joined;
"""

mismatch_sql = base_join + "SELECT * FROM joined WHERE status = 'QTY_MISMATCH' ORDER BY ABS(qty_diff) DESC, sku;"
only_hnau_sql = base_join + "SELECT * FROM joined WHERE status = 'ONLY_IN_HNAU' ORDER BY sku;"
only_vs_sql   = base_join + "SELECT * FROM joined WHERE status = 'ONLY_IN_VS'   ORDER BY sku;"

stats_df      = pd.read_sql_query(stats_sql, conn)
mismatches_df = pd.read_sql_query(mismatch_sql, conn)
only_hnau_df  = pd.read_sql_query(only_hnau_sql, conn)
only_vs_df    = pd.read_sql_query(only_vs_sql, conn)

conn.close()

for df in (mismatches_df, only_hnau_df, only_vs_df):
    for col in ("hnau_qty", "vs_qty", "qty_diff"):
        if col in df.columns:
            df[col] = df[col].astype("Int64")

print("\n=== SUMMARY STATS ===")
print(stats_df.to_string(index=False))

print("\n=== QTY_MISMATCH (differences only) ===")
print("None" if mismatches_df.empty else mismatches_df.to_string(index=False))

print("\n=== ONLY_IN_HNAU ===")
print("None" if only_hnau_df.empty else only_hnau_df.to_string(index=False))

print("\n=== ONLY_IN_VS ===")
print("None" if only_vs_df.empty else only_vs_df.to_string(index=False))

mismatches_df.to_csv(export_mismatch, index=False)
only_hnau_df.to_csv(export_only_hnau, index=False)
only_vs_df.to_csv(export_only_vs, index=False)

print(f"\nExports:")
print(f" - Mismatches:   {export_mismatch}")
print(f" - Only in HNAU: {export_only_hnau}")
print(f" - Only in VS:   {export_only_vs}")
print(f"\nSQLite DB: {db_path}")
