-- =============================================================================
-- SQLite Inventory Comparison: HNAU vs VS snapshot
-- Author: Damian (ready-to-run)
-- =============================================================================
-- This script:
--   * Creates staging tables that match your CSV headers exactly.
--   * Imports CSVs from ~/Downloads/.
--   * Normalizes keys (TRIM + UPPER) and casts quantities to INTEGER.
--   * Deduplicates by SKU (SUM of qty per SKU).
--   * Emulates FULL OUTER JOIN to find mismatches and “only in one side”.
--   * Outputs details and stats. Optionally exports mismatches to CSV.
-- =============================================================================

-- Recommended display settings (won't affect data)
.headers on
.mode column
.width 18 12 12 12 14 12 14

-- -----------------------------------------------------------------------------
-- 0) (Optional) Start with a clean slate if re-running
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS hnau_norm;
DROP VIEW IF EXISTS vs_norm;
DROP TABLE IF EXISTS hnau_raw;
DROP TABLE IF EXISTS vs_raw;

-- -----------------------------------------------------------------------------
-- 1) Create staging tables that match CSV headers exactly
--    (Keep everything TEXT here; we will cast in normalized views.)
-- -----------------------------------------------------------------------------
CREATE TABLE hnau_raw (
  sku_oms_details_sku              TEXT,
  online_salable_qty_quantity      TEXT,
  sku_oms_details_sap_supplier_id  TEXT
);

CREATE TABLE vs_raw (
  account       TEXT,
  supplier_sku  TEXT,
  free_stock    TEXT
);

-- -----------------------------------------------------------------------------
-- 2) Import CSVs from Downloads
--    macOS/Linux: use ~/Downloads/...
--    Windows example: %USERPROFILE%\Downloads\...
--    NOTE: If your sqlite3 supports it, use --skip 1 to skip the header row.
-- -----------------------------------------------------------------------------
.mode csv

-- macOS/Linux paths (uncomment these two lines)
.import --skip 1 ~/Downloads/hnau_production_skus_29_08_2025.csv hnau_raw
.import --skip 1 ~/Downloads/vs_products_snapshot_29_08_2025.csv vs_raw

-- Windows PowerShell sample (comment the two lines above and uncomment below)
-- .import --skip 1 "%USERPROFILE%/Downloads/hnau_production_skus_29_08_2025.csv" hnau_raw
-- .import --skip 1 "%USERPROFILE%/Downloads/vs_products_snapshot_29_08_2025.csv" vs_raw

-- Fallback if your sqlite3 does NOT support --skip:
--   1) Leave .import as-is (it will import headers as a first row).
--   2) Then delete the header rows:
--      DELETE FROM hnau_raw WHERE rowid = (SELECT MIN(rowid) FROM hnau_raw);
--      DELETE FROM vs_raw   WHERE rowid = (SELECT MIN(rowid) FROM vs_raw);

-- -----------------------------------------------------------------------------
-- 3) Normalized, deduplicated views (sum qty per SKU; keep one supplier/account)
-- -----------------------------------------------------------------------------
CREATE VIEW hnau_norm AS
SELECT
  UPPER(TRIM(sku_oms_details_sku))                         AS sku,
  SUM(
    CAST(
      COALESCE(NULLIF(TRIM(online_salable_qty_quantity), ''), '0') AS INTEGER
    )
  )                                                        AS qty,
  MIN(TRIM(sku_oms_details_sap_supplier_id))               AS supplier_id
FROM hnau_raw
WHERE TRIM(sku_oms_details_sku) IS NOT NULL
GROUP BY 1;

CREATE VIEW vs_norm AS
SELECT
  UPPER(TRIM(supplier_sku))                                AS sku,
  SUM(
    CAST(COALESCE(NULLIF(TRIM(free_stock), ''), '0') AS INTEGER)
  )                                                        AS qty,
  MIN(TRIM(account))                                       AS account
FROM vs_raw
WHERE TRIM(supplier_sku) IS NOT NULL
GROUP BY 1;

-- Quick sanity checks (row counts)
SELECT 'hnau_norm_rows' AS metric, COUNT(*) AS value FROM hnau_norm
UNION ALL
SELECT 'vs_norm_rows'   AS metric, COUNT(*) AS value FROM vs_norm;

-- -----------------------------------------------------------------------------
-- 4) Detailed mismatch report (emulated FULL OUTER JOIN)
--    - Includes:
--       * ONLY_IN_HNAU (present only in hnau)
--       * ONLY_IN_VS   (present only in vs)
--       * QTY_MISMATCH (present in both but different qty)
--       * MATCH        (present in both with same qty) -- filtered out below
-- -----------------------------------------------------------------------------
WITH joined AS (
  -- Left side: all HNAU; mark status depending on VS presence/quantity
  SELECT
    h.sku                               AS sku,
    h.qty                               AS hnau_qty,
    v.qty                               AS vs_qty,
    (COALESCE(v.qty, 0) - COALESCE(h.qty, 0)) AS qty_diff,
    h.supplier_id                       AS supplier_id,
    v.account                           AS account,
    CASE
      WHEN v.sku IS NULL THEN 'ONLY_IN_HNAU'
      WHEN COALESCE(h.qty, 0) = COALESCE(v.qty, 0) THEN 'MATCH'
      ELSE 'QTY_MISMATCH'
    END                                 AS status
  FROM hnau_norm h
  LEFT JOIN vs_norm v
    ON v.sku = h.sku

  UNION ALL

  -- Right-only rows: present in VS but not in HNAU
  SELECT
    v.sku                               AS sku,
    h.qty                               AS hnau_qty,
    v.qty                               AS vs_qty,
    (COALESCE(v.qty, 0) - COALESCE(h.qty, 0)) AS qty_diff,
    h.supplier_id                       AS supplier_id,
    v.account                           AS account,
    'ONLY_IN_VS'                        AS status
  FROM vs_norm v
  LEFT JOIN hnau_norm h
    ON h.sku = v.sku
  WHERE h.sku IS NULL
)

-- Show only mismatches and existence differences
SELECT *
FROM joined
WHERE status <> 'MATCH'
ORDER BY status, sku;

-- -----------------------------------------------------------------------------
-- 5) Summary statistics
-- -----------------------------------------------------------------------------
WITH joined AS (
  SELECT
    h.sku                               AS sku,
    h.qty                               AS hnau_qty,
    v.qty                               AS vs_qty,
    (COALESCE(v.qty, 0) - COALESCE(h.qty, 0)) AS qty_diff,
    h.supplier_id                       AS supplier_id,
    v.account                           AS account,
    CASE
      WHEN v.sku IS NULL THEN 'ONLY_IN_HNAU'
      WHEN COALESCE(h.qty, 0) = COALESCE(v.qty, 0) THEN 'MATCH'
      ELSE 'QTY_MISMATCH'
    END                                 AS status
  FROM hnau_norm h
  LEFT JOIN vs_norm v
    ON v.sku = h.sku

  UNION ALL

  SELECT
    v.sku                               AS sku,
    h.qty                               AS hnau_qty,
    v.qty                               AS vs_qty,
    (COALESCE(v.qty, 0) - COALESCE(h.qty, 0)) AS qty_diff,
    h.supplier_id                       AS supplier_id,
    v.account                           AS account,
    'ONLY_IN_VS'                        AS status
  FROM vs_norm v
  LEFT JOIN hnau_norm h
    ON h.sku = v.sku
  WHERE h.sku IS NULL
)
SELECT
  (SELECT COUNT(*) FROM hnau_norm)                                                       AS hnau_rows,
  (SELECT COUNT(*) FROM vs_norm)                                                         AS vs_rows,
  SUM(CASE WHEN status = 'MATCH'        THEN 1 ELSE 0 END)                               AS matches,
  SUM(CASE WHEN status = 'QTY_MISMATCH' THEN 1 ELSE 0 END)                               AS qty_mismatches,
  SUM(CASE WHEN status = 'ONLY_IN_HNAU' THEN 1 ELSE 0 END)                               AS only_in_hnau,
  SUM(CASE WHEN status = 'ONLY_IN_VS'   THEN 1 ELSE 0 END)                               AS only_in_vs,
  -- Totals seen across the joined set (NULL-safe)
  SUM(COALESCE(hnau_qty, 0))                                                             AS total_hnau_qty,
  SUM(COALESCE(vs_qty, 0))                                                               AS total_vs_qty,
  -- Discrepancy magnitudes:
  SUM(CASE WHEN status = 'QTY_MISMATCH' THEN ABS(qty_diff) ELSE 0 END)                   AS sum_abs_qty_diff,
  AVG(CASE WHEN status = 'QTY_MISMATCH' THEN ABS(qty_diff) END)                          AS avg_abs_qty_diff
FROM joined;

-- -----------------------------------------------------------------------------
-- 6) (Optional) Top 50 largest quantity discrepancies
-- -----------------------------------------------------------------------------
WITH joined AS (
  SELECT
    h.sku AS sku, h.qty AS hnau_qty, v.qty AS vs_qty,
    (COALESCE(v.qty, 0) - COALESCE(h.qty, 0)) AS qty_diff,
    h.supplier_id, v.account,
    CASE
      WHEN v.sku IS NULL THEN 'ONLY_IN_HNAU'
      WHEN COALESCE(h.qty, 0) = COALESCE(v.qty, 0) THEN 'MATCH'
      ELSE 'QTY_MISMATCH'
    END AS status
  FROM hnau_norm h
  LEFT JOIN vs_norm v ON v.sku = h.sku
  UNION ALL
  SELECT
    v.sku AS sku, h.qty AS hnau_qty, v.qty AS vs_qty,
    (COALESCE(v.qty, 0) - COALESCE(h.qty, 0)) AS qty_diff,
    h.supplier_id, v.account,
    'ONLY_IN_VS' AS status
  FROM vs_norm v
  LEFT JOIN hnau_norm h ON h.sku = v.sku
  WHERE h.sku IS NULL
)
SELECT *
FROM joined
WHERE status = 'QTY_MISMATCH'
ORDER BY ABS(qty_diff) DESC, sku
LIMIT 50;

-- -----------------------------------------------------------------------------
-- 7) (Optional) Export mismatches to CSV in Downloads
-- -----------------------------------------------------------------------------
-- Switch to CSV for export and send next query result to file, then reset.
.mode csv
.headers on
.once ~/Downloads/stock_mismatches_29_08_2025.csv
WITH joined AS (
  SELECT
    h.sku AS sku, h.qty AS hnau_qty, v.qty AS vs_qty,
    (COALESCE(v.qty, 0) - COALESCE(h.qty, 0)) AS qty_diff,
    h.supplier_id, v.account,
    CASE
      WHEN v.sku IS NULL THEN 'ONLY_IN_HNAU'
      WHEN COALESCE(h.qty, 0) = COALESCE(v.qty, 0) THEN 'MATCH'
      ELSE 'QTY_MISMATCH'
    END AS status
  FROM hnau_norm h
  LEFT JOIN vs_norm v ON v.sku = h.sku
  UNION ALL
  SELECT
    v.sku AS sku, h.qty AS hnau_qty, v.qty AS vs_qty,
    (COALESCE(v.qty, 0) - COALESCE(h.qty, 0)) AS qty_diff,
    h.supplier_id, v.account,
    'ONLY_IN_VS' AS status
  FROM vs_norm v
  LEFT JOIN hnau_norm h ON h.sku = v.sku
  WHERE h.sku IS NULL
)
SELECT *
FROM joined
WHERE status <> 'MATCH'
ORDER BY status, sku;
.once

-- Restore console-friendly format
.mode column
