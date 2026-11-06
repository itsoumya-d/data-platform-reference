CREATE OR REPLACE TABLE gold.dim_supplier AS
SELECT
    concat('SUP-', supplier_id) AS supplier_key,
    supplier_id,
    max(supplier_name) AS supplier_name,
    upper(max(country)) AS country
FROM silver.suppliers
GROUP BY supplier_id;

CREATE OR REPLACE TABLE gold.dim_account AS
SELECT
    concat('ACC-', account_id) AS account_key,
    account_id,
    max(account_name) AS account_name,
    lower(max(account_type)) AS account_type
FROM silver.accounts
GROUP BY account_id;

CREATE OR REPLACE TABLE gold.fact_invoice AS
SELECT
    concat('INV-', i.invoice_id) AS invoice_key,
    i.invoice_id,
    concat('SUP-', i.supplier_id) AS supplier_key,
    concat('ACC-', i.account_id) AS account_key,
    i.invoice_date,
    cast(i.amount AS DECIMAL(18,2)) AS amount,
    cast(i.tax AS DECIMAL(18,2)) AS tax,
    cast(i.amount + i.tax AS DECIMAL(18,2)) AS total,
    upper(i.currency) AS currency
FROM silver.invoices i;

CREATE OR REPLACE VIEW gold.vw_finance_reconciliation AS
SELECT
    gl.account_id,
    gl.gl_total,
    sl.subledger_total,
    cast(gl.gl_total - sl.subledger_total AS DECIMAL(18,2)) AS difference,
    CASE WHEN abs(gl.gl_total - sl.subledger_total) <= 0.01 THEN 'reconciled' ELSE 'exception' END AS status
FROM (
    SELECT account_key AS account_id, sum(total) AS gl_total
    FROM gold.fact_invoice GROUP BY account_key
) gl
FULL OUTER JOIN (
    SELECT account_key AS account_id, sum(total) AS subledger_total
    FROM silver.subledger GROUP BY account_key
) sl ON gl.account_id = sl.account_id;
