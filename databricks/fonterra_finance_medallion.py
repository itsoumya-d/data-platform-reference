from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, lower, upper

spark = SparkSession.builder.appName("fonterra-finance-medallion").getOrCreate()

invoices = spark.read.table("bronze.invoices")
suppliers = spark.read.table("bronze.suppliers")
accounts = spark.read.table("bronze.accounts")

silver_invoices = invoices.select(
    col("invoice_id"),
    col("supplier_id"),
    col("account_id"),
    col("invoice_date").cast("date"),
    col("amount").cast("decimal(18,2)"),
    col("tax").cast("decimal(18,2)"),
    upper(col("currency")).alias("currency"),
)
silver_suppliers = suppliers.select("supplier_id", "supplier_name", upper(col("country")).alias("country"))
silver_accounts = accounts.select("account_id", "account_name", lower(col("account_type")).alias("account_type"))

silver_invoices.write.mode("overwrite").saveAsTable("silver.invoices")
silver_suppliers.write.mode("overwrite").saveAsTable("silver.suppliers")
silver_accounts.write.mode("overwrite").saveAsTable("silver.accounts")

gold_fact = silver_invoices.select(
    concat(lit("INV-"), col("invoice_id")).alias("invoice_key"),
    col("invoice_id"),
    concat(lit("SUP-"), col("supplier_id")).alias("supplier_key"),
    concat(lit("ACC-"), col("account_id")).alias("account_key"),
    col("invoice_date"),
    col("amount"),
    col("tax"),
    (col("amount") + col("tax")).cast("decimal(18,2)").alias("total"),
    col("currency"),
)
gold_fact.write.mode("overwrite").saveAsTable("gold.fact_invoice")
