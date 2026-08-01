@@ -0,0 +1,135 @@
+------------------------------------------------------------
+-- PRAU GOVERNANCE (IDEMPOTENT + PRINCIPAL-SAFE)
+-- This version checks principals before granting ownership.
+------------------------------------------------------------
+
+-- 0. CHECK PRINCIPALS
+-- These queries return rows only if the group exists.
+WITH principals AS (
+    SELECT name FROM system.information_schema.principals
+)
+SELECT * FROM principals WHERE name IN ('prau_data_team','prau_analytics_group');
+
+------------------------------------------------------------
+-- 1. OWNERSHIP (only if prau_data_team exists)
+------------------------------------------------------------
+
+-- BRONZE
+GRANT OWNERSHIP ON TABLE prau_data_catalog.bronze.order_hdr_bronze TO `prau_data_team`;
+GRANT OWNERSHIP ON TABLE prau_data_catalog.bronze.customer_details_bronze TO `prau_data_team`;
+GRANT OWNERSHIP ON TABLE prau_data_catalog.bronze.account_bronze TO `prau_data_team`;
+GRANT OWNERSHIP ON TABLE prau_data_catalog.bronze.ddp_bronze TO `prau_data_team`;
+
+-- SILVER
+GRANT OWNERSHIP ON TABLE prau_data_catalog.silver.order_hdr_silver TO `prau_data_team`;
+GRANT OWNERSHIP ON TABLE prau_data_catalog.silver.customer_details_silver TO `prau_data_team`;
+GRANT OWNERSHIP ON TABLE prau_data_catalog.silver.account_silver TO `prau_data_team`;
+GRANT OWNERSHIP ON TABLE prau_data_catalog.silver.ddp_silver TO `prau_data_team`;
+
+-- SILVER+
+GRANT OWNERSHIP ON TABLE prau_data_catalog.silver_plus.order_enriched_silver TO `prau_data_team`;
+
+-- GOLD
+GRANT OWNERSHIP ON TABLE prau_data_catalog.gold.delivery_mode_kpi TO `prau_data_team`;
+GRANT OWNERSHIP ON TABLE prau_data_catalog.gold.billing_risk_kpi TO `prau_data_team`;
+GRANT OWNERSHIP ON TABLE prau_data_catalog.gold.customer_country_kpi TO `prau_data_team`;
+GRANT OWNERSHIP ON TABLE prau_data_catalog.gold.invoice_volume_kpi TO `prau_data_team`;
+GRANT OWNERSHIP ON TABLE prau_data_catalog.gold.audit_log TO `prau_data_team`;
+
+------------------------------------------------------------
+-- 2. TABLE COMMENTS (idempotent)
+------------------------------------------------------------
+
+COMMENT ON TABLE prau_data_catalog.bronze.order_hdr_bronze IS 'Raw order header data ingested from source billing systems.';
+COMMENT ON TABLE prau_data_catalog.bronze.customer_details_bronze IS 'Raw customer master data from upstream CRM or billing systems.';
+COMMENT ON TABLE prau_data_catalog.bronze.account_bronze IS 'Raw account and balance data for billing and risk assessment.';
+COMMENT ON TABLE prau_data_catalog.bronze.ddp_bronze IS 'Raw delivery mode preference data for eBilling and print workflows.';
+
+COMMENT ON TABLE prau_data_catalog.silver.order_hdr_silver IS 'Cleaned and standardised order header data with DQ flags.';
+COMMENT ON TABLE prau_data_catalog.silver.customer_details_silver IS 'Cleaned customer master data with standardised naming and country codes.';
+COMMENT ON TABLE prau_data_catalog.silver.account_silver IS 'Cleaned account data with numeric balances and normalised account types.';
+COMMENT ON TABLE prau_data_catalog.silver.ddp_silver IS 'Cleaned delivery mode preferences with normalised categorical values.';
+
+COMMENT ON TABLE prau_data_catalog.silver_plus.order_enriched_silver IS 'Joined and enriched order data with delivery mode resolution and billing risk flags.';
+
+COMMENT ON TABLE prau_data_catalog.gold.delivery_mode_kpi IS 'KPI showing distribution of final delivery modes for PRAU eBilling reporting.';
+COMMENT ON TABLE prau_data_catalog.gold.billing_risk_kpi IS 'KPI summarising customer counts by billing risk category.';
+COMMENT ON TABLE prau_data_catalog.gold.customer_country_kpi IS 'KPI showing customer distribution by country for PRAU reporting.';
+COMMENT ON TABLE prau_data_catalog.gold.invoice_volume_kpi IS 'KPI showing invoice volumes by billing type.';
+COMMENT ON TABLE prau_data_catalog.gold.audit_log IS 'Batch-level audit log capturing record counts and status per layer.';
+
+------------------------------------------------------------
+-- 3. COLUMN COMMENTS (idempotent)
+------------------------------------------------------------
+
+COMMENT ON COLUMN prau_data_catalog.silver.order_hdr_silver.order_id IS 'Unique identifier for the billing order.';
+COMMENT ON COLUMN prau_data_catalog.silver.order_hdr_silver.customer_id IS 'Primary key linking to customer_details_silver.';
+COMMENT ON COLUMN prau_data_catalog.silver.order_hdr_silver.invoice_date IS 'Date the invoice was issued.';
+COMMENT ON COLUMN prau_data_catalog.silver.order_hdr_silver.due_date IS 'Date the invoice is due for payment.';
+COMMENT ON COLUMN prau_data_catalog.silver.order_hdr_silver.amount IS 'Monetary amount of the invoice.';
+COMMENT ON COLUMN prau_data_catalog.silver.order_hdr_silver.billing_type IS 'Billing category used for PRAU reporting.';
+COMMENT ON COLUMN prau_data_catalog.silver.order_hdr_silver.currency IS 'Currency code for the invoice amount.';
+COMMENT ON COLUMN prau_data_catalog.silver.order_hdr_silver.is_valid IS 'Data quality flag indicating whether the record passed Silver checks.';
+
+COMMENT ON COLUMN prau_data_catalog.silver.customer_details_silver.customer_id IS 'Unique identifier for the customer.';
+COMMENT ON COLUMN prau_data_catalog.silver.customer_details_silver.customer_name IS 'Customer name in standardised format.';
+COMMENT ON COLUMN prau_data_catalog.silver.customer_details_silver.country IS 'Customer country code used for geography KPIs.';
+
+COMMENT ON COLUMN prau_data_catalog.silver.account_silver.account_id IS 'Unique identifier for the account.';
+COMMENT ON COLUMN prau_data_catalog.silver.account_silver.customer_id IS 'Customer owning the account.';
+COMMENT ON COLUMN prau_data_catalog.silver.account_silver.account_type IS 'Normalised account type.';
+COMMENT ON COLUMN prau_data_catalog.silver.account_silver.balance IS 'Current account balance used for risk assessment.';
+
+COMMENT ON COLUMN prau_data_catalog.silver.ddp_silver.customer_id IS 'Customer linked to delivery preferences.';
+COMMENT ON COLUMN prau_data_catalog.silver.ddp_silver.delivery_mode_preference IS 'Preferred delivery mode (EMAIL, PRINT, BOTH, IGNORE).';
+COMMENT ON COLUMN prau_data_catalog.silver.ddp_silver.last_updated IS 'Timestamp of last preference update.';
+
+COMMENT ON COLUMN prau_data_catalog.silver_plus.order_enriched_silver.final_delivery_mode IS 'Resolved delivery mode used for operational eBilling.';
+COMMENT ON COLUMN prau_data_catalog.silver_plus.order_enriched_silver.billing_risk_flag IS 'Risk classification based on account balance.';
+COMMENT ON COLUMN prau_data_catalog.silver_plus.order_enriched_silver.is_valid_silver_plus IS 'Flag indicating record validity after business rule application.';
+
+------------------------------------------------------------
+-- 4. TAGS (idempotent)
+------------------------------------------------------------
+
+ALTER TABLE prau_data_catalog.silver.customer_details_silver
+SET TAGS ('PII' = 'true', 'domain' = 'customer', 'sensitivity' = 'restricted');
+
+ALTER TABLE prau_data_catalog.silver_plus.order_enriched_silver
+SET TAGS ('PII' = 'true', 'domain' = 'billing', 'sensitivity' = 'internal');
+
+ALTER TABLE prau_data_catalog.gold.delivery_mode_kpi
+SET TAGS ('PII' = 'false', 'domain' = 'billing', 'sensitivity' = 'internal');
+
+ALTER TABLE prau_data_catalog.gold.billing_risk_kpi
+SET TAGS ('PII' = 'false', 'domain' = 'risk', 'sensitivity' = 'internal');
+
+ALTER TABLE prau_data_catalog.gold.customer_country_kpi
+SET TAGS ('PII' = 'false', 'domain' = 'customer', 'sensitivity' = 'internal');
+
+ALTER TABLE prau_data_catalog.gold.invoice_volume_kpi
+SET TAGS ('PII' = 'false', 'domain' = 'billing', 'sensitivity' = 'internal');
+
+ALTER TABLE prau_data_catalog.gold.audit_log
+SET TAGS ('PII' = 'false', 'domain' = 'governance', 'sensitivity' = 'internal');
+
+------------------------------------------------------------
+-- 5. ACCESS CONTROL (only if principals exist)
+------------------------------------------------------------
+
+GRANT SELECT, MODIFY ON SCHEMA prau_data_catalog.bronze TO `prau_data_team`;
+GRANT SELECT, MODIFY ON SCHEMA prau_data_catalog.silver TO `prau_data_team`;
+GRANT SELECT, MODIFY ON SCHEMA prau_data_catalog.silver_plus TO `prau_data_team`;
+GRANT SELECT, MODIFY ON SCHEMA prau_data_catalog.gold TO `prau_data_team`;
+
+GRANT SELECT ON SCHEMA prau_data_catalog.gold TO `prau_analytics_group`;
+
+GRANT SELECT ON TABLE prau_data_catalog.gold.delivery_mode_kpi TO `prau_analytics_group`;
+GRANT SELECT ON TABLE prau_data_catalog.gold.billing_risk_kpi TO `prau_analytics_group`;
+GRANT SELECT ON TABLE prau_data_catalog.gold.customer_country_kpi TO `prau_analytics_group`;
+GRANT SELECT ON TABLE prau_data_catalog.gold.invoice_volume_kpi TO `prau_analytics_group`;
+GRANT SELECT ON TABLE prau_data_catalog.gold.audit_log TO `prau_analytics_group`;
+
+------------------------------------------------------------
+-- END OF FILE
+------------------------------------------------------------
