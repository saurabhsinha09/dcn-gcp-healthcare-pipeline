TRUNCATE TABLE `dcn-development.healthcare_temp.audit_log`;

TRUNCATE TABLE `dcn-development.healthcare_temp.pipeline_logs`;

--TRUNCATE TABLE `dcn-development.healthcare_bronze.departments_ha`;

--TRUNCATE TABLE `dcn-development.healthcare_bronze.encounters_ha`;

--TRUNCATE TABLE `dcn-development.healthcare_bronze.patients_ha`;

--TRUNCATE TABLE `dcn-development.healthcare_bronze.providers_ha`;

--TRUNCATE TABLE `dcn-development.healthcare_bronze.transactions_ha`;

--TRUNCATE TABLE `dcn-development.healthcare_bronze.departments_hb`;

--TRUNCATE TABLE `dcn-development.healthcare_bronze.encounters_hb`;

--TRUNCATE TABLE `dcn-development.healthcare_bronze.patients_hb`;

--TRUNCATE TABLE `dcn-development.healthcare_bronze.providers_hb`;

--TRUNCATE TABLE `dcn-development.healthcare_bronze.transactions_hb`;

TRUNCATE TABLE `dcn-development.healthcare_silver.departments`;

TRUNCATE TABLE `dcn-development.healthcare_silver.providers`;

TRUNCATE TABLE `dcn-development.healthcare_silver.patients`;

TRUNCATE TABLE `dcn-development.healthcare_silver.transactions`;

TRUNCATE TABLE `dcn-development.healthcare_silver.encounters`;

TRUNCATE TABLE `dcn-development.healthcare_silver.claims`;

TRUNCATE TABLE `dcn-development.healthcare_silver.cpt_codes`;

TRUNCATE TABLE `dcn-development.healthcare_gold.provider_charge_summary`;

TRUNCATE TABLE `dcn-development.healthcare_gold.patient_history`;

TRUNCATE TABLE `dcn-development.healthcare_gold.provider_performance`;

TRUNCATE TABLE `dcn-development.healthcare_gold.department_performance`;

TRUNCATE TABLE `dcn-development.healthcare_gold.financial_metrics`;

TRUNCATE TABLE `dcn-development.healthcare_gold.payor_performance`;