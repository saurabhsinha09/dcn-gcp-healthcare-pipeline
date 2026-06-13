-- Description: Create external tables for bronze dataset in BigQuery
-- please do not forget to replace the bucket path

CREATE EXTERNAL TABLE IF NOT EXISTS `dcn-development.healthcare_bronze.departments_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://dcn-healthcare-bucket/landing/hospital-a/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `dcn-development.healthcare_bronze.encounters_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://dcn-healthcare-bucket/landing/hospital-a/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `dcn-development.healthcare_bronze.patients_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://dcn-healthcare-bucket/landing/hospital-a/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `dcn-development.healthcare_bronze.providers_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://dcn-healthcare-bucket/landing/hospital-a/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `dcn-development.healthcare_bronze.transactions_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://dcn-healthcare-bucket/landing/hospital-a/transactions/*.json']
);

---------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS `dcn-development.healthcare_bronze.departments_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://dcn-healthcare-bucket/landing/hospital-b/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `dcn-development.healthcare_bronze.encounters_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://dcn-healthcare-bucket/landing/hospital-b/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `dcn-development.healthcare_bronze.patients_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://dcn-healthcare-bucket/landing/hospital-b/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `dcn-development.healthcare_bronze.providers_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://dcn-healthcare-bucket/landing/hospital-b/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `dcn-development.healthcare_bronze.transactions_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://dcn-healthcare-bucket/landing/hospital-b/transactions/*.json']
);

---------------------------------------------------------------------------------------------------------------------------