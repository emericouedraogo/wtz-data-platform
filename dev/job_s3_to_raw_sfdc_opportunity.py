import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3 - VWTS
AmazonS3VWTS_node1747755561296 = glueContext.create_dynamic_frame.from_catalog(database="raw", table_name="raw_sfdc_opportunity_vwts", transformation_ctx="AmazonS3VWTS_node1747755561296")

# Script generated for node Amazon S3 - VWT
AmazonS3VWT_node1747755513736 = glueContext.create_dynamic_frame.from_catalog(database="raw", table_name="raw_sfdc_opportunity_vwt", transformation_ctx="AmazonS3VWT_node1747755513736")

# Script generated for node opportunity_vwts
opportunity_vwts_node1749083317341 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3VWTS_node1747755561296, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-017820666813-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_sfdc.opportunity_vwts", "connectionName": "Redshift wtz-dataplatform-dev-public", "preactions": "CREATE TABLE IF NOT EXISTS raw_sfdc.opportunity_vwts (id VARCHAR, accountid VARCHAR, stagename VARCHAR, amount DOUBLE PRECISION, probability DOUBLE PRECISION, closedate DATE, leadsource VARCHAR, isclosed BOOLEAN, currencyisocode VARCHAR, createddate TIMESTAMP, direct_customer_duns__c VARCHAR, end_user__c VARCHAR, ge_es_opportunity_number__c VARCHAR, gew_sic_code__c VARCHAR, ge_og_install_country__c VARCHAR, ge_og_pii_state__c VARCHAR, ge_pw_sub_industry__c VARCHAR, ge_pw_tml_r1_stus__c VARCHAR, ge_region__c VARCHAR, ge_w_cms_opportunity_status__c VARCHAR, opportunity_classification__c VARCHAR, sw_opportunity_name__c VARCHAR, opportunity_industry__c VARCHAR, es_record_type_f__c VARCHAR, ai_po_status__c VARCHAR, account_name_opportunity_owner__c VARCHAR, wtz_established_market__c VARCHAR, wtz_region__c VARCHAR, wtz_business_line__c VARCHAR, wt_synergy_type__c VARCHAR, wt_opportunity_amount_weighted_value__c DOUBLE PRECISION, tenant_id VARCHAR); TRUNCATE TABLE raw_sfdc.opportunity_vwts;"}, transformation_ctx="opportunity_vwts_node1749083317341")

# Script generated for node opportunity_vwt
opportunity_vwt_node1749083415360 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3VWT_node1747755513736, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-017820666813-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_sfdc.opportunity_vwt", "connectionName": "Redshift wtz-dataplatform-dev-public", "preactions": "CREATE TABLE IF NOT EXISTS raw_sfdc.opportunity_vwt (id VARCHAR, accountid VARCHAR, recordtypeid VARCHAR, name VARCHAR, stagename VARCHAR, probability DOUBLE PRECISION, expectedrevenue DOUBLE PRECISION, closedate DATE, type VARCHAR, leadsource VARCHAR, isclosed BOOLEAN, currencyisocode VARCHAR, ownerid VARCHAR, createddate TIMESTAMP, attribute__c VARCHAR, end_user_account__c VARCHAR, execution_country__c VARCHAR, market__c VARCHAR, opportunity_number__c VARCHAR, account_shipping_country__c VARCHAR, opportunity_value__c DOUBLE PRECISION, sic_code_vwt__c VARCHAR, sub_market_vwt__c VARCHAR, wtz_businessline__c VARCHAR, wtz_region__c VARCHAR, wtz_establishedmarket__c VARCHAR, vwt_lt_synergy_type__c VARCHAR, tenant_id VARCHAR); TRUNCATE TABLE raw_sfdc.opportunity_vwt;"}, transformation_ctx="opportunity_vwt_node1749083415360")

job.commit()