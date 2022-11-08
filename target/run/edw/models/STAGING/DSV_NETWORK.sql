
  create or replace  view DEV_EDW.STAGING.DSV_NETWORK  as (
     
 
 
----SRC LAYER----
WITH
SRC_NTWK as ( SELECT *     from      STAGING.DST_NETWORK )
//SRC_NTWK as ( SELECT *     from      DST_NETWORK)
----LOGIC LAYER----
,
LOGIC_NTWK as ( SELECT 
		UNIQUE_ID_KEY AS UNIQUE_ID_KEY,
		CUST_NO AS CUST_NO,
		NTWK_NUMBER AS NTWK_NUMBER,
		NTWK_NAME AS NTWK_NAME,
		STATUS AS STATUS,
		NTWK_STATUS_DSC AS NTWK_STATUS_DSC,
		START_DATE AS START_DATE,
		END_DATE AS END_DATE,
		REF_DSC AS REF_DSC,
		NTWK_TYPE AS NTWK_TYPE,
		MCO_ACQUISITION_PATH AS MCO_ACQUISITION_PATH,
		ACQUIRING_NTWK_NAME AS ACQUIRING_NTWK_NAME,
		ACQUIRING_NTWK_NUMBER AS ACQUIRING_NTWK_NUMBER,
		COB_DATE AS COB_DATE,
		NTWK_FINANCIAL_STREET_ADDRESS_1 AS NTWK_FINANCIAL_STREET_ADDRESS_1,
		NTWK_FINANCIAL_STREET_ADDRESS_2 AS NTWK_FINANCIAL_STREET_ADDRESS_2,
		NTWK_FINANCIAL_ADDRESS_CITY_NAME AS NTWK_FINANCIAL_ADDRESS_CITY_NAME,
		NTWK_FINANCIAL_ADDRESS_STATE_CODE AS NTWK_FINANCIAL_ADDRESS_STATE_CODE,
		NTWK_FINANCIAL_ADDRESS_ZIP_CODE AS NTWK_FINANCIAL_ADDRESS_ZIP_CODE,
		NTWK_FINANCIAL_ADDRESS_ZIP4_CODE AS NTWK_FINANCIAL_ADDRESS_ZIP4_CODE,
		NTWK_PHYSICAL_STREET_ADDRESS_1 AS NTWK_PHYSICAL_STREET_ADDRESS_1,
		NTWK_PHYSICAL_STREET_ADDRESS_2 AS NTWK_PHYSICAL_STREET_ADDRESS_2,
		NTWK_PHYSICAL_ADDRESS_CITY_NAME AS NTWK_PHYSICAL_ADDRESS_CITY_NAME,
		NTWK_PHYSICAL_ADDRESS_STATE_CODE AS NTWK_PHYSICAL_ADDRESS_STATE_CODE,
		NTWK_PHYSICAL_ADDRESS_ZIP_CODE AS NTWK_PHYSICAL_ADDRESS_ZIP_CODE,
		NTWK_PHYSICAL_ADDRESS_ZIP4_CODE AS NTWK_PHYSICAL_ADDRESS_ZIP4_CODE,
		NTWK_MAILING_STREET_ADDRESS_1 AS NTWK_MAILING_STREET_ADDRESS_1,
		NTWK_MAILING_STREET_ADDRESS_2 AS NTWK_MAILING_STREET_ADDRESS_2,
		NTWK_MAILING_ADDRESS_CITY_NAME AS NTWK_MAILING_ADDRESS_CITY_NAME,
		NTWK_MAILING_ADDRESS_STATE_CODE AS NTWK_MAILING_ADDRESS_STATE_CODE,
		NTWK_MAILING_ADDRESS_ZIP_CODE AS NTWK_MAILING_ADDRESS_ZIP_CODE,
		NTWK_MAILING_ADDRESS_ZIP4_CODE AS NTWK_MAILING_ADDRESS_ZIP4_CODE,
		NTWK_CUSTOMER_SERVICE_FULL_PHONE_NUMBER AS NTWK_CUSTOMER_SERVICE_FULL_PHONE_NUMBER,
		MERGED_MCO AS MERGED_MCO 
				from SRC_NTWK
            )
----RENAME LAYER ----
,
RENAME_NTWK as ( SELECT UNIQUE_ID_KEY AS UNIQUE_ID_KEY,
			
			CUST_NO AS CORESUITE_CUSTOMER_NUMBER,
			
			NTWK_NUMBER AS NETWORK_NUMBER,
			
			NTWK_NAME AS NETWORK_NAME,
			
			STATUS AS NETWORK_CERTIFICATION_STATUS_CODE,
			
			NTWK_STATUS_DSC AS NETWORK_CERTIFICATION_STATUS_DESC,
			
			START_DATE AS NETWORK_CERTIFICATION_EFFECTIVE_DATE,
			
			END_DATE AS NETWORK_CERTIFICATION_END_DATE,
			
			REF_DSC AS NETWORK_TYPE_DESC,
			
			NTWK_TYPE AS NETWORK_TYPE_CODE,MCO_ACQUISITION_PATH AS MCO_ACQUISITION_PATH,
			
			ACQUIRING_NTWK_NAME AS CURRENT_ACQUIRING_MCO_NAME,
			
			ACQUIRING_NTWK_NUMBER AS CURRENT_ACQUIRING_MCO_NUMBER,
			
			COB_DATE AS MCO_CLOSE_BUSINESS_DATE,
			
			NTWK_FINANCIAL_STREET_ADDRESS_1 AS NETWORK_FINANCIAL_STREET_ADDRESS_1,
			
			NTWK_FINANCIAL_STREET_ADDRESS_2 AS NETWORK_FINANCIAL_STREET_ADDRESS_2,
			
			NTWK_FINANCIAL_ADDRESS_CITY_NAME AS NETWORK_FINANCIAL_ADDRESS_CITY_NAME,
			
			NTWK_FINANCIAL_ADDRESS_STATE_CODE AS NETWORK_FINANCIAL_ADDRESS_STATE_CODE,
			
			NTWK_FINANCIAL_ADDRESS_ZIP_CODE AS NETWORK_FINANCIAL_ADDRESS_ZIP_CODE,
			
			NTWK_FINANCIAL_ADDRESS_ZIP4_CODE AS NETWORK_FINANCIAL_ADDRESS_ZIP4_CODE,
			
			NTWK_PHYSICAL_STREET_ADDRESS_1 AS NETWORK_PHYSICAL_STREET_ADDRESS_1,
			
			NTWK_PHYSICAL_STREET_ADDRESS_2 AS NETWORK_PHYSICAL_STREET_ADDRESS_2,
			
			NTWK_PHYSICAL_ADDRESS_CITY_NAME AS NETWORK_PHYSICAL_ADDRESS_CITY_NAME,
			
			NTWK_PHYSICAL_ADDRESS_STATE_CODE AS NETWORK_PHYSICAL_ADDRESS_STATE_CODE,
			
			NTWK_PHYSICAL_ADDRESS_ZIP_CODE AS NETWORK_PHYSICAL_ADDRESS_ZIP_CODE,
			
			NTWK_PHYSICAL_ADDRESS_ZIP4_CODE AS NETWORK_PHYSICAL_ADDRESS_ZIP4_CODE,
			
			NTWK_MAILING_STREET_ADDRESS_1 AS NETWORK_MAILING_STREET_ADDRESS_1,
			
			NTWK_MAILING_STREET_ADDRESS_2 AS NETWORK_MAILING_STREET_ADDRESS_2,
			
			NTWK_MAILING_ADDRESS_CITY_NAME AS NETWORK_MAILING_ADDRESS_CITY_NAME,
			
			NTWK_MAILING_ADDRESS_STATE_CODE AS NETWORK_MAILING_ADDRESS_STATE_CODE,
			
			NTWK_MAILING_ADDRESS_ZIP_CODE AS NETWORK_MAILING_ADDRESS_ZIP_CODE,
			
			NTWK_MAILING_ADDRESS_ZIP4_CODE AS NETWORK_MAILING_ADDRESS_ZIP4_CODE,
			
			NTWK_CUSTOMER_SERVICE_FULL_PHONE_NUMBER AS NETWORK_CUSTOMER_SERVICE_FULL_PHONE_NUMBER,
			
			MERGED_MCO AS MERGED_NETWORK_NUMBER 
			from      LOGIC_NTWK
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_NTWK as ( SELECT  * 
			from     RENAME_NTWK 
            
        )
----JOIN LAYER----
,
 JOIN_NTWK as ( SELECT * 
			from  FILTER_NTWK )
 SELECT * FROM JOIN_NTWK
  );
