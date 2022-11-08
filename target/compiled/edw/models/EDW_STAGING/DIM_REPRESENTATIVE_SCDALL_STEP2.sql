----SRC LAYER----
WITH
SCD1 as ( SELECT 
"1099_RECEIVER_IND", 
CUSTOMER_TYPE_CODE, 
CUSTOMER_TYPE_DESC, 
TAX_ID_UNAVAILABLE_IND, 
CUSTOMER_NUMBER, 
REPRESENTATIVE_ID_END_DATE, 
CUST_INTRN_CHNL_PHN_NO as PRIMARY_BUSINESS_PHONE_NUMBER, 
TAX_ID_OVERRIDE_IND, 
REPRESENTATIVE_ID_NUMBER, 
FOREIGN_CITIZEN_IND, 
REPRESENTATIVE_ID_EFFECTIVE_DATE, 
TAX_EXEMPT_IND, 
W9_RECVEIVER_IND , 
UNIQUE_ID_KEY    
	--, '1901-01-01' as DBT_VALID_FROM, '2099-12-31' as DBT_VALID_TO
	from      STAGING.DSV_REPRESENTATIVE ),
SCD2 as ( SELECT *    
	from      EDW_STAGING_SNAPSHOT.DIM_REPRESENTATIVE_SNAPSHOT_STEP1 ),
FINAL as ( SELECT * 
            from  SCD2 
                INNER JOIN SCD1 USING( UNIQUE_ID_KEY )  )
select * from FINAL