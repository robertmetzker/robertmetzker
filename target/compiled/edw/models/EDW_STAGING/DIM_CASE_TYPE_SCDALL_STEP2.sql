----SRC LAYER----
WITH
SCD1 as ( SELECT 
-- CASE_CATEGORY_CODE, CASE_CATEGORY_DESC, CASE_TYPE_CODE, CASE_TYPE_DESC , UNIQUE_ID_KEY    
CASE_PRIORITY_TYPE_DESC,CASE_PRIORITY_TYPE_CODE,CASE_RESOLUTION_TYPE_CODE,CONTEXT_TYPE_DESC, CASE_CATEGORY_TYPE_DESC,CONTEXT_TYPE_CODE,CASE_TYPE_CODE,CASE_TYPE_DESC, CASE_CATEGORY_TYPE_CODE,CASE_RESOLUTION_TYPE_DESC,UNIQUE_ID_KEY
	from      STAGING.DSV_CASE_TYPE )

select * from SCD1