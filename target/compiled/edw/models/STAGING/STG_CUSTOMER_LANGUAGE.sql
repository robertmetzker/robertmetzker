----SRC LAYER----
WITH
SRC_CL as ( SELECT *     from      DEV_VIEWS.PCMP.CUSTOMER_LANGUAGE ),
SRC_LT as ( SELECT *     from      DEV_VIEWS.PCMP.LANGUAGE_TYPE )
----LOGIC LAYER----
,
LOGIC_CL as ( SELECT 
		CUST_LANG_ID AS CUST_LANG_ID,
		CUST_ID AS CUST_ID,
		UPPER(TRIM(LANG_TYP_CD)) AS LANG_TYP_CD,
		uPPER(CUST_LANG_PRI_IND) AS CUST_LANG_PRI_IND,
		AUDIT_USER_ID_CREA AS AUDIT_USER_ID_CREA,
		AUDIT_USER_CREA_DTM AS AUDIT_USER_CREA_DTM,
		AUDIT_USER_ID_UPDT AS AUDIT_USER_ID_UPDT,
		AUDIT_USER_UPDT_DTM AS AUDIT_USER_UPDT_DTM,
		UPPER(VOID_IND) AS VOID_IND 
				from SRC_CL
            ),
LOGIC_LT as ( SELECT 
		UPPER(TRIM(LANG_TYP_CD)) AS LANG_TYP_CD,
		UPPER(TRIM(LANG_TYP_NM)) AS LANG_TYP_NM,
		UPPER(LANG_TYP_VOID_IND) AS LANG_TYP_VOID_IND 
				from SRC_LT
            )
----RENAME LAYER ----
,
RENAME_CL as ( SELECT CUST_LANG_ID AS CUST_LANG_ID,CUST_ID AS CUST_ID,
			
			LANG_TYP_CD AS CUST_LANG_TYP_CD,CUST_LANG_PRI_IND AS CUST_LANG_PRI_IND,AUDIT_USER_ID_CREA AS AUDIT_USER_ID_CREA,AUDIT_USER_CREA_DTM AS AUDIT_USER_CREA_DTM,AUDIT_USER_ID_UPDT AS AUDIT_USER_ID_UPDT,AUDIT_USER_UPDT_DTM AS AUDIT_USER_UPDT_DTM,VOID_IND AS VOID_IND 
			from      LOGIC_CL
        ),
RENAME_LT as ( SELECT LANG_TYP_CD AS LANG_TYP_CD,LANG_TYP_NM AS LANG_TYP_NM,LANG_TYP_VOID_IND AS LANG_TYP_VOID_IND 
			from      LOGIC_LT
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_CL as ( SELECT  * 
			from     RENAME_CL 
            
        ),

        FILTER_LT as ( SELECT  * 
			from     RENAME_LT 
            
        )
----JOIN LAYER----
,
CL as ( SELECT * 
			from  FILTER_CL
				LEFT JOIN FILTER_LT on FILTER_CL.CUST_LANG_TYP_CD = FILTER_LT.LANG_TYP_CD)
select * from CL