

      create or replace  table DEV_EDW.STAGING.STG_CUSTOMER_ADDRESS  as
      (----SRC LAYER----
WITH
------------------------------------------------------------------------------------------------------------------
-- Adding filter VOID_IND = 'N' in source layer to filter the VOIDED records before the derivation condition below
------------------------------------------------------------------------------------------------------------------
SRC_CA as ( SELECT *     from      DEV_VIEWS.PCMP.CUSTOMER_ADDRESS WHERE UPPER(TRIM(VOID_IND)) = 'N'),
SRC_CAT as ( SELECT *     from      DEV_VIEWS.PCMP.CUSTOMER_ADDRESS_TYPE ),
SRC_ST as ( SELECT *     from      DEV_VIEWS.PCMP.STATE),
SRC_CO as ( SELECT *     from      DEV_VIEWS.PCMP.COUNTRY )
//SRC_CA as ( SELECT *     from      CUSTOMER_ADDRESS),
//SRC_CAT as ( SELECT *     from      CUSTOMER_ADDRESS_TYPE),
//SRC_ST as ( SELECT *     from      STATE),
//SRC_CO as ( SELECT *     from      COUNTRY)
----LOGIC LAYER----
,
LOGIC_CA as ( SELECT 
		CUST_ADDR_ID AS CUST_ADDR_ID,
		CUST_ID AS CUST_ID,
		UPPER(TRIM(CUST_ADDR_TYP_CD)) AS CUST_ADDR_TYP_CD,
		cast(CUST_ADDR_EFF_DT  as DATE) AS CUST_ADDR_EFF_DT ,
		cast(CUST_ADDR_END_DT as DATE) AS CUST_ADDR_END_DT,
		case when date(CUST_ADDR_EFF_DT) = date(CUST_ADDR_END_DT) then date(CUST_ADDR_END_DT)
             else lead(date(CUST_ADDR_EFF_DT)) over (partition by CUST_ID, CUST_ADDR_TYP_CD order by CUST_ADDR_EFF_DT, AUDIT_USER_CREA_DTM) -1 end 
			 as DRVD_CUST_ADDR_END_DATE,
		UPPER(TRIM(CUST_ADDR_STR_1)) AS CUST_ADDR_STR_1,
		UPPER(TRIM(CUST_ADDR_STR_2)) AS CUST_ADDR_STR_2,
		STT_ID AS STT_ID,
		CNTRY_ID AS CNTRY_ID,
		UPPER(TRIM(CUST_ADDR_CITY_NM)) AS CUST_ADDR_CITY_NM,
		UPPER(TRIM(CUST_ADDR_CNTY_NM)) AS CUST_ADDR_CNTY_NM,
		UPPER(TRIM(CUST_ADDR_POST_CD)) AS CUST_ADDR_POST_CD,
		UPPER(TRIM(CUST_ADDR_UPDT_SRC)) AS CUST_ADDR_UPDT_SRC,
		UPPER(TRIM(CUST_ADDR_VLDT_IND)) AS CUST_ADDR_VLDT_IND,
		UPPER(TRIM(CUST_ADDR_VLDT_PRFM_IND)) AS CUST_ADDR_VLDT_PRFM_IND,
		UPPER(TRIM(CUST_ADDR_COMT)) AS CUST_ADDR_COMT,
		UPPER(TRIM(VOID_IND)) AS VOID_IND,
		AUDIT_USER_ID_CREA AS AUDIT_USER_ID_CREA,
		AUDIT_USER_CREA_DTM AS AUDIT_USER_CREA_DTM,
		AUDIT_USER_ID_UPDT AS AUDIT_USER_ID_UPDT,
		AUDIT_USER_UPDT_DTM AS AUDIT_USER_UPDT_DTM,
		MAX (COALESCE(CUST_ADDR_END_DT, '12/31/2099'))
                         OVER (PARTITION BY CUST_ID, CUST_ADDR_TYP_CD ORDER BY CUST_ADDR_EFF_DT, COALESCE(CUST_ADDR_END_DT, '12/31/2099')
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS OVERLAP_FIX_QLFR
			FROM SRC_CA
			WHERE CUST_ADDR_EFF_DT < COALESCE(CUST_ADDR_END_DT, '12/31/2099')
			QUALIFY COALESCE(CUST_ADDR_END_DT, '12/31/2099') = OVERLAP_FIX_QLFR
            ),
LOGIC_CAT as ( SELECT 
		TRIM(CUST_ADDR_TYP_NM) AS CUST_ADDR_TYP_NM,
		UPPER(TRIM(CUST_ADDR_TYP_CD)) AS CUST_ADDR_TYP_CD 
				from SRC_CAT
            ),
LOGIC_ST as ( SELECT 
		UPPER(TRIM(STT_ABRV)) AS STT_ABRV,
		UPPER(TRIM(STT_NM)) AS STT_NM,
		STT_ID AS STT_ID,
		UPPER(TRIM(STT_VOID_IND)) AS STT_VOID_IND 
				from SRC_ST
            ),
LOGIC_CO as ( SELECT 
		UPPER(TRIM(CNTRY_NM)) AS CNTRY_NM,
		CNTRY_ID AS CNTRY_ID,
		UPPER(TRIM(CNTRY_VOID_IND)) AS CNTRY_VOID_IND 
				from SRC_CO
            )
----RENAME LAYER ----
,
RENAME_CA as ( SELECT CUST_ADDR_ID AS CUST_ADDR_ID,CUST_ID AS CUST_ID,CUST_ADDR_TYP_CD AS CUST_ADDR_TYP_CD,
			
			CUST_ADDR_EFF_DT AS CUST_ADDR_EFF_DATE,
			
			CUST_ADDR_END_DT AS CUST_ADDR_END_DATE, DRVD_CUST_ADDR_END_DATE AS DRVD_CUST_ADDR_END_DATE, CUST_ADDR_STR_1 AS CUST_ADDR_STR_1,CUST_ADDR_STR_2 AS CUST_ADDR_STR_2,STT_ID AS STT_ID,CNTRY_ID AS CNTRY_ID,CUST_ADDR_CITY_NM AS CUST_ADDR_CITY_NM,CUST_ADDR_CNTY_NM AS CUST_ADDR_CNTY_NM,CUST_ADDR_POST_CD AS CUST_ADDR_POST_CD,CUST_ADDR_UPDT_SRC AS CUST_ADDR_UPDT_SRC,CUST_ADDR_VLDT_IND AS CUST_ADDR_VLDT_IND,CUST_ADDR_VLDT_PRFM_IND AS CUST_ADDR_VLDT_PRFM_IND,CUST_ADDR_COMT AS CUST_ADDR_COMT,VOID_IND AS VOID_IND,AUDIT_USER_ID_CREA AS AUDIT_USER_ID_CREA,AUDIT_USER_CREA_DTM AS AUDIT_USER_CREA_DTM,AUDIT_USER_ID_UPDT AS AUDIT_USER_ID_UPDT,AUDIT_USER_UPDT_DTM AS AUDIT_USER_UPDT_DTM 
			from      LOGIC_CA
        ),
RENAME_CAT as ( SELECT CUST_ADDR_TYP_NM AS CUST_ADDR_TYP_NM,
			
			CUST_ADDR_TYP_CD AS CAT_CUST_ADDR_TYP_CD 
			from      LOGIC_CAT
        ),
RENAME_ST as ( SELECT STT_ABRV AS STT_ABRV,STT_NM AS STT_NM,
			
			STT_ID AS ST_STT_ID,STT_VOID_IND AS STT_VOID_IND 
			from      LOGIC_ST
        ),
RENAME_CO as ( SELECT CNTRY_NM AS CNTRY_NM,
			
			CNTRY_ID AS CO_CNTRY_ID,CNTRY_VOID_IND AS CNTRY_VOID_IND 
			from      LOGIC_CO
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_CA as ( SELECT  * 
			from     RENAME_CA 
            QUALIFY (ROW_NUMBER() OVER (PARTITION BY CUST_ID,CUST_ADDR_TYP_CD,CUST_ADDR_EFF_DATE ORDER BY  AUDIT_USER_CREA_DTM DESC ))= 1
        ),

        FILTER_CAT as ( SELECT  * 
			from     RENAME_CAT 
            
        ),

        FILTER_ST as ( SELECT  * 
			from     RENAME_ST 
            WHERE STT_VOID_IND = 'N'
        ),

        FILTER_CO as ( SELECT  * 
			from     RENAME_CO 
            WHERE CNTRY_VOID_IND = 'N'
        )
----JOIN LAYER----
,
CA as ( SELECT * 
			from  FILTER_CA
				LEFT JOIN FILTER_CAT ON FILTER_CA.CUST_ADDR_TYP_CD = FILTER_CAT.CAT_CUST_ADDR_TYP_CD 
				LEFT JOIN FILTER_ST ON FILTER_CA.STT_ID = FILTER_ST.ST_STT_ID 
				LEFT JOIN FILTER_CO ON FILTER_CA.CNTRY_ID = FILTER_CO.CO_CNTRY_ID  )

select * from CA
      );
    