----SRC LAYER----
WITH
SRC_CN as ( SELECT *     from      DEV_VIEWS.PCMP.CUSTOMER_NAME ),
SRC_CNT as ( SELECT *     from      DEV_VIEWS.PCMP.CUSTOMER_NAME_TYPE ),
SRC_CNTT as ( SELECT *     from      DEV_VIEWS.PCMP.CUSTOMER_NAME_TITLE_TYPE ),
SRC_CNST as ( SELECT *     from      DEV_VIEWS.PCMP.CUSTOMER_NAME_SUFFIX_TYPE )
//SRC_CN as ( SELECT *     from      CUSTOMER_NAME),
//SRC_CNT as ( SELECT *     from      CUSTOMER_NAME_TYPE),
//SRC_CNTT as ( SELECT *     from      CUSTOMER_NAME_TITLE_TYPE),
//SRC_CNST as ( SELECT *     from      CUSTOMER_NAME_SUFFIX_TYPE)
----LOGIC LAYER----
,
LOGIC_CN as ( SELECT 
		CUST_NM_ID AS CUST_NM_ID,
		CUST_ID AS CUST_ID,
		UPPER(TRIM(CUST_NM_TYP_CD)) AS CUST_NM_TYP_CD,
		UPPER(TRIM(CUST_NM_NM)) AS CUST_NM_NM,
		UPPER(TRIM(CUST_NM_DRV_UPCS_NM)) AS CUST_NM_DRV_UPCS_NM,
		UPPER(TRIM(CUST_NM_SRCH_NRMLIZED_NM)) AS CUST_NM_SRCH_NRMLIZED_NM,
		UPPER(TRIM(CUST_NM_TTL_TYP_CD)) AS CUST_NM_TTL_TYP_CD,
		UPPER(TRIM(CUST_NM_FST)) AS CUST_NM_FST,
		UPPER(CUST_NM_MID) AS CUST_NM_MID,
		UPPER(TRIM(CUST_NM_LST)) AS CUST_NM_LST,
		UPPER(TRIM(CUST_NM_SFX_TYP_CD)) AS CUST_NM_SFX_TYP_CD,
		UPPER(CUST_NM_TAX_VRF_IND) AS CUST_NM_TAX_VRF_IND,
		cast(CUST_NM_EFF_DT as DATE) AS CUST_NM_EFF_DT,
		cast(CUST_NM_END_DT as DATE) AS CUST_NM_END_DT,
		AUDIT_USER_ID_CREA AS AUDIT_USER_ID_CREA,
		AUDIT_USER_CREA_DTM AS AUDIT_USER_CREA_DTM,
		AUDIT_USER_ID_UPDT AS AUDIT_USER_ID_UPDT,
		AUDIT_USER_UPDT_DTM AS AUDIT_USER_UPDT_DTM,
		UPPER(VOID_IND) AS VOID_IND 
				from SRC_CN
            ),
LOGIC_CNT as ( SELECT 
		UPPER(TRIM(CUST_NM_TYP_NM)) AS CUST_NM_TYP_NM,
		UPPER(TRIM(CUST_NM_TYP_CD)) AS CUST_NM_TYP_CD,
		UPPER(CUST_NM_TYP_VOID_IND) AS CUST_NM_TYP_VOID_IND 
				from SRC_CNT
            ),
LOGIC_CNTT as ( SELECT 
		UPPER(TRIM(CUST_NM_TTL_TYP_NM)) AS CUST_NM_TTL_TYP_NM,
		UPPER(TRIM(CUST_NM_TTL_TYP_CD)) AS CUST_NM_TTL_TYP_CD,
		UPPER(CUST_NM_TTL_TYP_VOID_IND) AS CUST_NM_TTL_TYP_VOID_IND 
				from SRC_CNTT
            ),
LOGIC_CNST as ( SELECT 
		UPPER(TRIM(CUST_NM_SFX_TYP_NM)) AS CUST_NM_SFX_TYP_NM,
		UPPER(TRIM(CUST_NM_SFX_TYP_CD)) AS CUST_NM_SFX_TYP_CD,
		UPPER(CUST_NM_SFX_TYP_VOID_IND) AS CUST_NM_SFX_TYP_VOID_IND 
				from SRC_CNST
            )
----RENAME LAYER ----
,
RENAME_CN as ( SELECT CUST_NM_ID AS CUST_NM_ID,CUST_ID AS CUST_ID,CUST_NM_TYP_CD AS CUST_NM_TYP_CD,CUST_NM_NM AS CUST_NM_NM,CUST_NM_DRV_UPCS_NM AS CUST_NM_DRV_UPCS_NM,CUST_NM_SRCH_NRMLIZED_NM AS CUST_NM_SRCH_NRMLIZED_NM,CUST_NM_TTL_TYP_CD AS CUST_NM_TTL_TYP_CD,CUST_NM_FST AS CUST_NM_FST,CUST_NM_MID AS CUST_NM_MID,CUST_NM_LST AS CUST_NM_LST,CUST_NM_SFX_TYP_CD AS CUST_NM_SFX_TYP_CD,CUST_NM_TAX_VRF_IND AS CUST_NM_TAX_VRF_IND,CUST_NM_EFF_DT AS CUST_NM_EFF_DT,CUST_NM_END_DT AS CUST_NM_END_DT,AUDIT_USER_ID_CREA AS AUDIT_USER_ID_CREA,AUDIT_USER_CREA_DTM AS AUDIT_USER_CREA_DTM,AUDIT_USER_ID_UPDT AS AUDIT_USER_ID_UPDT,AUDIT_USER_UPDT_DTM AS AUDIT_USER_UPDT_DTM,VOID_IND AS VOID_IND 
			from      LOGIC_CN
        ),
RENAME_CNT as ( SELECT CUST_NM_TYP_NM AS CUST_NM_TYP_NM,
			
			CUST_NM_TYP_CD AS CNT_CUST_NM_TYP_CD,CUST_NM_TYP_VOID_IND AS CUST_NM_TYP_VOID_IND 
			from      LOGIC_CNT
        ),
RENAME_CNTT as ( SELECT CUST_NM_TTL_TYP_NM AS CUST_NM_TTL_TYP_NM,
			
			CUST_NM_TTL_TYP_CD AS CNTT_CUST_NM_TTL_TYP_CD,CUST_NM_TTL_TYP_VOID_IND AS CUST_NM_TTL_TYP_VOID_IND 
			from      LOGIC_CNTT
        ),
RENAME_CNST as ( SELECT CUST_NM_SFX_TYP_NM AS CUST_NM_SFX_TYP_NM,
			
			CUST_NM_SFX_TYP_CD AS CNST_CUST_NM_SFX_TYP_CD,CUST_NM_SFX_TYP_VOID_IND AS CUST_NM_SFX_TYP_VOID_IND 
			from      LOGIC_CNST
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_CN as ( SELECT  * 
			from     RENAME_CN 
            
        ),

        FILTER_CNT as ( SELECT  * 
			from     RENAME_CNT 
            WHERE CUST_NM_TYP_VOID_IND = 'N'
        ),

        FILTER_CNTT as ( SELECT  * 
			from     RENAME_CNTT 
            WHERE CUST_NM_TTL_TYP_VOID_IND = 'N'
        ),

        FILTER_CNST as ( SELECT  * 
			from     RENAME_CNST 
            WHERE CUST_NM_SFX_TYP_VOID_IND = 'N'
        )
----JOIN LAYER----
,
CN as ( SELECT * 
			from  FILTER_CN
				LEFT JOIN FILTER_CNT ON FILTER_CN.CUST_NM_TYP_CD = FILTER_CNT.CNT_CUST_NM_TYP_CD 
				LEFT JOIN FILTER_CNTT ON FILTER_CN.CUST_NM_TTL_TYP_CD = FILTER_CNTT.CNTT_CUST_NM_TTL_TYP_CD 
				LEFT JOIN FILTER_CNST ON FILTER_CN.CUST_NM_SFX_TYP_CD = FILTER_CNST.CNST_CUST_NM_SFX_TYP_CD  )
select 
   CUST_NM_ID
  ,CUST_ID
  ,CUST_NM_TYP_CD
  ,CUST_NM_TYP_NM
  ,CUST_NM_NM
  ,CUST_NM_DRV_UPCS_NM
  ,CUST_NM_SRCH_NRMLIZED_NM
  ,CUST_NM_TTL_TYP_CD
  ,CUST_NM_TTL_TYP_NM
  ,CUST_NM_FST
  ,CUST_NM_MID
  ,CUST_NM_LST
  ,CUST_NM_SFX_TYP_CD
  ,CUST_NM_SFX_TYP_NM
  ,CUST_NM_TAX_VRF_IND
  ,CUST_NM_EFF_DT
  ,CASE WHEN COUNT(CUST_ID) OVER (PARTITION BY CUST_ID,CUST_NM_TYP_CD) > 1 AND CUST_NM_TYP_CD = 'PRI_DBA_NM'
           THEN LEAD(CUST_NM_EFF_DT) OVER (PARTITION BY CUST_ID,CUST_NM_TYP_CD ORDER BY CUST_NM_EFF_DT,CUST_NM_END_DT) -1 
        WHEN LEAD(CUST_NM_EFF_DT) OVER (PARTITION BY CUST_ID,CUST_NM_TYP_CD ORDER BY CUST_NM_EFF_DT,CUST_NM_END_DT) != (CUST_NM_END_DT-1) AND CUST_NM_TYP_CD = 'PRI_DBA_NM'
           THEN LEAD(CUST_NM_EFF_DT) OVER (PARTITION BY CUST_ID,CUST_NM_TYP_CD,CUST_NM_END_DT ORDER BY CUST_NM_EFF_DT,CUST_NM_END_DT) -1
        ELSE CUST_NM_END_DT END AS CUST_NM_END_DT
  ,AUDIT_USER_ID_CREA
  ,AUDIT_USER_CREA_DTM
  ,AUDIT_USER_ID_UPDT
  ,AUDIT_USER_UPDT_DTM
  ,VOID_IND
  ,CNT_CUST_NM_TYP_CD
  ,CUST_NM_TYP_VOID_IND
  ,CNTT_CUST_NM_TTL_TYP_CD
  ,CUST_NM_TTL_TYP_VOID_IND
  ,CNST_CUST_NM_SFX_TYP_CD
  ,CUST_NM_SFX_TYP_VOID_IND

 from CN