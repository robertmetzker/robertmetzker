----SRC LAYER----
WITH
SRC_APC as ( SELECT *     from      DEV_VIEWS.BASE.APC_INFO ),
SRC_REF as ( SELECT *     from      STAGING.STG_CAM_REF ),
----LOGIC LAYER----
LOGIC_APC as ( SELECT 
		UPPER(trim(APC_CODE)) AS APC_CODE,
		cast(EFFECTIVE_DATE as DATE) AS EFFECTIVE_DATE,
		cast(EXPIRATION_DATE as DATE) AS EXPIRATION_DATE,
		UPPER(trim(DESCRIPTION)) AS DESCRIPTION,
		APC_AMOUNT AS APC_AMOUNT,
		UPPER(trim(STATUS)) AS STATUS,
		UPPER(trim(ENTRY_USER_ID)) AS ENTRY_USER_ID,
		cast(ENTRY_DATE as DATE) AS ENTRY_DATE,
		UPPER(trim(ULM)) AS ULM,
		cast(DLM as DATE) AS DLM,
		UPPER(trim(OFFSET_TYPE)) AS OFFSET_TYPE,
		OFFSET_AMOUNT AS OFFSET_AMOUNT,
		RELATIVE_WEIGHT AS RELATIVE_WEIGHT 
				from SRC_APC
            ),
LOGIC_REF as ( SELECT 
		trim(REF_IDN) AS REF_IDN,
		trim(REF_DGN) AS REF_DGN,
		trim(REF_DSC) AS REF_DSC,
		REF_EXP_DTE AS REF_EXP_DTE 
				from SRC_REF
            )
----RENAME LAYER ----
,
RENAME_APC as ( SELECT APC_CODE AS APC_CODE,EFFECTIVE_DATE AS EFFECTIVE_DATE,EXPIRATION_DATE AS EXPIRATION_DATE,DESCRIPTION AS DESCRIPTION,APC_AMOUNT AS APC_AMOUNT,STATUS AS STATUS,ENTRY_USER_ID AS ENTRY_USER_ID,ENTRY_DATE AS ENTRY_DATE,ULM AS ULM,DLM AS DLM,OFFSET_TYPE AS OFFSET_TYPE,OFFSET_AMOUNT AS OFFSET_AMOUNT,RELATIVE_WEIGHT AS RELATIVE_WEIGHT 
			from      LOGIC_APC
        ),
RENAME_REF as ( SELECT REF_IDN AS REF_IDN,REF_DGN AS REF_DGN,
			
			REF_DSC AS STATUS_DESC,REF_EXP_DTE AS REF_EXP_DTE 
			from      LOGIC_REF
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_APC as ( SELECT  * 
			from     RENAME_APC 
            
        ),

        FILTER_REF as ( SELECT  * 
			from     RENAME_REF 
            WHERE REF_DGN = 'ASC' AND REF_EXP_DTE > CURRENT_DATE
        )
----JOIN LAYER----
,
APC as ( SELECT * 
			from  FILTER_APC
				LEFT JOIN FILTER_REF ON FILTER_APC.STATUS = FILTER_REF.REF_IDN  )
select * from APC