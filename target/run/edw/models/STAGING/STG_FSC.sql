

      create or replace  table DEV_EDW.STAGING.STG_FSC  as
      (----SRC LAYER----
WITH
SRC_FB as ( SELECT *     from      DEV_VIEWS.BASE.FSC_BASE ),
SRC_FG as ( SELECT *     from      DEV_VIEWS.BASE.FSC_GENERAL ),
SRC_MOD as ( SELECT *     from      DEV_VIEWS.BASE.REF  ),
SRC_TOS as ( SELECT *     from      DEV_VIEWS.BASE.REF  ),
SRC_FN as ( SELECT *     from      DEV_VIEWS.BASE.FSC_NDC  ),

----LOGIC LAYER----

LOGIC_FB as ( SELECT 
		FSC_RID AS FSC_RID,
		UPPER(TRIM(SERVICE_CODE)) AS SERVICE_CODE,
		UPPER(TRIM(SERVICE_MOD)) AS SERVICE_MOD 
				from SRC_FB
            ),
LOGIC_FG as ( SELECT 
		UPPER(TRIM(DESCRIPTION)) AS DESCRIPTION,
		UPPER(TRIM(LONG_DESC)) AS LONG_DESC,
		UPPER(TRIM(MCO_VALID)) AS MCO_VALID,
		UPPER(TRIM(TYPE_OF_SVC)) AS TYPE_OF_SVC,
		cast(EFFECTIVE_DATE as DATE) AS EFFECTIVE_DATE,
		NULLIF (cast(EXPIRATION_DATE as DATE),'2099-12-31'::DATE) AS EXPIRATION_DATE,
		UPPER(TRIM(ENTRY_USER_ID)) AS ENTRY_USER_ID,
		ENTRY_DATE AS ENTRY_DATE,
		UPPER(TRIM(ULM)) AS ULM,
		DLM AS DLM,
		FSC_RID AS FSC_RID 
				from SRC_FG
            ),
LOGIC_MOD as ( SELECT 
		UPPER(TRIM(REF_DSC)) AS REF_DSC,
		TRIM(REF_IDN) AS REF_IDN,
		TRIM(REF_DGN) AS REF_DGN,
		cast(REF_EXP_DTE as DATE) AS REF_EXP_DTE 
				from SRC_MOD
            ),
LOGIC_TOS as ( SELECT 
		UPPER(TRIM(REF_DSC)) AS REF_DSC,
		TRIM(REF_IDN) AS REF_IDN,
		TRIM(REF_DGN) AS REF_DGN,
		cast(REF_EXP_DTE as DATE) AS REF_EXP_DTE 
				from SRC_TOS
            ),
LOGIC_FN as ( SELECT 
		UPPER(TRIM(DRUG_STRENGTH)) AS DRUG_STRENGTH,
		TRIM(GPI) AS GPI,
		FSC_RID AS FSC_RID,
		cast(EXPIRATION_DATE as DATE) AS EXPIRATION_DATE 
				from SRC_FN
            )
----RENAME LAYER ----
,
RENAME_FB as ( SELECT FSC_RID AS FSC_RID,SERVICE_CODE AS SERVICE_CODE,SERVICE_MOD AS SERVICE_MOD 
			from      LOGIC_FB
        ),
RENAME_FG as ( SELECT 
			
			DESCRIPTION AS SERVICE_DESC,
			
			LONG_DESC AS SERVICE_LONG_DESC,MCO_VALID AS MCO_VALID,
			
			TYPE_OF_SVC AS TYPE_OF_SVC_CODE,EFFECTIVE_DATE AS EFFECTIVE_DATE,EXPIRATION_DATE AS EXPIRATION_DATE,ENTRY_USER_ID AS ENTRY_USER_ID,ENTRY_DATE AS ENTRY_DATE,ULM AS ULM,DLM AS DLM,
			
			FSC_RID AS FG_FSC_RID 
			from      LOGIC_FG
        ),
RENAME_MOD as ( SELECT 
			
			REF_DSC AS MOD_DESC,
			
			REF_IDN AS MOD_REF_IDN,
			
			REF_DGN AS MOD_REF_DGN,
			
			REF_EXP_DTE AS MOD_REF_EXP_DTE 
			from      LOGIC_MOD
        ),
RENAME_TOS as ( SELECT 
			
			REF_DSC AS TYPE_OF_SVC_DESC,
			
			REF_IDN AS TOS_REF_IDN,
			
			REF_DGN AS TOS_REF_DGN,
			
			REF_EXP_DTE AS TOS_REF_EXP_DTE 
			from      LOGIC_TOS
        ),
RENAME_FN as ( SELECT DRUG_STRENGTH AS DRUG_STRENGTH,GPI AS GPI,
			
			FSC_RID AS FN_FSC_RID,
			
			EXPIRATION_DATE AS FN_EXPIRATION_DATE 
			from      LOGIC_FN
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_FB as ( SELECT  * 
			from     RENAME_FB 
            
        ),

        FILTER_FG as ( SELECT  * 
			from     RENAME_FG 
            
        ),

        FILTER_TOS as ( SELECT  * 
			from     RENAME_TOS 
            WHERE TOS_REF_DGN = 'TOS' and TOS_REF_EXP_DTE >= current_date
        ),

        FILTER_FN as ( SELECT  * 
			from     RENAME_FN 
            WHERE FN_EXPIRATION_DATE >= current_date
        ),

        FILTER_MOD as ( SELECT  * 
			from     RENAME_MOD 
            WHERE MOD_REF_DGN = 'MOD' and MOD_REF_EXP_DTE >= current_date
        )
----JOIN LAYER----
,
FG as ( SELECT * 
			from  FILTER_FG
				LEFT JOIN FILTER_TOS ON FILTER_FG.TYPE_OF_SVC_CODE = FILTER_TOS.TOS_REF_IDN  ),
FB as ( SELECT * 
			from  FILTER_FB
				INNER JOIN FG ON FILTER_FB.FSC_RID = FG.FG_FSC_RID 
				LEFT JOIN FILTER_FN ON FILTER_FB.FSC_RID = FILTER_FN.FN_FSC_RID 
				LEFT JOIN FILTER_MOD ON FILTER_FB.SERVICE_MOD = FILTER_MOD.MOD_REF_IDN  )
select * from FB
      );
    