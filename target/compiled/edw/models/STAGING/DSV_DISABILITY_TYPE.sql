

---- SRC LAYER ----
WITH
SRC_DIS as ( SELECT *     from     STAGING.DST_DISABILITY_TYPE ),
//SRC_DIS as ( SELECT *     from     DST_DISABILITY_TYPE) ,

---- LOGIC LAYER ----

LOGIC_DIS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, CLM_DISAB_MANG_DISAB_TYP_CD                        as                        CLM_DISAB_MANG_DISAB_TYP_CD 
		, CLM_DISAB_MANG_RSN_TYP_CD                          as                          CLM_DISAB_MANG_RSN_TYP_CD 
		, CLM_DISAB_MANG_MED_STS_TYP_CD                      as                      CLM_DISAB_MANG_MED_STS_TYP_CD 
		, CLM_DISAB_MANG_WK_STS_TYP_CD                       as                       CLM_DISAB_MANG_WK_STS_TYP_CD 
		, CURRENT_DISABILITY_STATUS_IND                      as                      CURRENT_DISABILITY_STATUS_IND
		, CLM_DISAB_MANG_DISAB_TYP_NM                        as                        CLM_DISAB_MANG_DISAB_TYP_NM 
		, CLM_DISAB_MANG_RSN_TYP_NM                          as                          CLM_DISAB_MANG_RSN_TYP_NM 
		, CLM_DISAB_MANG_MED_STS_TYP_NM                      as                      CLM_DISAB_MANG_MED_STS_TYP_NM 
		, CLM_DISAB_MANG_WK_STS_TYP_NM                       as                       CLM_DISAB_MANG_WK_STS_TYP_NM 
		from SRC_DIS
            )

---- RENAME LAYER ----
,

RENAME_DIS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CLM_DISAB_MANG_DISAB_TYP_CD                        as                               DISABILITY_TYPE_CODE
		, CLM_DISAB_MANG_RSN_TYP_CD                          as                        DISABILITY_REASON_TYPE_CODE
		, CLM_DISAB_MANG_MED_STS_TYP_CD                      as                DISABILITY_MEDICAL_STATUS_TYPE_CODE
		, CLM_DISAB_MANG_WK_STS_TYP_CD                       as                   DISABILITY_WORK_STATUS_TYPE_CODE
		, CURRENT_DISABILITY_STATUS_IND                      as                      CURRENT_DISABILITY_STATUS_IND
		, CLM_DISAB_MANG_DISAB_TYP_NM                        as                               DISABILITY_TYPE_NAME
		, CLM_DISAB_MANG_RSN_TYP_NM                          as             DISABILITY_MANAGEMENT_REASON_TYPE_NAME
		, CLM_DISAB_MANG_MED_STS_TYP_NM                      as                DISABILITY_MEDICAL_STATUS_TYPE_NAME
		, CLM_DISAB_MANG_WK_STS_TYP_NM                       as                   DISABILITY_WORK_STATUS_TYPE_NAME 
				FROM     LOGIC_DIS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DIS                            as ( SELECT * from    RENAME_DIS   ),

---- JOIN LAYER ----

 JOIN_DIS  as  ( SELECT * 
				FROM  FILTER_DIS )
 SELECT * FROM  JOIN_DIS