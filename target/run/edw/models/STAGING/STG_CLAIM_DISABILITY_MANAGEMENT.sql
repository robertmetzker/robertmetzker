

      create or replace  table DEV_EDW.STAGING.STG_CLAIM_DISABILITY_MANAGEMENT  as
      (---- SRC LAYER ----
WITH
SRC_CDM as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_DISABILITY_MANAGEMENT ),
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM ),
SRC_DT as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_DISAB_MANG_DISAB_TYP ),
SRC_RT as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_DISABILITY_MANG_RSN_TYP ),
SRC_MST as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_DISAB_MANG_MED_STS_TYP ),
SRC_WST as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_DISAB_MANG_WK_STS_TYP ),
//SRC_CDM as ( SELECT *     from     PCMP.CLAIM_DISABILITY_MANAGEMENT) ,
//SRC_C as ( SELECT *     from     PCMP.CLAIM) ,
//SRC_DT as ( SELECT *     from     PCMP.CLAIM_DISAB_MANG_DISAB_TYP) ,
//SRC_RT as ( SELECT *     from     PCMP.CLAIM_DISABILITY_MANG_RSN_TYP) ,
//SRC_MST as ( SELECT *     from     PCMP.CLAIM_DISAB_MANG_MED_STS_TYP) ,
//SRC_WST as ( SELECT *     from     PCMP.CLAIM_DISAB_MANG_WK_STS_TYP) ,

---- LOGIC LAYER ----

LOGIC_CDM as ( SELECT 
		  CLM_DISAB_MANG_ID                                  AS                                  CLM_DISAB_MANG_ID 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, upper( TRIM( CLM_DISAB_MANG_DISAB_TYP_CD ) )       AS                        CLM_DISAB_MANG_DISAB_TYP_CD 
		, upper( TRIM( CLM_DISAB_MANG_RSN_TYP_CD ) )         AS                          CLM_DISAB_MANG_RSN_TYP_CD 
		, upper( TRIM( CLM_DISAB_MANG_MED_STS_TYP_CD ) )     AS                      CLM_DISAB_MANG_MED_STS_TYP_CD 
		, upper( TRIM( CLM_DISAB_MANG_WK_STS_TYP_CD ) )      AS                       CLM_DISAB_MANG_WK_STS_TYP_CD 
		, cast( CLM_DISAB_MANG_EFF_DT as DATE )              AS                              CLM_DISAB_MANG_EFF_DT 
		, cast( CLM_DISAB_MANG_END_DT as DATE )              AS                              CLM_DISAB_MANG_END_DT 
		, CLM_DISAB_ACTL_ELPS_DD                             AS                             CLM_DISAB_ACTL_ELPS_DD 
		, CLM_DISAB_CAL_ELPS_DD                              AS                              CLM_DISAB_CAL_ELPS_DD 
		, upper( TRIM( CLM_DISAB_COMT ) )                    AS                                     CLM_DISAB_COMT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CDM
            ),
LOGIC_C as ( SELECT 
		  upper( TRIM( CLM_NO ) )                            AS                                             CLM_NO 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, upper( CLM_REL_SNPSHT_IND )                        AS                                 CLM_REL_SNPSHT_IND 
		from SRC_C
            ),
LOGIC_DT as ( SELECT 
		  upper( TRIM( CLM_DISAB_MANG_DISAB_TYP_NM ) )       AS                        CLM_DISAB_MANG_DISAB_TYP_NM 
		, upper( TRIM( CLM_DISAB_MANG_DISAB_TYP_CD ) )       AS                        CLM_DISAB_MANG_DISAB_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_DT
            ),
LOGIC_RT as ( SELECT 
		  upper( TRIM( CLM_DISAB_MANG_RSN_TYP_NM ) )         AS                          CLM_DISAB_MANG_RSN_TYP_NM 
		, upper( TRIM( CLM_DISAB_MANG_RSN_TYP_CD ) )         AS                          CLM_DISAB_MANG_RSN_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_RT
            ),
LOGIC_MST as ( SELECT 
		  upper( TRIM( CLM_DISAB_MANG_MED_STS_TYP_NM ) )     AS                      CLM_DISAB_MANG_MED_STS_TYP_NM 
		, upper( TRIM( CLM_DISAB_MANG_MED_STS_TYP_CD ) )     AS                      CLM_DISAB_MANG_MED_STS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_MST
            ),
LOGIC_WST as ( SELECT 
		  upper( TRIM( CLM_DISAB_MANG_WK_STS_TYP_NM ) )      AS                       CLM_DISAB_MANG_WK_STS_TYP_NM 
		, upper( TRIM( CLM_DISAB_MANG_WK_STS_TYP_CD ) )      AS                       CLM_DISAB_MANG_WK_STS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_WST
            )

---- RENAME LAYER ----
,

RENAME_CDM as ( SELECT 
		  CLM_DISAB_MANG_ID                                  as                                  CLM_DISAB_MANG_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_DISAB_MANG_DISAB_TYP_CD                        as                        CLM_DISAB_MANG_DISAB_TYP_CD
		, CLM_DISAB_MANG_RSN_TYP_CD                          as                          CLM_DISAB_MANG_RSN_TYP_CD
		, CLM_DISAB_MANG_MED_STS_TYP_CD                      as                      CLM_DISAB_MANG_MED_STS_TYP_CD
		, CLM_DISAB_MANG_WK_STS_TYP_CD                       as                       CLM_DISAB_MANG_WK_STS_TYP_CD
		, CLM_DISAB_MANG_EFF_DT                              as                              CLM_DISAB_MANG_EFF_DT
		, CLM_DISAB_MANG_END_DT                              as                              CLM_DISAB_MANG_END_DT
		, CLM_DISAB_ACTL_ELPS_DD                             as                             CLM_DISAB_ACTL_ELPS_DD
		, CLM_DISAB_CAL_ELPS_DD                              as                              CLM_DISAB_CAL_ELPS_DD
		, CLM_DISAB_COMT                                     as                                     CLM_DISAB_COMT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CDM   ), 
RENAME_C as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
		, AGRE_ID                                            as                                          C_AGRE_ID
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND 
				FROM     LOGIC_C   ), 
RENAME_DT as ( SELECT 
		  CLM_DISAB_MANG_DISAB_TYP_NM                        as                        CLM_DISAB_MANG_DISAB_TYP_NM
		, CLM_DISAB_MANG_DISAB_TYP_CD                        as                     DT_CLM_DISAB_MANG_DISAB_TYP_CD
		, VOID_IND                                           as                                        DT_VOID_IND 
				FROM     LOGIC_DT   ), 
RENAME_RT as ( SELECT 
		  CLM_DISAB_MANG_RSN_TYP_NM                          as                          CLM_DISAB_MANG_RSN_TYP_NM
		, CLM_DISAB_MANG_RSN_TYP_CD                          as                       RT_CLM_DISAB_MANG_RSN_TYP_CD
		, VOID_IND                                           as                                        RT_VOID_IND 
				FROM     LOGIC_RT   ), 
RENAME_MST as ( SELECT 
		  CLM_DISAB_MANG_MED_STS_TYP_NM                      as                      CLM_DISAB_MANG_MED_STS_TYP_NM
		, CLM_DISAB_MANG_MED_STS_TYP_CD                      as                  MST_CLM_DISAB_MANG_MED_STS_TYP_CD
		, VOID_IND                                           as                                       MST_VOID_IND 
				FROM     LOGIC_MST   ), 
RENAME_WST as ( SELECT 
		  CLM_DISAB_MANG_WK_STS_TYP_NM                       as                       CLM_DISAB_MANG_WK_STS_TYP_NM
		, CLM_DISAB_MANG_WK_STS_TYP_CD                       as                   WST_CLM_DISAB_MANG_WK_STS_TYP_CD
		, VOID_IND                                           as                                       WST_VOID_IND 
				FROM     LOGIC_WST   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CDM                            as ( SELECT * from    RENAME_CDM   ),
FILTER_C                              as ( SELECT * from    RENAME_C 
				WHERE CLM_REL_SNPSHT_IND = 'N'  ),
FILTER_DT                             as ( SELECT * from    RENAME_DT 
				WHERE DT_VOID_IND = 'N'  ),
FILTER_RT                             as ( SELECT * from    RENAME_RT 
				WHERE RT_VOID_IND = 'N'  ),
FILTER_MST                            as ( SELECT * from    RENAME_MST 
				WHERE MST_VOID_IND = 'N'  ),
FILTER_WST                            as ( SELECT * from    RENAME_WST 
				WHERE WST_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CDM as ( SELECT * 
				FROM  FILTER_CDM
				INNER JOIN FILTER_C ON  FILTER_CDM.AGRE_ID =  FILTER_C.C_AGRE_ID 
						LEFT JOIN FILTER_DT ON  FILTER_CDM.CLM_DISAB_MANG_DISAB_TYP_CD =  FILTER_DT.DT_CLM_DISAB_MANG_DISAB_TYP_CD 
						LEFT JOIN FILTER_RT ON  FILTER_CDM.CLM_DISAB_MANG_RSN_TYP_CD =  FILTER_RT.RT_CLM_DISAB_MANG_RSN_TYP_CD 
						LEFT JOIN FILTER_MST ON  FILTER_CDM.CLM_DISAB_MANG_MED_STS_TYP_CD =  FILTER_MST.MST_CLM_DISAB_MANG_MED_STS_TYP_CD 
						LEFT JOIN FILTER_WST ON  FILTER_CDM.CLM_DISAB_MANG_WK_STS_TYP_CD =  FILTER_WST.WST_CLM_DISAB_MANG_WK_STS_TYP_CD  )
SELECT * 
from CDM
      );
    