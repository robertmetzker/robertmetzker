---- SRC LAYER ----
WITH
SRC_COD            as ( SELECT *     FROM     DEV_VIEWS.PCMP.CLAIM_OTHER_DATE ),
SRC_CDT            as ( SELECT *     FROM     DEV_VIEWS.PCMP.CLAIM_DATE_TYPE ),
//SRC_COD            as ( SELECT *     FROM     CLAIM_OTHER_DATE) ,
//SRC_CDT            as ( SELECT *     FROM     CLAIM_DATE_TYPE) ,

---- LOGIC LAYER ----


LOGIC_COD as ( SELECT 
		  CLM_OTHR_DT_ID                                     as                                     CLM_OTHR_DT_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		, upper( TRIM( CLM_DT_TYP_CD ) )                     as                                      CLM_DT_TYP_CD 
		, cast( CLM_OTHR_DT_EFF_DT as DATE )                 as                                 CLM_OTHR_DT_EFF_DT 
		, cast( CLM_OTHR_DT_END_DT as DATE )                 as                                 CLM_OTHR_DT_END_DT 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		FROM SRC_COD
            ),

LOGIC_CDT as ( SELECT 
		  upper( TRIM( CLM_DT_TYP_NM ) )                     as                                      CLM_DT_TYP_NM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		, upper( CLM_DT_TYP_CD )                             as                                      CLM_DT_TYP_CD 
		FROM SRC_CDT
            )

---- RENAME LAYER ----
,

RENAME_COD        as ( SELECT 
		  CLM_OTHR_DT_ID                                     as                                     CLM_OTHR_DT_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_DT_TYP_CD                                      as                                      CLM_DT_TYP_CD
		, CLM_OTHR_DT_EFF_DT                                 as                                 CLM_OTHR_DT_EFF_DT
		, CLM_OTHR_DT_END_DT                                 as                                 CLM_OTHR_DT_END_DT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_COD   ), 
RENAME_CDT        as ( SELECT 
		  CLM_DT_TYP_NM                                      as                                      CLM_DT_TYP_NM
		, VOID_IND                                           as                                       CDT_VOID_IND
		, CLM_DT_TYP_CD                                      as                                  CDT_CLM_DT_TYP_CD 
				FROM     LOGIC_CDT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_COD                            as ( SELECT * FROM    RENAME_COD   ),
FILTER_CDT                            as ( SELECT * FROM    RENAME_CDT   ),

---- JOIN LAYER ----

COD as ( SELECT * 
				FROM  FILTER_COD
				LEFT JOIN  FILTER_CDT ON  FILTER_COD.CLM_DT_TYP_CD =  FILTER_CDT.CDT_CLM_DT_TYP_CD  )
SELECT 
		  CLM_OTHR_DT_ID
		, AGRE_ID
		, CLM_DT_TYP_CD
		, CLM_DT_TYP_NM
		, CLM_OTHR_DT_EFF_DT
		, CLM_OTHR_DT_END_DT
		, AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM
		, VOID_IND 
FROM COD