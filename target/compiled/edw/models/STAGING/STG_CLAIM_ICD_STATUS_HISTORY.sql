---- SRC LAYER ----
WITH
SRC_CISH as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_ICD_STATUS_HISTORY ),
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM ),
SRC_ICD as ( SELECT *     from     DEV_VIEWS.PCMP.ICD ),
SRC_STS as ( SELECT *     from     DEV_VIEWS.PCMP.ICD_STATUS_TYPE ),
SRC_BCISH as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_CLAIM_ICD_STATUS_HISTORY ),
SRC_LOC as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_ICD_LOCATION_TYPE ),
SRC_SIT as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_ICD_SITE_TYPE ),
//SRC_CISH as ( SELECT *     from     CLAIM_ICD_STATUS_HISTORY) ,
//SRC_C as ( SELECT *     from     CLAIM) ,
//SRC_ICD as ( SELECT *     from     ICD) ,
//SRC_STS as ( SELECT *     from     ICD_STATUS_TYPE) ,
//SRC_BCISH as ( SELECT *     from     BWC_CLAIM_ICD_STATUS_HISTORY) ,
//SRC_LOC as ( SELECT *     from     BWC_ICD_LOCATION_TYPE) ,
//SRC_SIT as ( SELECT *     from     BWC_ICD_SITE_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CISH as ( SELECT 
		  HIST_ID                                            AS                                            HIST_ID 
		, CLM_ICD_STS_ID                                     AS                                     CLM_ICD_STS_ID 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, ICD_ID                                             AS                                             ICD_ID 
		, upper( TRIM( ICD_STS_TYP_CD ) )                    AS                                     ICD_STS_TYP_CD 
		, upper( CLM_ICD_STS_PRI_IND )                       AS                                CLM_ICD_STS_PRI_IND 
		, cast( CLM_ICD_STS_EFF_DT as DATE )                 AS                                 CLM_ICD_STS_EFF_DT 
		, cast( CLM_ICD_STS_END_DT as DATE )                 AS                                 CLM_ICD_STS_END_DT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, HIST_EFF_DTM                                       AS                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       AS                                       HIST_END_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CISH
            ),
LOGIC_C as ( SELECT 
		  upper( TRIM( CLM_NO ) )                            AS                                             CLM_NO 
		, AGRE_ID                                            AS                                            AGRE_ID
		, UPPER(CLM_REL_SNPSHT_IND)                                 AS                                 CLM_REL_SNPSHT_IND
 		from SRC_C
            ),
LOGIC_ICD as ( SELECT 
		  upper( TRIM( ICD_CD ) )                            AS                                             ICD_CD 
		, upper( TRIM( ICD_VER_CD ) )                        AS                                         ICD_VER_CD 
		, ICD_ID                                             AS                                             ICD_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_ICD
            ),
LOGIC_STS as ( SELECT 
		  upper( TRIM( ICD_STS_TYP_NM ) )                    AS                                     ICD_STS_TYP_NM 
		, upper( TRIM( ICD_STS_TYP_CD ) )                    AS                                     ICD_STS_TYP_CD 
		from SRC_STS
            ),
LOGIC_BCISH as ( SELECT 
		  cast( ICD_STS_DT as DATE )                         AS                                         ICD_STS_DT 
		, upper( TRIM( ICD_LOC_TYP_CD ) )                    AS                                     ICD_LOC_TYP_CD 
		, upper( TRIM( ICD_SITE_TYP_CD ) )                   AS                                    ICD_SITE_TYP_CD 
		, upper( TRIM( CLM_ICD_DESC ) )                      AS                                       CLM_ICD_DESC 
		, HIST_ID                                            AS                                            HIST_ID 
		from SRC_BCISH
            ),
LOGIC_LOC as ( SELECT 
		  upper( TRIM( ICD_LOC_TYP_NM ) )                    AS                                     ICD_LOC_TYP_NM 
		, upper( TRIM( ICD_LOC_TYP_CD ) )                    AS                                     ICD_LOC_TYP_CD 
		from SRC_LOC
            ),
LOGIC_SIT as ( SELECT 
		  upper( TRIM( ICD_SITE_TYP_NM ) )                   AS                                    ICD_SITE_TYP_NM 
		, upper( TRIM( ICD_SITE_TYP_CD ) )                   AS                                    ICD_SITE_TYP_CD 
		from SRC_SIT
            )

---- RENAME LAYER ----
,

RENAME_CISH as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID
		, CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, ICD_ID                                             as                                             ICD_ID
		, ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND
		, CLM_ICD_STS_EFF_DT                                 as                                 CLM_ICD_STS_EFF_DT
		, CLM_ICD_STS_END_DT                                 as                                 CLM_ICD_STS_END_DT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM
		, HIST_END_DTM                                       as                                       HIST_END_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CISH   ), 
RENAME_C as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
		, AGRE_ID                                            as                                          C_AGRE_ID
		, CLM_REL_SNPSHT_IND                                 AS                                 CLM_REL_SNPSHT_IND 
				FROM     LOGIC_C   ), 
RENAME_ICD as ( SELECT 
		  ICD_CD                                             as                                             ICD_CD
		, ICD_VER_CD                                         as                                         ICD_VER_CD
		, ICD_ID                                             as                                         ICD_ICD_ID
		, VOID_IND                                           as                                       ICD_VOID_IND 
				FROM     LOGIC_ICD   ), 
RENAME_STS as ( SELECT 
		  ICD_STS_TYP_NM                                     as                                     ICD_STS_TYP_NM
		, ICD_STS_TYP_CD                                     as                                 STS_ICD_STS_TYP_CD 
				FROM     LOGIC_STS   ), 
RENAME_BCISH as ( SELECT 
		  ICD_STS_DT                                         as                                         ICD_STS_DT
		, ICD_LOC_TYP_CD                                     as                                     ICD_LOC_TYP_CD
		, ICD_SITE_TYP_CD                                    as                                    ICD_SITE_TYP_CD
		, CLM_ICD_DESC                                       as                                       CLM_ICD_DESC
		, HIST_ID                                            as                                      BCISH_HIST_ID 
				FROM     LOGIC_BCISH   ), 
RENAME_LOC as ( SELECT 
		  ICD_LOC_TYP_NM                                     as                                     ICD_LOC_TYP_NM
		, ICD_LOC_TYP_CD                                     as                                 LOC_ICD_LOC_TYP_CD 
				FROM     LOGIC_LOC   ), 
RENAME_SIT as ( SELECT 
		  ICD_SITE_TYP_NM                                    as                                    ICD_SITE_TYP_NM
		, ICD_SITE_TYP_CD                                    as                                SIT_ICD_SITE_TYP_CD 
				FROM     LOGIC_SIT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CISH                           as ( SELECT * from    RENAME_CISH   ),
FILTER_BCISH                          as ( SELECT * from    RENAME_BCISH   ),
FILTER_C                              as ( SELECT * from    RENAME_C   ),
FILTER_ICD                            as ( SELECT * from    RENAME_ICD 
				WHERE ICD_VOID_IND = 'N'  ),
FILTER_STS                            as ( SELECT * from    RENAME_STS   ),
FILTER_LOC                            as ( SELECT * from    RENAME_LOC   ),
FILTER_SIT                            as ( SELECT * from    RENAME_SIT   ),

---- JOIN LAYER ----

BCISH as ( SELECT * 
				FROM  FILTER_BCISH
				        LEFT JOIN FILTER_LOC ON  FILTER_BCISH.ICD_LOC_TYP_CD =  FILTER_LOC.LOC_ICD_LOC_TYP_CD 
						LEFT JOIN FILTER_SIT ON  FILTER_BCISH.ICD_SITE_TYP_CD =  FILTER_SIT.SIT_ICD_SITE_TYP_CD  ),
CISH as ( SELECT * 
				FROM  FILTER_CISH
				        INNER JOIN BCISH ON  FILTER_CISH.HIST_ID = BCISH.BCISH_HIST_ID 
						INNER JOIN FILTER_C ON  FILTER_CISH.AGRE_ID =  FILTER_C.C_AGRE_ID 
						INNER JOIN FILTER_ICD ON  FILTER_CISH.ICD_ID =  FILTER_ICD.ICD_ICD_ID 
						INNER JOIN FILTER_STS ON  FILTER_CISH.ICD_STS_TYP_CD =  FILTER_STS.STS_ICD_STS_TYP_CD  )
SELECT * 
from CISH