---- SRC LAYER ----
WITH
SRC_PREM as ( SELECT *     from     DEV_VIEWS.PCMP.PREMIUM_PERIOD ),
SRC_PP as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD ),
SRC_PT as ( SELECT *     from     DEV_VIEWS.PCMP.PREMIUM_TYPE ),
//SRC_PREM as ( SELECT *     from     PREMIUM_PERIOD) ,
//SRC_PP as ( SELECT *     from     POLICY_PERIOD) ,
//SRC_PT as ( SELECT *     from     PREMIUM_TYPE) ,

---- LOGIC LAYER ----

LOGIC_PREM as ( SELECT 
		  PREM_PRD_ID                                        AS                                        PREM_PRD_ID 
		, PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		, upper( TRIM( PREM_TYP_CD ) )                       AS                                        PREM_TYP_CD 
		, cast( PREM_PRD_EFF_DT as DATE )                    AS                                    PREM_PRD_EFF_DT 
		, cast( PREM_PRD_END_DT as DATE )                    AS                                    PREM_PRD_END_DT 
		, cast( PREM_PRD_ORIG_END_DT as DATE )               AS                               PREM_PRD_ORIG_END_DT 
		, cast( PREM_PRD_RT_DT as DATE )                     AS                                     PREM_PRD_RT_DT 
		, upper( PREM_PRD_SPLT_MOD_IND )                     AS                              PREM_PRD_SPLT_MOD_IND 
		, upper( PREM_PRD_CANC_IND )                         AS                                  PREM_PRD_CANC_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PREM
            ),
LOGIC_PP as ( SELECT 
		  upper( TRIM( PLCY_NO ) )                           AS                                            PLCY_NO 
		, PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		from SRC_PP
            ),
LOGIC_PT as ( SELECT 
		  upper( TRIM( PREM_TYP_NM ) )                       AS                                        PREM_TYP_NM 
		, upper( TRIM( PREM_TYP_CD ) )                       AS                                        PREM_TYP_CD 
		, upper( PREM_TYP_VOID_IND )                         AS                                  PREM_TYP_VOID_IND 
		from SRC_PT
            )

---- RENAME LAYER ----
,

RENAME_PREM as ( SELECT 
		  PREM_PRD_ID                                        as                                        PREM_PRD_ID
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, PREM_TYP_CD                                        as                                        PREM_TYP_CD
		, PREM_PRD_EFF_DT                                    as                                    PREM_PRD_EFF_DT
		, PREM_PRD_END_DT                                    as                                    PREM_PRD_END_DT
		, PREM_PRD_ORIG_END_DT                               as                               PREM_PRD_ORIG_END_DT
		, PREM_PRD_RT_DT                                     as                                     PREM_PRD_RT_DT
		, PREM_PRD_SPLT_MOD_IND                              as                              PREM_PRD_SPLT_MOD_IND
		, PREM_PRD_CANC_IND                                  as                                  PREM_PRD_CANC_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_PREM   ), 
RENAME_PP as ( SELECT 
		  PLCY_NO                                            as                                            PLCY_NO
		, PLCY_PRD_ID                                        as                                     PP_PLCY_PRD_ID 
				FROM     LOGIC_PP   ), 
RENAME_PT as ( SELECT 
		  PREM_TYP_NM                                        as                                        PREM_TYP_NM
		, PREM_TYP_CD                                        as                                     PT_PREM_TYP_CD
		, PREM_TYP_VOID_IND                                  as                                  PREM_TYP_VOID_IND 
				FROM     LOGIC_PT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PREM                           as ( SELECT * from    RENAME_PREM   ),
FILTER_PP                             as ( SELECT * from    RENAME_PP   ),
FILTER_PT                             as ( SELECT * from    RENAME_PT 
				WHERE PREM_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

PREM as ( SELECT * 
				FROM  FILTER_PREM
				INNER JOIN FILTER_PP ON  FILTER_PREM.PLCY_PRD_ID =  FILTER_PP.PP_PLCY_PRD_ID 
								LEFT JOIN FILTER_PT ON  FILTER_PREM.PREM_TYP_CD =  FILTER_PT.PT_PREM_TYP_CD  )
SELECT * 
from PREM