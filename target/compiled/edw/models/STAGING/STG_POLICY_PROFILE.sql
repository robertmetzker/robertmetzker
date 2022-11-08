---- SRC LAYER ----
WITH
SRC_PROF as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PROFILE ),
SRC_PP as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD ),
SRC_PPCT as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PROFILE_CATEGORY_TYPE ),
SRC_PS as ( SELECT *     from     DEV_VIEWS.PCMP.PROFILE_STATEMENT ),
//SRC_PROF as ( SELECT *     from     POLICY_PROFILE) ,
//SRC_PP as ( SELECT *     from     POLICY_PERIOD) ,
//SRC_PPCT as ( SELECT *     from     POLICY_PROFILE_CATEGORY_TYPE) ,
//SRC_PS as ( SELECT *     from     PROFILE_STATEMENT) ,

---- LOGIC LAYER ----

LOGIC_PROF as ( SELECT 
		  PLCY_PRFL_ID                                       AS                                       PLCY_PRFL_ID 
		, PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		, upper( TRIM( PLCY_PRFL_CTG_TYP_CD ) )              AS                               PLCY_PRFL_CTG_TYP_CD 
		, PRFL_STMT_ID                                       AS                                       PRFL_STMT_ID 
		, upper( TRIM( PLCY_PRFL_ANSW_TEXT ) )               AS                                PLCY_PRFL_ANSW_TEXT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( PLCY_PRFL_VOID_IND )                        AS                                 PLCY_PRFL_VOID_IND 
		from SRC_PROF
            ),
LOGIC_PP as ( SELECT 
		  upper( TRIM( PLCY_NO ) )                           AS                                            PLCY_NO 
		, PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		from SRC_PP
            ),
LOGIC_PPCT as ( SELECT 
		  upper( TRIM( PLCY_PRFL_CTG_TYP_NM ) )              AS                               PLCY_PRFL_CTG_TYP_NM 
		, upper( TRIM( PLCY_PRFL_CTG_TYP_CD ) )              AS                               PLCY_PRFL_CTG_TYP_CD 
		, upper( PLCY_PRFL_CTG_TYP_VOID_IND )                AS                         PLCY_PRFL_CTG_TYP_VOID_IND 
		from SRC_PPCT
            ),
LOGIC_PS as ( SELECT 
		  upper( TRIM( PRFL_STMT_DESC ) )                    AS                                     PRFL_STMT_DESC 
		, PRFL_STMT_ID                                       AS                                       PRFL_STMT_ID 
		from SRC_PS
            )

---- RENAME LAYER ----
,

RENAME_PROF as ( SELECT 
		  PLCY_PRFL_ID                                       as                                       PLCY_PRFL_ID
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, PLCY_PRFL_CTG_TYP_CD                               as                               PLCY_PRFL_CTG_TYP_CD
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID
		, PLCY_PRFL_ANSW_TEXT                                as                                PLCY_PRFL_ANSW_TEXT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, PLCY_PRFL_VOID_IND                                 as                                 PLCY_PRFL_VOID_IND 
				FROM     LOGIC_PROF   ), 
RENAME_PP as ( SELECT 
		  PLCY_NO                                            as                                            PLCY_NO
		, PLCY_PRD_ID                                        as                                     PP_PLCY_PRD_ID 
				FROM     LOGIC_PP   ), 
RENAME_PPCT as ( SELECT 
		  PLCY_PRFL_CTG_TYP_NM                               as                               PLCY_PRFL_CTG_TYP_NM
		, PLCY_PRFL_CTG_TYP_CD                               as                          PPCT_PLCY_PRFL_CTG_TYP_CD
		, PLCY_PRFL_CTG_TYP_VOID_IND                         as                         PLCY_PRFL_CTG_TYP_VOID_IND 
				FROM     LOGIC_PPCT   ), 
RENAME_PS as ( SELECT 
		  PRFL_STMT_DESC                                     as                                     PRFL_STMT_DESC
		, PRFL_STMT_ID                                       as                                    PS_PRFL_STMT_ID 
				FROM     LOGIC_PS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PROF                           as ( SELECT * from    RENAME_PROF   ),
FILTER_PS                             as ( SELECT * from    RENAME_PS   ),
FILTER_PP                             as ( SELECT * from    RENAME_PP   ),
FILTER_PPCT                           as ( SELECT * from    RENAME_PPCT 
				WHERE PLCY_PRFL_CTG_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

PROF as ( SELECT * 
				FROM  FILTER_PROF
				INNER JOIN FILTER_PS ON  FILTER_PROF.PRFL_STMT_ID =  FILTER_PS.PS_PRFL_STMT_ID 
								LEFT JOIN FILTER_PP ON  FILTER_PROF.PLCY_PRD_ID =  FILTER_PP.PP_PLCY_PRD_ID 
								LEFT JOIN FILTER_PPCT ON  FILTER_PROF.PLCY_PRFL_CTG_TYP_CD =  FILTER_PPCT.PPCT_PLCY_PRFL_CTG_TYP_CD  )
SELECT * 
from PROF