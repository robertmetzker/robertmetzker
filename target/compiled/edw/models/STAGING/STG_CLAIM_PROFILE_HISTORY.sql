---- SRC LAYER ----
WITH
SRC_CP             as ( SELECT *     FROM     DEV_VIEWS.PCMP.CLAIM_PROFILE_HISTORY ),
SRC_CPCT           as ( SELECT *     FROM     DEV_VIEWS.PCMP.CLAIM_PROFILE_CATEGORY_TYPE ),
SRC_PS             as ( SELECT *     FROM     DEV_VIEWS.PCMP.PROFILE_STATEMENT ),
SRC_PSVT           as ( SELECT *     FROM     DEV_VIEWS.PCMP.PROFILE_SELECTION_VALUE_TYPE ),
//SRC_CP             as ( SELECT *     FROM     CLAIM_PROFILE_HISTORY) ,
//SRC_CPCT           as ( SELECT *     FROM     CLAIM_PROFILE_CATEGORY_TYPE) ,
//SRC_PS             as ( SELECT *     FROM     PROFILE_STATEMENT) ,
//SRC_PSVT           as ( SELECT *     FROM     PROFILE_SELECTION_VALUE_TYPE) ,

---- LOGIC LAYER ----


LOGIC_CP as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID 
		, CLM_PRFL_ID                                        as                                        CLM_PRFL_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		, PTCP_ID                                            as                                            PTCP_ID 
		, upper( TRIM( CLM_PRFL_CTG_TYP_CD ) )               as                                CLM_PRFL_CTG_TYP_CD 
		, upper( TRIM( LOB_TYP_CD ) )                        as                                         LOB_TYP_CD 
		, upper( TRIM( PTCP_TYP_CD ) )                       as                                        PTCP_TYP_CD 
		, upper( TRIM( JUR_TYP_CD ) )                        as                                         JUR_TYP_CD 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
		, PRFL_STMT_SEL_ID                                   as                                   PRFL_STMT_SEL_ID 
		, PRFL_STMT_SEL_NEST_ID                              as                              PRFL_STMT_SEL_NEST_ID 
		, upper( TRIM( PRFL_SEL_VAL_TYP_CD ) )               as                                PRFL_SEL_VAL_TYP_CD 
		, nullif(TRIM( CLM_PRFL_ANSW_TEXT ),'')              as                                 CLM_PRFL_ANSW_TEXT 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       as                                       HIST_END_DTM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		FROM SRC_CP
            ),

LOGIC_CPCT as ( SELECT 
		  upper( TRIM( CLM_PRFL_CTG_TYP_NM ) )               as                                CLM_PRFL_CTG_TYP_NM 
		, upper( TRIM( CLM_PRFL_CTG_TYP_CD ) )               as                                CLM_PRFL_CTG_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		FROM SRC_CPCT
            ),

LOGIC_PS as ( SELECT 
		  upper( TRIM( PRFL_STMT_DESC ) )                    as                                     PRFL_STMT_DESC 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
		, upper( PRFL_STMT_VOID_IND )                        as                                 PRFL_STMT_VOID_IND 
		FROM SRC_PS
            ),

LOGIC_PSVT as ( SELECT 
		  upper( TRIM( PRFL_SEL_VAL_TYP_NM ) )               as                                PRFL_SEL_VAL_TYP_NM 
		, TRIM( PRFL_SEL_VAL_TYP_CD )                        as                                PRFL_SEL_VAL_TYP_CD 
		, upper( PRFL_SEL_VAL_TYP_VOID_IND )                 as                          PRFL_SEL_VAL_TYP_VOID_IND 
		FROM SRC_PSVT
            )

---- RENAME LAYER ----
,

RENAME_CP         as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID
		, CLM_PRFL_ID                                        as                                        CLM_PRFL_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, PTCP_ID                                            as                                            PTCP_ID
		, CLM_PRFL_CTG_TYP_CD                                as                                CLM_PRFL_CTG_TYP_CD
		, LOB_TYP_CD                                         as                                         LOB_TYP_CD
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD
		, JUR_TYP_CD                                         as                                         JUR_TYP_CD
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID
		, PRFL_STMT_SEL_ID                                   as                                   PRFL_STMT_SEL_ID
		, PRFL_STMT_SEL_NEST_ID                              as                              PRFL_STMT_SEL_NEST_ID
		, PRFL_SEL_VAL_TYP_CD                                as                                PRFL_SEL_VAL_TYP_CD
		, CLM_PRFL_ANSW_TEXT                                 as                                 CLM_PRFL_ANSW_TEXT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM
		, HIST_END_DTM                                       as                                       HIST_END_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CP   ), 
RENAME_CPCT       as ( SELECT 
		  CLM_PRFL_CTG_TYP_NM                                as                                CLM_PRFL_CTG_TYP_NM
		, CLM_PRFL_CTG_TYP_CD                                as                           CPCT_CLM_PRFL_CTG_TYP_CD
		, VOID_IND                                           as                                      CPCT_VOID_IND 
				FROM     LOGIC_CPCT   ), 
RENAME_PS         as ( SELECT 
		  PRFL_STMT_DESC                                     as                                     PRFL_STMT_DESC
		, PRFL_STMT_ID                                       as                                    PS_PRFL_STMT_ID
		, PRFL_STMT_VOID_IND                                 as                              PS_PRFL_STMT_VOID_IND 
				FROM     LOGIC_PS   ), 
RENAME_PSVT       as ( SELECT 
		  PRFL_SEL_VAL_TYP_NM                                as                                PRFL_SEL_VAL_TYP_NM
		, PRFL_SEL_VAL_TYP_CD                                as                           PSVT_PRFL_SEL_VAL_TYP_CD
		, PRFL_SEL_VAL_TYP_VOID_IND                          as                     PSVT_PRFL_SEL_VAL_TYP_VOID_IND 
				FROM     LOGIC_PSVT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CP                             as ( SELECT * FROM    RENAME_CP   ),
FILTER_CPCT                           as ( SELECT * FROM    RENAME_CPCT 
                                            WHERE CPCT_VOID_IND = 'N'  ),
FILTER_PS                             as ( SELECT * FROM    RENAME_PS 
                                            WHERE PS_PRFL_STMT_VOID_IND = 'N'  ),
FILTER_PSVT                           as ( SELECT * FROM    RENAME_PSVT 
                                            WHERE PSVT_PRFL_SEL_VAL_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CP as ( SELECT *  
				FROM  FILTER_CP
				LEFT JOIN  FILTER_CPCT ON  FILTER_CP.CLM_PRFL_CTG_TYP_CD =  FILTER_CPCT.CPCT_CLM_PRFL_CTG_TYP_CD 
								LEFT JOIN  FILTER_PS ON  FILTER_CP.PRFL_STMT_ID =  FILTER_PS.PS_PRFL_STMT_ID 
								LEFT JOIN  FILTER_PSVT ON  FILTER_CP.CLM_PRFL_ANSW_TEXT =  FILTER_PSVT.PSVT_PRFL_SEL_VAL_TYP_CD  )
SELECT 
		  HIST_ID
		, CLM_PRFL_ID
		, AGRE_ID
		, PTCP_ID
		, CLM_PRFL_CTG_TYP_CD
		, CLM_PRFL_CTG_TYP_NM
		, LOB_TYP_CD
		, PTCP_TYP_CD
		, JUR_TYP_CD
		, PRFL_STMT_ID
		, PRFL_STMT_DESC
		, PRFL_STMT_SEL_ID
		, PRFL_STMT_SEL_NEST_ID
		, PRFL_SEL_VAL_TYP_CD
		, PRFL_SEL_VAL_TYP_NM
		, upper(CLM_PRFL_ANSW_TEXT) as CLM_PRFL_ANSW_TEXT
		, AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM
		, HIST_EFF_DTM
		, HIST_END_DTM
		, VOID_IND 
FROM CP