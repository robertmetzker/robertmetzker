

      create or replace  table DEV_EDW.STAGING.STG_CASE_PROFILE  as
      (---- SRC LAYER ----
WITH
SRC_CP as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_PROFILE ),
SRC_CPCT as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_PROFILE_CATEGORY_TYPE ),
SRC_PS as ( SELECT *     from     DEV_VIEWS.PCMP.PROFILE_STATEMENT ),
//SRC_CP as ( SELECT *     from     CASE_PROFILE) ,
//SRC_CPCT as ( SELECT *     from     CASE_PROFILE_CATEGORY_TYPE) ,
//SRC_PS as ( SELECT *     from     PROFILE_STATEMENT) ,

---- LOGIC LAYER ----

LOGIC_CP as ( SELECT 
		  CASE_PRFL_ID                                       AS                                       CASE_PRFL_ID 
		, CASE_ID                                            AS                                            CASE_ID 
		, CASE_PRFL_SEQ_NO                                   AS                                   CASE_PRFL_SEQ_NO 
		, upper( TRIM( CASE_PRFL_CTG_TYP_CD ) )              AS                               CASE_PRFL_CTG_TYP_CD 
		, PRFL_STMT_ID                                       AS                                       PRFL_STMT_ID 
		, upper( TRIM( CASE_PRFL_ANSW_TXT ) )                AS                                 CASE_PRFL_ANSW_TXT 
		, CASE_EVNT_ID                                       AS                                       CASE_EVNT_ID 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CP
            ),
LOGIC_CPCT as ( SELECT 
		  upper( TRIM( CASE_PRFL_CTG_TYP_NM ) )              AS                               CASE_PRFL_CTG_TYP_NM 
		, upper( TRIM( CASE_PRFL_CTG_TYP_CD ) )              AS                               CASE_PRFL_CTG_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CPCT
            ),
LOGIC_PS as ( SELECT 
		  upper( TRIM( PRFL_STMT_DESC ) )                    AS                                     PRFL_STMT_DESC  
		, PRFL_STMT_ID                                       AS                                       PRFL_STMT_ID 
		from SRC_PS
            )

---- RENAME LAYER ----
,

RENAME_CP as ( SELECT 
		  CASE_PRFL_ID                                       as                                       CASE_PRFL_ID
		, CASE_ID                                            as                                            CASE_ID
		, CASE_PRFL_SEQ_NO                                   as                                   CASE_PRFL_SEQ_NO
		, CASE_PRFL_CTG_TYP_CD                               as                               CASE_PRFL_CTG_TYP_CD
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID
		, CASE_PRFL_ANSW_TXT                                 as                                 CASE_PRFL_ANSW_TXT
		, CASE_EVNT_ID                                       as                                       CASE_EVNT_ID
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CP   ), 
RENAME_CPCT as ( SELECT 
		  CASE_PRFL_CTG_TYP_NM                               as                               CASE_PRFL_CTG_TYP_NM
		, CASE_PRFL_CTG_TYP_CD                               as                          CPCT_CASE_PRFL_CTG_TYP_CD
		, VOID_IND                                           as                                      CPCT_VOID_IND 
				FROM     LOGIC_CPCT   ), 
RENAME_PS as ( SELECT 
		  PRFL_STMT_DESC                                     as                                     PRFL_STMT_DESC
		, PRFL_STMT_ID                                       as                                    PS_PRFL_STMT_ID 
				FROM     LOGIC_PS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CP                             as ( SELECT * from    RENAME_CP   ),
FILTER_PS                             as ( SELECT * from    RENAME_PS   ),
FILTER_CPCT                           as ( SELECT * from    RENAME_CPCT 
				WHERE CPCT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CP as ( SELECT * 
				FROM  FILTER_CP
				INNER JOIN FILTER_PS ON  FILTER_CP.PRFL_STMT_ID =  FILTER_PS.PS_PRFL_STMT_ID 
						LEFT JOIN FILTER_CPCT ON  FILTER_CP.CASE_PRFL_CTG_TYP_CD =  FILTER_CPCT.CPCT_CASE_PRFL_CTG_TYP_CD  )
SELECT * 
from CP
      );
    