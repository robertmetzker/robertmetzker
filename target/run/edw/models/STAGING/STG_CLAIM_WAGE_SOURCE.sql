

      create or replace  table DEV_EDW.STAGING.STG_CLAIM_WAGE_SOURCE  as
      (---- SRC LAYER ----
WITH
SRC_CWS as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_WAGE_SOURCE ),
SRC_CLM as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM ),
SRC_CEST as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_EMPLOYMENT_STATUS_TYPE ),
SRC_CWCT as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_WAGE_CATEGORY_TYPE ),
//SRC_CWS as ( SELECT *     from     CLAIM_WAGE_SOURCE) ,
//SRC_CLM as ( SELECT *     from     CLAIM) ,
//SRC_CEST as ( SELECT *     from     CLAIM_EMPLOYMENT_STATUS_TYPE) ,
//SRC_CWCT as ( SELECT *     from     CLAIM_WAGE_CATEGORY_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CWS as ( SELECT 
		  CLM_WG_SRC_ID                                      AS                                      CLM_WG_SRC_ID 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, upper( TRIM( CLM_EMPL_STS_TYP_CD ) )               AS                                CLM_EMPL_STS_TYP_CD 
		, upper( TRIM( CLM_WG_CTG_TYP_CD ) )                 AS                                  CLM_WG_CTG_TYP_CD 
		, upper( TRIM( CLM_WG_SRC_JOB_TTL ) )                AS                                 CLM_WG_SRC_JOB_TTL 
		, upper( CLM_WG_SRC_INS_IND )                        AS                                 CLM_WG_SRC_INS_IND 
		, CLM_WG_SRC_OTHR_CUST_ID                            AS                            CLM_WG_SRC_OTHR_CUST_ID 
		, upper( TRIM( CLM_WG_SRC_OTHR_NM ) )                AS                                 CLM_WG_SRC_OTHR_NM 
		, upper( TRIM( CLM_WG_SRC_EMPLR_PHN_NO ) )           AS                            CLM_WG_SRC_EMPLR_PHN_NO 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CWS
            ),
LOGIC_CLM as ( SELECT 
		  upper( TRIM( CLM_NO ) )                            AS                                             CLM_NO 
		, AGRE_ID                                            AS                                            AGRE_ID 
		from SRC_CLM
            ),
LOGIC_CEST as ( SELECT 
		  upper( TRIM( CLM_EMPL_STS_TYP_NM ) )               AS                                CLM_EMPL_STS_TYP_NM 
		, upper( TRIM( CLM_EMPL_STS_TYP_CD ) )               AS                                CLM_EMPL_STS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CEST
            ),
LOGIC_CWCT as ( SELECT 
		  upper( TRIM( CLM_WG_CTG_TYP_NM ) )                 AS                                  CLM_WG_CTG_TYP_NM 
		, upper( TRIM( CLM_WG_CTG_TYP_CD ) )                 AS                                  CLM_WG_CTG_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CWCT
            )

---- RENAME LAYER ----
,

RENAME_CWS as ( SELECT 
		  CLM_WG_SRC_ID                                      as                                      CLM_WG_SRC_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_EMPL_STS_TYP_CD                                as                                CLM_EMPL_STS_TYP_CD
		, CLM_WG_CTG_TYP_CD                                  as                                  CLM_WG_CTG_TYP_CD
		, CLM_WG_SRC_JOB_TTL                                 as                                 CLM_WG_SRC_JOB_TTL
		, CLM_WG_SRC_INS_IND                                 as                                 CLM_WG_SRC_INS_IND
		, CLM_WG_SRC_OTHR_CUST_ID                            as                            CLM_WG_SRC_OTHR_CUST_ID
		, CLM_WG_SRC_OTHR_NM                                 as                                 CLM_WG_SRC_OTHR_NM
		, CLM_WG_SRC_EMPLR_PHN_NO                            as                            CLM_WG_SRC_EMPLR_PHN_NO
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CWS   ), 
RENAME_CLM as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
		, AGRE_ID                                            as                                        CLM_AGRE_ID 
				FROM     LOGIC_CLM   ), 
RENAME_CEST as ( SELECT 
		  CLM_EMPL_STS_TYP_NM                                as                                CLM_EMPL_STS_TYP_NM
		, CLM_EMPL_STS_TYP_CD                                as                           CEST_CLM_EMPL_STS_TYP_CD
		, VOID_IND                                           as                                      CEST_VOID_IND 
				FROM     LOGIC_CEST   ), 
RENAME_CWCT as ( SELECT 
		  CLM_WG_CTG_TYP_NM                                  as                                  CLM_WG_CTG_TYP_NM
		, CLM_WG_CTG_TYP_CD                                  as                             CWCT_CLM_WG_CTG_TYP_CD
		, VOID_IND                                           as                                      CWCT_VOID_IND 
				FROM     LOGIC_CWCT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CWS                            as ( SELECT * from    RENAME_CWS   ),
FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),
FILTER_CWCT                           as ( SELECT * from    RENAME_CWCT 
				WHERE CWCT_VOID_IND = 'N'  ),
FILTER_CEST                           as ( SELECT * from    RENAME_CEST 
				WHERE CEST_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CWS as ( SELECT * 
				FROM  FILTER_CWS
				LEFT JOIN FILTER_CLM ON  FILTER_CWS.AGRE_ID =  FILTER_CLM.CLM_AGRE_ID 
								LEFT JOIN FILTER_CWCT ON  FILTER_CWS.CLM_WG_CTG_TYP_CD =  FILTER_CWCT.CWCT_CLM_WG_CTG_TYP_CD 
								LEFT JOIN FILTER_CEST ON  FILTER_CWS.CLM_EMPL_STS_TYP_CD =  FILTER_CEST.CEST_CLM_EMPL_STS_TYP_CD  )
SELECT * 
from CWS
      );
    