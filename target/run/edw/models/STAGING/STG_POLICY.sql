

      create or replace  table DEV_EDW.STAGING.STG_POLICY  as
      (---- SRC LAYER ----
WITH
SRC_PP as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD ),
SRC_P as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY ),
SRC_A as ( SELECT *     from     DEV_VIEWS.PCMP.AGREEMENT ),
SRC_AT as ( SELECT *     from     DEV_VIEWS.PCMP.AGREEMENT_TYPE ),
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER ),
//SRC_PP as ( SELECT *     from     POLICY_PERIOD) ,
//SRC_P as ( SELECT *     from     POLICY) ,
//SRC_A as ( SELECT *     from     AGREEMENT) ,
//SRC_AT as ( SELECT *     from     AGREEMENT_TYPE) ,
//SRC_C as ( SELECT *     from     CUSTOMER) ,

---- LOGIC LAYER ----

LOGIC_PP as ( SELECT DISTINCT
		  upper( TRIM( PLCY_NO ) )                           AS                                            PLCY_NO 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PP
            ),
LOGIC_P as ( SELECT 
		  AGRE_ID                                            AS                                            AGRE_ID 
		, CUST_ID_ACCT_HLDR                                  AS                                  CUST_ID_ACCT_HLDR 
		, cast( PLCY_ORIG_DT as DATE )                       AS                                       PLCY_ORIG_DT 
		, upper( PLCY_WEB_QUOT_IND )                         AS                                  PLCY_WEB_QUOT_IND 
		, upper( PLCY_SHLL_IND )                             AS                                      PLCY_SHLL_IND 
		, upper( PLCY_KY_ACCT_IND )                          AS                                   PLCY_KY_ACCT_IND 
		, upper( PLCY_OUT_OF_CYC_IND )                       AS                                PLCY_OUT_OF_CYC_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_P
            ),
LOGIC_A as ( SELECT 
		  upper( TRIM( AGRE_TYP_CD ) )                       AS                                        AGRE_TYP_CD 
		, AGRE_ID                                            AS                                            AGRE_ID 
		from SRC_A
            ),
LOGIC_AT as ( SELECT 
		  upper( TRIM( AGRE_TYP_NM ) )                       AS                                        AGRE_TYP_NM 
		, upper( TRIM( AGRE_TYP_CD ) )                       AS                                        AGRE_TYP_CD 
		from SRC_AT
            ),
LOGIC_C as ( SELECT 
		  upper( TRIM( CUST_NO ) )                           AS                                            CUST_NO 
		, CUST_ID                                            AS                                            CUST_ID 
		from SRC_C
            )

---- RENAME LAYER ----
,

RENAME_PP as ( SELECT 
		  PLCY_NO                                            as                                            PLCY_NO
		, AGRE_ID                                            as                                         PP_AGRE_ID
		, VOID_IND                                           as                                        PP_VOID_IND 
				FROM     LOGIC_PP   ), 
RENAME_P as ( SELECT 
		  AGRE_ID                                            as                                            AGRE_ID
		, CUST_ID_ACCT_HLDR                                  as                                  CUST_ID_ACCT_HLDR
		, PLCY_ORIG_DT                                       as                                       PLCY_ORIG_DT
		, PLCY_WEB_QUOT_IND                                  as                       STRAIGHT_THRU_PROCESSING_IND
		, PLCY_SHLL_IND                                      as                                      PLCY_SHLL_IND
		, PLCY_KY_ACCT_IND                                   as                                   PLCY_KY_ACCT_IND
		, PLCY_OUT_OF_CYC_IND                                as                                PLCY_OUT_OF_CYC_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_P   ), 
RENAME_A as ( SELECT 
		  AGRE_TYP_CD                                        as                                        AGRE_TYP_CD
		, AGRE_ID                                            as                                          A_AGRE_ID 
				FROM     LOGIC_A   ), 
RENAME_AT as ( SELECT 
		  AGRE_TYP_NM                                        as                                        AGRE_TYP_NM
		, AGRE_TYP_CD                                        as                                     AT_AGRE_TYP_CD 
				FROM     LOGIC_AT   ), 
RENAME_C as ( SELECT 
		  CUST_NO                                            as                                            CUST_NO
		, CUST_ID                                            as                                            CUST_ID 
				FROM     LOGIC_C   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_P                              as ( SELECT * from    RENAME_P   ),
FILTER_A                              as ( SELECT * from    RENAME_A 
				WHERE AGRE_TYP_CD = 'PLCY'  ),
FILTER_PP                             as ( SELECT * from    RENAME_PP 
				WHERE PP_VOID_IND = 'N'  ),
FILTER_AT                             as ( SELECT * from    RENAME_AT   ),
FILTER_C                              as ( SELECT * from    RENAME_C   ),

---- JOIN LAYER ----

A as ( SELECT * 
				FROM  FILTER_A
				LEFT JOIN FILTER_AT ON  FILTER_A.AGRE_TYP_CD =  FILTER_AT.AT_AGRE_TYP_CD  ),
P as ( SELECT * 
				FROM  FILTER_P
				INNER JOIN A ON  FILTER_P.AGRE_ID = A.A_AGRE_ID 
						LEFT JOIN FILTER_PP ON  FILTER_P.AGRE_ID =  FILTER_PP.PP_AGRE_ID 
						INNER JOIN FILTER_C ON  FILTER_P.CUST_ID_ACCT_HLDR =  FILTER_C.CUST_ID  )
SELECT * 
from P
      );
    