---- SRC LAYER ----
WITH
SRC_P as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY ),
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER ),
SRC_PP as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD ),
SRC_A as ( SELECT *     from     DEV_VIEWS.PCMP.AGREEMENT ),
//SRC_P as ( SELECT *     from     POLICY) ,
//SRC_C as ( SELECT *     from     CUSTOMER) ,
//SRC_PP as ( SELECT *     from     POLICY_PERIOD) ,
//SRC_A as ( SELECT *     from     AGREEMENT) ,

---- LOGIC LAYER ----

LOGIC_P as ( SELECT 
		  upper( TRIM( QUOT_NO ) )                           AS                                            QUOT_NO 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, cast( QUOT_RECV_DT as DATE )                       AS                                       QUOT_RECV_DT 
		, CUST_ID_ACCT_HLDR                                  AS                                  CUST_ID_ACCT_HLDR 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_P
            ),
LOGIC_C as ( SELECT 
		  upper( TRIM( CUST_NO ) )                           AS                                            CUST_NO 
		, CUST_ID                                            AS                                            CUST_ID 
		from SRC_C
            ),
LOGIC_PP as ( SELECT DISTINCT
		  upper( TRIM( PLCY_NO ) )                           AS                                            PLCY_NO 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PP
            ),
LOGIC_A as ( SELECT 
		  AGRE_ID                                            AS                                            AGRE_ID 
		, upper( TRIM( AGRE_TYP_CD ) )                       AS                                        AGRE_TYP_CD 
		from SRC_A
            )

---- RENAME LAYER ----
,

RENAME_P as ( SELECT 
		  QUOT_NO                                            as                                            QUOT_NO
		, AGRE_ID                                            as                                            AGRE_ID
		, QUOT_RECV_DT                                       as                                       QUOT_RECV_DT
		, CUST_ID_ACCT_HLDR                                  as                                  CUST_ID_ACCT_HLDR
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_P   ), 
RENAME_C as ( SELECT 
		  CUST_NO                                            as                                            CUST_NO
		, CUST_ID                                            as                                            CUST_ID 
				FROM     LOGIC_C   ), 
RENAME_PP as ( SELECT 
		  PLCY_NO                                            as                                            PLCY_NO
		, AGRE_ID                                            as                                         PP_AGRE_ID
		, VOID_IND                                           as                                        PP_VOID_IND 
				FROM     LOGIC_PP   ), 
RENAME_A as ( SELECT 
		  AGRE_ID                                            as                                          A_AGRE_ID
		, AGRE_TYP_CD                                        as                                      A_AGRE_TYP_CD 
				FROM     LOGIC_A   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_P                              as ( SELECT * from    RENAME_P   ),
FILTER_A                              as ( SELECT * from    RENAME_A 
				WHERE A_AGRE_TYP_CD = 'QUOT'  ),
FILTER_PP                             as ( SELECT * from    RENAME_PP 
				WHERE PP_VOID_IND = 'N'  ),
FILTER_C                              as ( SELECT * from    RENAME_C   ),

---- JOIN LAYER ----

P as ( SELECT * 
				FROM  FILTER_P
				INNER JOIN FILTER_A ON  FILTER_P.AGRE_ID =  FILTER_A.A_AGRE_ID 
								LEFT JOIN FILTER_PP ON  FILTER_P.AGRE_ID =  FILTER_PP.PP_AGRE_ID 
						INNER JOIN FILTER_C ON  FILTER_P.CUST_ID_ACCT_HLDR =  FILTER_C.CUST_ID  )
SELECT * 
from P