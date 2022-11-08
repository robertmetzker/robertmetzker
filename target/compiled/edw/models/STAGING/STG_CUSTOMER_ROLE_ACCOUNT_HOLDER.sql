---- SRC LAYER ----
WITH
SRC_CR as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_ROLE ),
SRC_CRT as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_ROLE_TYPE ),
SRC_RAH as ( SELECT *     from     DEV_VIEWS.PCMP.ROLE_ACCOUNT_HOLDER ),
SRC_SIC as ( SELECT *     from     DEV_VIEWS.PCMP.SIC_TYPE ),
SRC_AST as ( SELECT *     from     DEV_VIEWS.PCMP.ACCOUNT_STATUS_TYPE ),
//SRC_CR as ( SELECT *     from     CUSTOMER_ROLE) ,
//SRC_CRT as ( SELECT *     from     CUSTOMER_ROLE_TYPE) ,
//SRC_RAH as ( SELECT *     from     ROLE_ACCOUNT_HOLDER) ,
//SRC_SIC as ( SELECT *     from     SIC_TYPE) ,
//SRC_AST as ( SELECT *     from     ACCOUNT_STATUS_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CR as ( SELECT 
		  ROLE_ID                                            AS                                            ROLE_ID 
		, CUST_ID                                            AS                                            CUST_ID 
		, upper( TRIM( ROLE_TYP_CD ) )                       AS                                        ROLE_TYP_CD 
		from SRC_CR
            ),
LOGIC_CRT as ( SELECT 
		  upper( TRIM( CUST_ROLE_TYP_NM ) )                  AS                                   CUST_ROLE_TYP_NM 
		, upper( TRIM( CUST_ROLE_TYP_CD ) )                  AS                                   CUST_ROLE_TYP_CD 
		from SRC_CRT
            ),
LOGIC_RAH as ( SELECT 
		  ROLE_ACCT_HLDR_ID                                  AS                                  ROLE_ACCT_HLDR_ID 
		, upper( TRIM( ROLE_ACCT_HLDR_REF_NO ) )             AS                              ROLE_ACCT_HLDR_REF_NO 
		, upper( TRIM( SIC_TYP_CD ) )                        AS                                         SIC_TYP_CD 
		, upper( TRIM( ACCT_STS_TYP_CD ) )                   AS                                    ACCT_STS_TYP_CD 
		, upper( ROLE_ACCT_HLDR_RISK_MGMT_IND )              AS                       ROLE_ACCT_HLDR_RISK_MGMT_IND 
		, ROLE_ACCT_HLDR_FST_YR_WRTN_DT                      AS                      ROLE_ACCT_HLDR_FST_YR_WRTN_DT 
		, upper( ROLE_ACCT_HLDR_STRM_BILL_IND )              AS                       ROLE_ACCT_HLDR_STRM_BILL_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		, ROLE_ID                                            AS                                            ROLE_ID 
		from SRC_RAH
            ),
LOGIC_SIC as ( SELECT 
		  upper( TRIM( SIC_TYP_NM ) )                        AS                                         SIC_TYP_NM 
		, upper( TRIM( SIC_TYP_CD ) )                        AS                                         SIC_TYP_CD 
		, upper( SIC_TYP_VOID_IND )                          AS                                   SIC_TYP_VOID_IND 
		from SRC_SIC
            ),
LOGIC_AST as ( SELECT 
		  upper( TRIM( ACCT_STS_TYP_NM ) )                   AS                                    ACCT_STS_TYP_NM 
		, upper( TRIM( ACCT_STS_TYP_CD ) )                   AS                                    ACCT_STS_TYP_CD 
		from SRC_AST
            )

---- RENAME LAYER ----
,

RENAME_CR as ( SELECT 
		  ROLE_ID                                            as                                            ROLE_ID
		, CUST_ID                                            as                                            CUST_ID
		, ROLE_TYP_CD                                        as                                        ROLE_TYP_CD 
				FROM     LOGIC_CR   ), 
RENAME_CRT as ( SELECT 
		  CUST_ROLE_TYP_NM                                   as                                   CUST_ROLE_TYP_NM
		, CUST_ROLE_TYP_CD                                   as                                   CUST_ROLE_TYP_CD 
				FROM     LOGIC_CRT   ), 
RENAME_RAH as ( SELECT 
		  ROLE_ACCT_HLDR_ID                                  as                                  ROLE_ACCT_HLDR_ID
		, ROLE_ACCT_HLDR_REF_NO                              as                              ROLE_ACCT_HLDR_REF_NO
		, SIC_TYP_CD                                         as                                         SIC_TYP_CD
		, ACCT_STS_TYP_CD                                    as                                    ACCT_STS_TYP_CD
		, ROLE_ACCT_HLDR_RISK_MGMT_IND                       as                       ROLE_ACCT_HLDR_RISK_MGMT_IND
		, ROLE_ACCT_HLDR_FST_YR_WRTN_DT                      as                      ROLE_ACCT_HLDR_FST_YR_WRTN_DT
		, ROLE_ACCT_HLDR_STRM_BILL_IND                       as                       ROLE_ACCT_HLDR_STRM_BILL_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND
		, ROLE_ID                                            as                                        RAH_ROLE_ID 
				FROM     LOGIC_RAH   ), 
RENAME_SIC as ( SELECT 
		  SIC_TYP_NM                                         as                                         SIC_TYP_NM
		, SIC_TYP_CD                                         as                                     SIC_SIC_TYP_CD
		, SIC_TYP_VOID_IND                                   as                                   SIC_TYP_VOID_IND 
				FROM     LOGIC_SIC   ), 
RENAME_AST as ( SELECT 
		  ACCT_STS_TYP_NM                                    as                                    ACCT_STS_TYP_NM
		, ACCT_STS_TYP_CD                                    as                                AST_ACCT_STS_TYP_CD 
				FROM     LOGIC_AST   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CR                             as ( SELECT * from    RENAME_CR   ),
FILTER_RAH                            as ( SELECT * from    RENAME_RAH   ),
FILTER_CRT                            as ( SELECT * from    RENAME_CRT   ),
FILTER_SIC                            as ( SELECT * from    RENAME_SIC 
				WHERE SIC_TYP_VOID_IND = 'N'  ),
FILTER_AST                            as ( SELECT * from    RENAME_AST   ),

---- JOIN LAYER ----

RAH as ( SELECT * 
				FROM  FILTER_RAH
				LEFT JOIN FILTER_SIC ON  FILTER_RAH.SIC_TYP_CD =  FILTER_SIC.SIC_SIC_TYP_CD 
								LEFT JOIN FILTER_AST ON  FILTER_RAH.ACCT_STS_TYP_CD =  FILTER_AST.AST_ACCT_STS_TYP_CD  ),
CR as ( SELECT * 
				FROM  FILTER_CR
				INNER JOIN RAH ON  FILTER_CR.ROLE_ID = RAH.RAH_ROLE_ID 
						INNER JOIN FILTER_CRT ON  FILTER_CR.ROLE_TYP_CD =  FILTER_CRT.CUST_ROLE_TYP_CD  )
SELECT * 
from CR