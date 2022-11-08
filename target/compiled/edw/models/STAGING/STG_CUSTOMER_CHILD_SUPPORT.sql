---- SRC LAYER ----
WITH
SRC_CCS as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_CHILD_SUPPORT ),
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER ),
SRC_CN as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_NAME ),
SRC_WPT as ( SELECT *     from     DEV_VIEWS.PCMP.WITHHOLDING_PERIOD_TYPE ),
//SRC_CCS as ( SELECT *     from     CUSTOMER_CHILD_SUPPORT) ,
//SRC_C as ( SELECT *     from     CUSTOMER) ,
//SRC_CN as ( SELECT *     from     CUSTOMER_NAME) ,
//SRC_WPT as ( SELECT *     from     WITHHOLDING_PERIOD_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CCS as ( SELECT 
		  CUST_CHLD_SUPT_ID                                  AS                                  CUST_CHLD_SUPT_ID 
		, CUST_ID_PRSN                                       AS                                       CUST_ID_PRSN 
		, CUST_ID_ISSR                                       AS                                       CUST_ID_ISSR 
		, cast( CUST_CHLD_SUPT_RECV_DT as DATE )             AS                             CUST_CHLD_SUPT_RECV_DT 
		, upper( TRIM( CUST_CHLD_SUPT_CASE_NO ) )            AS                             CUST_CHLD_SUPT_CASE_NO 
		, cast( CUST_CHLD_SUPT_EFF_DT as DATE )              AS                              CUST_CHLD_SUPT_EFF_DT 
		, cast( CUST_CHLD_SUPT_END_DT as DATE )              AS                              CUST_CHLD_SUPT_END_DT 
		, CUST_CHLD_SUPT_WH_AMT                              AS                              CUST_CHLD_SUPT_WH_AMT 
		, upper( TRIM( WH_PRD_TYP_CD ) )                     AS                                      WH_PRD_TYP_CD 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( CUST_CHLD_SUPT_VOID_IND )                   AS                            CUST_CHLD_SUPT_VOID_IND 
		from SRC_CCS
            ),
LOGIC_C as ( SELECT 
		  upper( TRIM( CUST_NO ) )                           AS                                            CUST_NO 
		, CUST_ID                                            AS                                            CUST_ID 
		from SRC_C
            ),
LOGIC_CN as ( SELECT 
		  upper( TRIM( CUST_NM_NM ) )                        AS                                         CUST_NM_NM 
		, CUST_ID                                            AS                                            CUST_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		, upper( TRIM( CUST_NM_TYP_CD ) )                    AS                                     CUST_NM_TYP_CD 
		, cast( CUST_NM_END_DT as DATE )                     AS                                     CUST_NM_END_DT 
		from SRC_CN
            ),
LOGIC_WPT as ( SELECT 
		  upper( TRIM( WH_PRD_TYP_NM ) )                     AS                                      WH_PRD_TYP_NM 
		, upper( TRIM( WH_PRD_TYP_CD ) )                     AS                                      WH_PRD_TYP_CD 
		, upper( WH_PRD_TYP_VOID_IND )                       AS                                WH_PRD_TYP_VOID_IND 
		from SRC_WPT
            )

---- RENAME LAYER ----
,

RENAME_CCS as ( SELECT 
		  CUST_CHLD_SUPT_ID                                  as                                  CUST_CHLD_SUPT_ID
		, CUST_ID_PRSN                                       as                                       CUST_ID_PRSN
		, CUST_ID_ISSR                                       as                                       CUST_ID_ISSR
		, CUST_CHLD_SUPT_RECV_DT                             as                             CUST_CHLD_SUPT_RECV_DT
		, CUST_CHLD_SUPT_CASE_NO                             as                             CUST_CHLD_SUPT_CASE_NO
		, CUST_CHLD_SUPT_EFF_DT                              as                              CUST_CHLD_SUPT_EFF_DT
		, CUST_CHLD_SUPT_END_DT                              as                              CUST_CHLD_SUPT_END_DT
		, CUST_CHLD_SUPT_WH_AMT                              as                              CUST_CHLD_SUPT_WH_AMT
		, WH_PRD_TYP_CD                                      as                                      WH_PRD_TYP_CD
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, CUST_CHLD_SUPT_VOID_IND                            as                            CUST_CHLD_SUPT_VOID_IND 
				FROM     LOGIC_CCS   ), 
RENAME_C as ( SELECT 
		  CUST_NO                                            as                                            CUST_NO
		, CUST_ID                                            as                                          C_CUST_ID 
				FROM     LOGIC_C   ), 
RENAME_CN as ( SELECT 
		  CUST_NM_NM                                         as                            CUST_CHLD_SUPT_AGNCY_NM
		, CUST_ID                                            as                                         CN_CUST_ID
		, VOID_IND                                           as                                        CN_VOID_IND
		, CUST_NM_TYP_CD                                     as                                     CUST_NM_TYP_CD
		, CUST_NM_END_DT                                     as                                     CUST_NM_END_DT 
				FROM     LOGIC_CN   ), 
RENAME_WPT as ( SELECT 
		  WH_PRD_TYP_NM                                      as                                      WH_PRD_TYP_NM
		, WH_PRD_TYP_CD                                      as                                  WPT_WH_PRD_TYP_CD
		, WH_PRD_TYP_VOID_IND                                as                                WH_PRD_TYP_VOID_IND 
				FROM     LOGIC_WPT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CCS                            as ( SELECT * from    RENAME_CCS   ),
FILTER_C                              as ( SELECT * from    RENAME_C   ),
FILTER_WPT                            as ( SELECT * from    RENAME_WPT 
				WHERE WH_PRD_TYP_VOID_IND = 'N'  ),
FILTER_CN                             as ( SELECT * from    RENAME_CN 
				WHERE CUST_NM_TYP_CD = 'BUSN_LEGAL_NM' AND CN_VOID_IND = 'N' AND CUST_NM_END_DT IS NULL  ),

---- JOIN LAYER ----

CCS as ( SELECT * 
				FROM  FILTER_CCS
				LEFT JOIN FILTER_C ON  FILTER_CCS.CUST_ID_PRSN =  FILTER_C.C_CUST_ID 
								LEFT JOIN FILTER_WPT ON  FILTER_CCS.WH_PRD_TYP_CD =  FILTER_WPT.WPT_WH_PRD_TYP_CD 
								LEFT JOIN FILTER_CN ON  FILTER_CCS.CUST_ID_ISSR =  FILTER_CN.CN_CUST_ID  )
SELECT * 
from CCS