---- SRC LAYER ----
WITH
SRC_PFT_BILL       as ( SELECT *     FROM     STAGING.STG_POLICY_FINANCIAL_TRANSACTION ),
SRC_PFTA           as ( SELECT *     FROM     STAGING.STG_POLICY_FINANCIAL_TRANSACTION_APPLIED ),
SRC_PFT_PAY        as ( SELECT *     FROM     STAGING.STG_POLICY_FINANCIAL_TRANSACTION ),
SRC_CN             as ( SELECT *     FROM     STAGING.STG_CUSTOMER_NAME ),
SRC_U              as ( SELECT *     FROM     STAGING.STG_USERS ),
//SRC_PFT_BILL       as ( SELECT *     FROM     STG_POLICY_FINANCIAL_TRANSACTION) ,
//SRC_PFTA           as ( SELECT *     FROM     STG_POLICY_FINANCIAL_TRANSACTION_APPLIED) ,
//SRC_PFT_PAY        as ( SELECT *     FROM     STG_POLICY_FINANCIAL_TRANSACTION) ,
//SRC_CN             as ( SELECT *     FROM     STG_CUSTOMER_NAME) ,
//SRC_U              as ( SELECT *     FROM     STG_USERS) ,

----- LOGIC LAYER ----


LOGIC_PFT_BILL as ( SELECT 
		  TRIM( PLCY_NO )                                    as                                            PLCY_NO 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, PFT_ID                                             as                                             PFT_ID
        , FNCL_TRAN_TYP_ID                                   AS                                      FNCL_TRAN_TYP_ID           
		FROM SRC_PFT_BILL
            ),

LOGIC_PFTA as ( SELECT 
		  PFTA_ID                                            as                                            PFTA_ID 
		, PFTA_DT                                            as                                            PFTA_DT 
		, PFTA_AMT                                           as                                           PFTA_AMT 
		, PFT_ID_APLD_TO                                     as                                     PFT_ID_APLD_TO 
		, PFT_ID_APLD_FR                                     as                                     PFT_ID_APLD_FR
		, PFT_ID_RVRS                                        as                                        PFT_ID_RVRS
		FROM SRC_PFTA
            ),

LOGIC_PFT_PAY as ( SELECT 
		  PFT_ID                                             as                                             PFT_ID 
		, PFT_DT                                             as                                             PFT_DT 
		, PFT_AMT                                            as                                            PFT_AMT 
		, TRIM( FNCL_TRAN_TYP_NM )                           as                                   FNCL_TRAN_TYP_NM 
		, TRIM( FNCL_TRAN_SUB_TYP_NM )                       as                               FNCL_TRAN_SUB_TYP_NM 
		, TRIM( PAY_TRK_SRC_TYP_NM )                         as                                 PAY_TRK_SRC_TYP_NM 
		, TRIM( PFT_CMT )                                    as                                            PFT_CMT 
		, CUST_ID                                            as                                            CUST_ID 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		FROM SRC_PFT_PAY
            ),

LOGIC_CN as ( SELECT 
		  TRIM( CUST_NM_NM )                                 as                                         CUST_NM_NM 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_NM_TYP_CD )                             as                                     CUST_NM_TYP_CD 
		, CUST_NM_END_DT                                     as                                     CUST_NM_END_DT 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_CN
            ),

LOGIC_U as ( SELECT 
		  TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		, TRIM( USER_DRV_UPCS_NM )                           as                                   USER_DRV_UPCS_NM 
            ,USER_ID AS USER_ID
		FROM SRC_U
            )

---- RENAME LAYER ----
,

RENAME_PFT_BILL   as ( SELECT 
		  PLCY_NO                                            as                                            PLCY_NO
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, PFT_ID                                             as                                        BILL_PFT_ID 
         , FNCL_TRAN_TYP_ID                                  AS                                  FNCL_TRAN_TYP_ID
				FROM     LOGIC_PFT_BILL   ), 
RENAME_PFTA       as ( SELECT 
		  PFTA_ID                                            as                                            PFTA_ID
		, PFTA_DT                                            as                                            PFTA_DT
		, PFTA_AMT                                           as                                           PFTA_AMT
		, PFT_ID_APLD_TO                                     as                                     PFT_ID_APLD_TO
		, PFT_ID_APLD_FR                                     as                                     PFT_ID_APLD_FR
		, PFT_ID_RVRS                                        as                                        PFT_ID_RVRS 
				FROM     LOGIC_PFTA   ), 
RENAME_PFT_PAY    as ( SELECT 
		  PFT_ID                                             as                                             PFT_ID
		, PFT_DT                                             as                                             PFT_DT
		, PFT_AMT                                            as                                            PFT_AMT
		, FNCL_TRAN_TYP_NM                                   as                                   FNCL_TRAN_TYP_NM
		, FNCL_TRAN_SUB_TYP_NM                               as                               FNCL_TRAN_SUB_TYP_NM
		, PAY_TRK_SRC_TYP_NM                                 as                                 PAY_TRK_SRC_TYP_NM
		, PFT_CMT                                            as                                            PFT_CMT
		, CUST_ID                                            as                                            CUST_ID
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
				FROM     LOGIC_PFT_PAY   ), 
RENAME_CN         as ( SELECT 
		  CUST_NM_NM                                         as                              PAID_BY_CUSTOMER_NAME
		, CUST_ID                                            as                                         CN_CUST_ID
		, CUST_NM_TYP_CD                                     as                                     CUST_NM_TYP_CD
		, CUST_NM_END_DT                                     as                                     CUST_NM_END_DT
		, VOID_IND                                           as                                        CN_VOID_IND 
				FROM     LOGIC_CN   ), 
RENAME_U          as ( SELECT 
		  USER_LGN_NM                                        as                             CREATE_USER_LOGIN_NAME
		, USER_DRV_UPCS_NM                                   as                                   CREATE_USER_NAME
                      ,USER_ID AS USER_ID
				FROM     LOGIC_U   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PFTA                           as ( SELECT * FROM    RENAME_PFTA   ),
FILTER_PFT_PAY                        as ( SELECT * FROM    RENAME_PFT_PAY   ),
FILTER_PFT_BILL                       as ( SELECT * FROM    RENAME_PFT_BILL 
                                            WHERE FNCL_TRAN_TYP_ID IN (7, 8, 10, 21, 22, 40, 6303001, 6303021, 6303023, 6303024, 6303025, 6303026, 6303027, 6303028, 6303030, 6303031, 6303032, 6303033, 6303034, 6303042, 6303043, 6303061, 6303062, 6303073, 6303093, 6303096, 6303097, 6303098, 6303099, 6303100, 6303101, 6303102, 6303103, 6303104, 6303105, 6303112, 6303113, 6303115, 6303122, 6303123, 6303124, 6303126, 6307001, 6307003, 6307005, 6307007, 6330206, 6330207, 6330208, 6330209, 6330210, 6332015, 6332018, 6333212, 6333228, 6333229, 6333230, 6333231, 6333232, 6333233)  ),
FILTER_U                              as ( SELECT * FROM    RENAME_U   ),
FILTER_CN                             as ( SELECT * FROM    RENAME_CN 
                                            WHERE CUST_NM_TYP_CD in ('BUSN_LEGAL_NM', 'PRSN_NM') AND CN_VOID_IND = 'N' AND CUST_NM_END_DT IS NULL  ),
FILTER_PFTA                           as ( SELECT * FROM    RENAME_PFTA   ),
FILTER_PFT_PAY                        as ( SELECT * FROM    RENAME_PFT_PAY   ),
FILTER_PFT_BILL                       as ( SELECT * FROM    RENAME_PFT_BILL 
                                            WHERE FNCL_TRAN_TYP_ID IN (7, 8, 10, 21, 22, 40, 6303001, 6303021, 6303023, 6303024, 6303025, 6303026, 6303027, 6303028, 6303030, 6303031, 6303032, 6303033, 6303034, 6303042, 6303043, 6303061, 6303062, 6303073, 6303093, 6303096, 6303097, 6303098, 6303099, 6303100, 6303101, 6303102, 6303103, 6303104, 6303105, 6303112, 6303113, 6303115, 6303122, 6303123, 6303124, 6303126, 6307001, 6307003, 6307005, 6307007, 6330206, 6330207, 6330208, 6330209, 6330210, 6332015, 6332018, 6333212, 6333228, 6333229, 6333230, 6333231, 6333232, 6333233)  ),
FILTER_U                              as ( SELECT * FROM    RENAME_U   ),
FILTER_CN                             as ( SELECT * FROM    RENAME_CN 
                                            WHERE CUST_NM_TYP_CD in ('BUSN_LEGAL_NM', 'PRSN_NM') AND CN_VOID_IND = 'N' AND CUST_NM_END_DT IS NULL  ), 

---- JOIN LAYER ----

PFT_PAY as ( SELECT * 
				FROM  FILTER_PFT_PAY
                                INNER JOIN FILTER_PFTA ON  FILTER_PFTA.PFT_ID_APLD_FR = FILTER_PFT_PAY.PFT_ID 
				                INNER JOIN FILTER_PFT_BILL ON  FILTER_PFTA.PFT_ID_APLD_TO = FILTER_PFT_BILL.BILL_PFT_ID
                                LEFT JOIN FILTER_U ON  FILTER_PFT_PAY.AUDIT_USER_ID_CREA =  FILTER_U.USER_ID
                                LEFT JOIN FILTER_CN ON  FILTER_PFT_PAY.CUST_ID =  FILTER_CN.CN_CUST_ID
								  ),
PFTA as ( SELECT * 
				FROM  FILTER_PFT_PAY
                                INNER JOIN FILTER_PFTA ON  FILTER_PFTA.PFT_ID_APLD_TO = FILTER_PFT_PAY.PFT_ID 
				                INNER JOIN FILTER_PFT_BILL ON  FILTER_PFTA.PFT_ID_APLD_FR = FILTER_PFT_BILL.BILL_PFT_ID
                                LEFT JOIN FILTER_U ON  FILTER_PFT_PAY.AUDIT_USER_ID_CREA =  FILTER_U.USER_ID
                                LEFT JOIN FILTER_CN ON  FILTER_PFT_PAY.CUST_ID =  FILTER_CN.CN_CUST_ID ),
-- ETL LAYER -----
 ETL AS (SELECT
 PLCY_NO
,PLCY_PRD_ID
,BILL_PFT_ID
,PFTA_ID
,PFTA_DT
,CASE WHEN PFT_AMT < 0 then (0 - PFTA_AMT)
WHEN PFT_AMT < 0 AND PFT_ID_RVRS IS NOT NULL THEN (0 - PFTA_AMT)
WHEN PFT_AMT < 0 AND FNCL_TRAN_TYP_NM in ('LEGACY REFUND') THEN (0 - PFTA_AMT)
ELSE PFTA_AMT END AS PFTA_AMT  
,PFT_ID
,PFT_DT
,PFT_AMT
,FNCL_TRAN_TYP_NM
,FNCL_TRAN_SUB_TYP_NM
,PAY_TRK_SRC_TYP_NM
,PFT_CMT
,CUST_ID
,PAID_BY_CUSTOMER_NAME
,AUDIT_USER_ID_CREA
,CREATE_USER_LOGIN_NAME
,CREATE_USER_NAME
FROM PFTA 

UNION ALL 

SELECT 
 PLCY_NO
,PLCY_PRD_ID
,BILL_PFT_ID
,PFTA_ID
,PFTA_DT
,CASE WHEN PFT_AMT < 0 AND PFT_ID_RVRS IS NOT NULL THEN PFTA_AMT
WHEN PFT_AMT < 0 THEN (0 - PFTA_AMT)
ELSE PFTA_AMT END AS PFTA_AMT
,PFT_ID
,PFT_DT
,PFT_AMT
,FNCL_TRAN_TYP_NM
,FNCL_TRAN_SUB_TYP_NM
,PAY_TRK_SRC_TYP_NM
,PFT_CMT
,CUST_ID
,PAID_BY_CUSTOMER_NAME
,AUDIT_USER_ID_CREA
,CREATE_USER_LOGIN_NAME
,CREATE_USER_NAME
FROM PFT_PAY )
,FINAL_ETL AS ( SELECT md5(cast(
    
    coalesce(cast(PFTA_ID as 
    varchar
), '') || '-' || coalesce(cast(BILL_PFT_ID as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY, * FROM ETL)

SELECT * FROM FINAL_ETL