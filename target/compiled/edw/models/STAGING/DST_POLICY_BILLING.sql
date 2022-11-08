---- SRC LAYER ----
WITH
SRC_PAY as ( SELECT *     from     STAGING.STG_POLICY_CONTROL_ELEMENT ),
SRC_FREQ as ( SELECT *     from     STAGING.STG_POLICY_CONTROL_ELEMENT ),
SRC_AUDIT as ( SELECT *     from     STAGING.STG_POLICY_CONTROL_ELEMENT ),
SRC_EMP_LS as ( SELECT *     from     STAGING.STG_POLICY_CONTROL_ELEMENT ),
SRC_PROG as ( SELECT *     from     STAGING.STG_POLICY_PROFILE ),
SRC_PREM1 as ( SELECT *     from     STAGING.STG_PREMIUM_PERIOD ),
SRC_PREM2 as ( SELECT *     from     STAGING.STG_PREMIUM_PERIOD ),
SRC_PREM3 as ( SELECT *     from     STAGING.STG_PREMIUM_PERIOD ),
SRC_WCP1 as ( SELECT *     from     STAGING.STG_WC_COVERAGE_PREMIUM ),
SRC_WCP2 as ( SELECT *     from     STAGING.STG_WC_COVERAGE_PREMIUM ),
SRC_WCP3 as ( SELECT *     from     STAGING.STG_WC_COVERAGE_PREMIUM ),
SRC_PAUD as ( SELECT *     from     STAGING.STG_POLICY_AUDIT ),
SRC_RPT as ( SELECT *     from     STAGING.STG_PAYROLL_REPORT ),
SRC_PFT as ( SELECT *     from     STAGING.STG_POLICY_FINANCIAL_TRANSACTION ),
//SRC_PAY as ( SELECT *     from     STG_POLICY_CONTROL_ELEMENT) ,
//SRC_FREQ as ( SELECT *     from     STG_POLICY_CONTROL_ELEMENT) ,
//SRC_AUDIT as ( SELECT *     from     STG_POLICY_CONTROL_ELEMENT) ,
//SRC_EMP_LS as ( SELECT *     from     STG_POLICY_CONTROL_ELEMENT) ,
//SRC_PROG as ( SELECT *     from     STG_POLICY_PROFILE) ,
//SRC_PREM1 as ( SELECT *     from     STG_PREMIUM_PERIOD),
//SRC_PREM2 as ( SELECT *     from     STG_PREMIUM_PERIOD),
//SRC_PREM3 as ( SELECT *     from     STG_PREMIUM_PERIOD),
//SRC_WCP1 as ( SELECT *     from     STG_WC_COVERAGE_PREMIUM),
//SRC_WCP2 as ( SELECT *     from     STG_WC_COVERAGE_PREMIUM),
//SRC_WCP3 as ( SELECT *     from     STG_WC_COVERAGE_PREMIUM),
//SRC_PAUD as ( SELECT *     from     STG_POLICY_AUDIT),
//SRC_RPT as ( SELECT *     from     STG_PAYROLL_REPORT),
//SRC_PFT as ( SELECT *     from     STG_POLICY_FINANCIAL_TRANSACTION),

---- LOGIC LAYER ----

LOGIC_PAY as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
        , CTL_ELEM_TYP_CD                                    as                                    CTL_ELEM_TYP_CD 
        , CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD 
		, CTL_ELEM_SUB_TYP_NM                                as                                CTL_ELEM_SUB_TYP_NM 
		, VOID_IND                                           as                                           VOID_IND
		from SRC_PAY
            ),

LOGIC_FREQ as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
        , CTL_ELEM_TYP_CD                                    as                                    CTL_ELEM_TYP_CD
        , CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD 
		, CTL_ELEM_SUB_TYP_NM                                as                                CTL_ELEM_SUB_TYP_NM 
		, VOID_IND                                           as                                           VOID_IND
		from SRC_FREQ
            ),

LOGIC_AUDIT as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
        , CTL_ELEM_TYP_CD                                    as                                    CTL_ELEM_TYP_CD
		, CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD 
		, CTL_ELEM_SUB_TYP_NM                                as                                CTL_ELEM_SUB_TYP_NM 
		, VOID_IND                                           as                                           VOID_IND
		from SRC_AUDIT
            ),

LOGIC_EMP_LS as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
        , CTL_ELEM_TYP_CD                                    as                                    CTL_ELEM_TYP_CD
        , CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD 
		, CTL_ELEM_SUB_TYP_NM	        					 as                                CTL_ELEM_SUB_TYP_NM 
		, VOID_IND                                           as                                           VOID_IND
		from SRC_EMP_LS
            ),

LOGIC_PROG as ( SELECT 
		   PLCY_PRFL_ID                                      as                                       PLCY_PRFL_ID       
        ,  PLCY_PRD_ID                                       as                                        PLCY_PRD_ID
        ,  PRFL_STMT_ID                                      as                                       PRFL_STMT_ID
        ,  PLCY_PRFL_ANSW_TEXT                               as                             POLICY_15K_PROGRAM_IND
		,  PLCY_PRFL_VOID_IND                                as                                 PLCY_PRFL_VOID_IND
		from SRC_PROG
            ),

LOGIC_PREM1 as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
        , PREM_PRD_ID                                        as                                        PREM_PRD_ID
		, PREM_TYP_CD                                        as                                        PREM_TYP_CD 
		, VOID_IND                                           as                                           VOID_IND
		from SRC_PREM1
            ),

LOGIC_PREM2 as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
        , PREM_PRD_ID                                        as                                        PREM_PRD_ID
		, PREM_TYP_CD                                        as                                        PREM_TYP_CD 
		, VOID_IND                                           as                                           VOID_IND
		from SRC_PREM2
            ),

LOGIC_PREM3 as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
        , PREM_PRD_ID                                        as                                        PREM_PRD_ID
        , PREM_TYP_CD                                        as                                        PREM_TYP_CD 
		, VOID_IND                                           as                                           VOID_IND
		from SRC_PREM3
            ),

LOGIC_WCP1 as ( SELECT 
		  PREM_PRD_ID                                        as                                        PREM_PRD_ID
		 , WC_COV_PREM_BS_VAL                                as                                 WC_COV_PREM_BS_VAL
         , WC_COV_PREM_VOID_IND                              as                               WC_COV_PREM_VOID_IND 
		from SRC_WCP1
            ),

LOGIC_WCP2 as ( SELECT 
		  PREM_PRD_ID                                        as                                        PREM_PRD_ID
		, WC_COV_PREM_BS_VAL                                 as                                 WC_COV_PREM_BS_VAL
        , WC_COV_PREM_VOID_IND                               as                               WC_COV_PREM_VOID_IND 
		from SRC_WCP2
            ),

LOGIC_WCP3 as ( SELECT 
		  PREM_PRD_ID                                        as                                        PREM_PRD_ID
		, WC_COV_PREM_BS_VAL                                 as                                 WC_COV_PREM_BS_VAL
        , WC_COV_PREM_VOID_IND                               as                               WC_COV_PREM_VOID_IND 
		from SRC_WCP3
            ),

LOGIC_PAUD as ( SELECT 
		  PLCY_PRD_AUDT_DTL_ID                               as                               PLCY_PRD_AUDT_DTL_ID
        , PLCY_AUDT_TYP_CD                                   as                                   PLCY_AUDT_TYP_CD       
		, PLCY_PRD_AUDT_DTL_EFF_DT                           as                           PLCY_PRD_AUDT_DTL_EFF_DT 
		, VOID_IND                                           as                                           VOID_IND
		from SRC_PAUD
            ),

LOGIC_RPT as ( SELECT 
		  PYRL_RPT_ID                                        as                                        PYRL_RPT_ID
        , PYRL_RPT_EST_RPT_IND                               as                               PYRL_RPT_EST_RPT_IND
		, VOID_IND                                           as                                           VOID_IND
		from SRC_RPT
            ),

LOGIC_PFT as ( SELECT
          PLCY_PRD_ID                                        as                                        PLCY_PRD_ID    
		, PYRL_RPT_ID                                        as                                        PYRL_RPT_ID
		, PLCY_PRD_AUDT_DTL_ID                               as                               PLCY_PRD_AUDT_DTL_ID
		from SRC_PFT
            )
            
---- RENAME LAYER ----
,
RENAME_PAY as ( SELECT 
		  PLCY_PRD_ID                                        as                                    PAY_PLCY_PRD_ID
        , CTL_ELEM_TYP_CD                                    as                                PAY_CTL_ELEM_TYP_CD 
        , CTL_ELEM_SUB_TYP_CD                                as                             PAYMENT_PLAN_TYPE_CODE
		, CTL_ELEM_SUB_TYP_NM                                as                             PAYMENT_PLAN_TYPE_DESC 
		, VOID_IND                                           as                                       PAY_VOID_IND
				FROM     LOGIC_PAY   ), 
RENAME_FREQ as ( SELECT 
		  PLCY_PRD_ID                                        as                                   FREQ_PLCY_PRD_ID
        , CTL_ELEM_TYP_CD                                    as                               FREQ_CTL_ELEM_TYP_CD 
        , CTL_ELEM_SUB_TYP_CD                                as                      REPORTING_FREQUENCY_TYPE_CODE
		, CTL_ELEM_SUB_TYP_NM                                as                      REPORTING_FREQUENCY_TYPE_DESC 
		, VOID_IND                                           as                                      FREQ_VOID_IND
				FROM     LOGIC_FREQ   ), 
RENAME_AUDIT as ( SELECT 
		  PLCY_PRD_ID                                        as                                  AUDIT_PLCY_PRD_ID
        , CTL_ELEM_TYP_CD                                    as                              AUDIT_CTL_ELEM_TYP_CD
		, CTL_ELEM_SUB_TYP_CD                                as                                    AUDIT_TYPE_CODE
		, CTL_ELEM_SUB_TYP_NM                                as                                    AUDIT_TYPE_DESC 
		, VOID_IND                                           as                                     AUDIT_VOID_IND
				FROM     LOGIC_AUDIT   ), 
RENAME_EMP_LS as ( SELECT 
		  PLCY_PRD_ID                                        as                                 EMP_LS_PLCY_PRD_ID
        , CTL_ELEM_TYP_CD                                    as                             EMP_LS_CTL_ELEM_TYP_CD
        , CTL_ELEM_SUB_TYP_CD                                as                         EMPLOYEE_LEASING_TYPE_CODE
		, CTL_ELEM_SUB_TYP_NM                                as                         EMPLOYEE_LEASING_TYPE_DESC 
		, VOID_IND                                           as                                    EMP_LS_VOID_IND
				FROM     LOGIC_EMP_LS   ), 
RENAME_PROG as ( SELECT 
		  PLCY_PRFL_ID                                       as                                       PLCY_PRFL_ID  
        , PLCY_PRD_ID                                        as                                   PROG_PLCY_PRD_ID
        , PRFL_STMT_ID                                       as                                       PRFL_STMT_ID
		, POLICY_15K_PROGRAM_IND                             as                             POLICY_15K_PROGRAM_IND 
		, PLCY_PRFL_VOID_IND                                 as                                 PLCY_PRFL_VOID_IND
				FROM     LOGIC_PROG   ),
RENAME_PREM1 as ( SELECT 
		  PLCY_PRD_ID                                        as                                  PREM1_PLCY_PRD_ID
        , PREM_PRD_ID                                        as                                        PREM_PRD_ID
		, PREM_TYP_CD                                        as                                  PREM1_PREM_TYP_CD 
		, VOID_IND                                           as                                     PREM1_VOID_IND
				FROM    LOGIC_PREM1   ),
RENAME_PREM2 as ( SELECT 
		  PLCY_PRD_ID                                        as                                  PREM2_PLCY_PRD_ID
        , PREM_PRD_ID                                        as                                        PREM_PRD_ID
		, PREM_TYP_CD                                        as                                  PREM2_PREM_TYP_CD 
		, VOID_IND                                           as                                     PREM2_VOID_IND
				FROM    LOGIC_PREM2   ),
RENAME_PREM3 as ( SELECT 
		  PLCY_PRD_ID                                        as                                  PREM3_PLCY_PRD_ID
        , PREM_PRD_ID                                        as                                        PREM_PRD_ID
		, PREM_TYP_CD                                        as                                  PREM3_PREM_TYP_CD 
		, VOID_IND                                           as                                     PREM3_VOID_IND
				FROM    LOGIC_PREM3   ),
RENAME_WCP1 as ( SELECT 
		  PREM_PRD_ID                                        as                                   WCP1_PREM_PRD_ID
		, WC_COV_PREM_BS_VAL                                 as                                 WC_COV_PREM_BS_VAL
        , WC_COV_PREM_VOID_IND                               as                          WCP1_WC_COV_PREM_VOID_IND 
				FROM    LOGIC_WCP1   ),
RENAME_WCP2 as ( SELECT 
		  PREM_PRD_ID                                        as                                   WCP2_PREM_PRD_ID
		, WC_COV_PREM_BS_VAL                                 as                                 WC_COV_PREM_BS_VAL
        , WC_COV_PREM_VOID_IND                               as                          WCP2_WC_COV_PREM_VOID_IND 
				FROM    LOGIC_WCP2   ),
RENAME_WCP3 as ( SELECT 
		  PREM_PRD_ID                                        as                                   WCP3_PREM_PRD_ID
		, WC_COV_PREM_BS_VAL                                 as                                 WC_COV_PREM_BS_VAL
        , WC_COV_PREM_VOID_IND                               as                          WCP3_WC_COV_PREM_VOID_IND 
				FROM    LOGIC_WCP3   ),
RENAME_PAUD as ( SELECT 
		  PLCY_PRD_AUDT_DTL_ID                               as                          PAUD_PLCY_PRD_AUDT_DTL_ID
        , PLCY_AUDT_TYP_CD                                   as                                   PLCY_AUDT_TYP_CD
        , PLCY_PRD_AUDT_DTL_EFF_DT                           as                           PLCY_PRD_AUDT_DTL_EFF_DT 
		, VOID_IND                                           as                                      PAUD_VOID_IND
				FROM    LOGIC_PAUD  ),
RENAME_RPT as ( SELECT 
		  PYRL_RPT_ID                                        as                                    RPT_PYRL_RPT_ID
        , PYRL_RPT_EST_RPT_IND                               as                               PYRL_RPT_EST_RPT_IND
        , VOID_IND                                           as                                       RPT_VOID_IND
				FROM    LOGIC_RPT   ),
RENAME_PFT as ( SELECT 
          PLCY_PRD_ID                                        as                                        PLCY_PRD_ID    
		, PYRL_RPT_ID                                        as                                        PYRL_RPT_ID
		, PLCY_PRD_AUDT_DTL_ID                               as                               PLCY_PRD_AUDT_DTL_ID
				FROM    LOGIC_PFT   )               
                                
---- FILTER LAYER (uses aliases) ----
,
FILTER_PFT                            as ( SELECT * from    RENAME_PFT   ),
FILTER_PAY                            as ( SELECT * from    RENAME_PAY 
                                            WHERE PAY_CTL_ELEM_TYP_CD = 'PYMT_PLN' AND PAY_VOID_IND = 'N'  ),
FILTER_FREQ                           as ( SELECT * from    RENAME_FREQ 
                                            WHERE FREQ_CTL_ELEM_TYP_CD = 'RPT_FREQ' AND FREQ_VOID_IND = 'N'  ),
FILTER_AUDIT                          as ( SELECT * from    RENAME_AUDIT 
                                            WHERE AUDIT_CTL_ELEM_TYP_CD = 'AUDT_TYP' AND AUDIT_VOID_IND = 'N'  ),
FILTER_EMP_LS                         as ( SELECT * from    RENAME_EMP_LS 
                                            WHERE EMP_LS_CTL_ELEM_TYP_CD = 'EMP_LS_PLCY_TYP' AND EMP_LS_VOID_IND = 'N'  ),
FILTER_PROG                           as ( SELECT * from    RENAME_PROG 
                                            WHERE PLCY_PRFL_VOID_IND = 'N' AND PRFL_STMT_ID = 6303027  ),
FILTER_PREM1                          as ( SELECT * from    RENAME_PREM1 
                                            WHERE PREM1_VOID_IND = 'N' AND PREM1_PREM_TYP_CD = 'E'  ),
FILTER_WCP1                           as ( SELECT * from RENAME_WCP1 
                                            WHERE WCP1_WC_COV_PREM_VOID_IND = 'N'  ),
FILTER_PREM2                          as ( SELECT * from    RENAME_PREM2 
                                            WHERE PREM2_VOID_IND = 'N' AND PREM2_PREM_TYP_CD = 'R'  ),
FILTER_WCP2                           as ( SELECT * from  RENAME_WCP2
                                            WHERE WCP2_WC_COV_PREM_VOID_IND = 'N'  ),
FILTER_PREM3                          as ( SELECT * from    RENAME_PREM3 
                                            WHERE PREM3_VOID_IND = 'N' AND PREM3_PREM_TYP_CD = 'A'  ),
FILTER_WCP3                           as ( SELECT * from  RENAME_WCP3
                                            WHERE WCP3_WC_COV_PREM_VOID_IND = 'N'  ),
FILTER_PAUD                           as ( SELECT * from    RENAME_PAUD 
                                            WHERE PAUD_VOID_IND = 'N' AND PLCY_PRD_AUDT_DTL_EFF_DT >= '2015-01-01'  ),
FILTER_RPT                            as ( SELECT * from    RENAME_RPT 
                                            WHERE RPT_VOID_IND = 'N')

---- JOIN LAYER ----
,
PREM1 as ( SELECT FILTER_PREM1.PREM1_PLCY_PRD_ID, sum(FILTER_WCP1.WC_COV_PREM_BS_VAL) AS sum_WCP1_WC_COV_PREM_BS_VAL
				FROM  FILTER_PREM1
				LEFT JOIN FILTER_WCP1 ON  FILTER_PREM1.PREM_PRD_ID =  FILTER_WCP1.WCP1_PREM_PRD_ID
                GROUP BY FILTER_PREM1.PREM1_PLCY_PRD_ID  ),
PREM2 as ( SELECT FILTER_PREM2.PREM2_PLCY_PRD_ID, sum(FILTER_WCP2.WC_COV_PREM_BS_VAL) AS sum_WCP2_WC_COV_PREM_BS_VAL
				FROM  FILTER_PREM2
				LEFT JOIN FILTER_WCP2 ON  FILTER_PREM2.PREM_PRD_ID =  FILTER_WCP2.WCP2_PREM_PRD_ID
                GROUP BY FILTER_PREM2.PREM2_PLCY_PRD_ID  ),
PREM3 as ( SELECT FILTER_PREM3.PREM3_PLCY_PRD_ID, sum(FILTER_WCP3.WC_COV_PREM_BS_VAL) AS sum_WCP3_WC_COV_PREM_BS_VAL 
				FROM  FILTER_PREM3
				LEFT JOIN FILTER_WCP3 ON  FILTER_PREM3.PREM_PRD_ID =  FILTER_WCP3.WCP3_PREM_PRD_ID
                GROUP BY FILTER_PREM3.PREM3_PLCY_PRD_ID  ),
PFT as ( SELECT * 
				FROM  FILTER_PFT
				LEFT JOIN FILTER_PAY ON  FILTER_PFT.PLCY_PRD_ID =  FILTER_PAY.PAY_PLCY_PRD_ID 
								LEFT JOIN FILTER_FREQ ON  FILTER_PFT.PLCY_PRD_ID =  FILTER_FREQ.FREQ_PLCY_PRD_ID 
								LEFT JOIN FILTER_AUDIT ON  FILTER_PFT.PLCY_PRD_ID =  FILTER_AUDIT.AUDIT_PLCY_PRD_ID 
								LEFT JOIN FILTER_EMP_LS ON  FILTER_PFT.PLCY_PRD_ID =  FILTER_EMP_LS.EMP_LS_PLCY_PRD_ID 
								LEFT JOIN FILTER_PROG ON  FILTER_PFT.PLCY_PRD_ID =  FILTER_PROG.PROG_PLCY_PRD_ID 
						LEFT JOIN PREM1 ON  FILTER_PFT.PLCY_PRD_ID = PREM1.PREM1_PLCY_PRD_ID 
						LEFT JOIN PREM2 ON  FILTER_PFT.PLCY_PRD_ID = PREM2.PREM2_PLCY_PRD_ID 
						LEFT JOIN PREM3 ON  FILTER_PFT.PLCY_PRD_ID = PREM3.PREM3_PLCY_PRD_ID 
								LEFT JOIN FILTER_PAUD ON  FILTER_PFT.PLCY_PRD_AUDT_DTL_ID =  FILTER_PAUD.PAUD_PLCY_PRD_AUDT_DTL_ID 
								LEFT JOIN FILTER_RPT ON  FILTER_PFT.PYRL_RPT_ID =  FILTER_RPT.RPT_PYRL_RPT_ID  )

----ETL LAYER----                                                            
,
ETL AS (SELECT
 		  CASE WHEN PAYMENT_PLAN_TYPE_CODE IS NULL THEN 'N/A'
		       ELSE PAYMENT_PLAN_TYPE_CODE END AS PAYMENT_PLAN_TYPE_CODE
		, CASE WHEN PAYMENT_PLAN_TYPE_DESC IS NULL THEN 'NO PAYMENT PLAN'
		       ELSE PAYMENT_PLAN_TYPE_DESC END AS PAYMENT_PLAN_TYPE_DESC
		, CASE WHEN REPORTING_FREQUENCY_TYPE_CODE IS NULL THEN 'N/A'
		       ELSE REPORTING_FREQUENCY_TYPE_CODE END AS REPORTING_FREQUENCY_TYPE_CODE
		, CASE WHEN REPORTING_FREQUENCY_TYPE_DESC IS NULL THEN 'NO REPORTING FREQUENCY'
		       ELSE REPORTING_FREQUENCY_TYPE_DESC END AS REPORTING_FREQUENCY_TYPE_DESC
		, CASE WHEN AUDIT_TYPE_CODE IS NULL THEN 'N/A'
		       ELSE AUDIT_TYPE_CODE END AS AUDIT_TYPE_CODE
		, CASE WHEN AUDIT_TYPE_DESC IS NULL THEN 'NO AUDIT TYPE'
		       ELSE AUDIT_TYPE_DESC END AS AUDIT_TYPE_DESC
		, CASE WHEN EMPLOYEE_LEASING_TYPE_CODE IS NULL THEN 'N/A'
		       ELSE EMPLOYEE_LEASING_TYPE_CODE END AS EMPLOYEE_LEASING_TYPE_CODE
		, CASE WHEN EMPLOYEE_LEASING_TYPE_DESC IS NULL THEN 'NO EMPLOYEE LEASING TYPE'
		       ELSE EMPLOYEE_LEASING_TYPE_DESC END AS EMPLOYEE_LEASING_TYPE_DESC
		, CASE WHEN POLICY_15K_PROGRAM_IND = 'YES' THEN 'Y' 
		       ELSE 'N' END    POLICY_15K_PROGRAM_IND
        , CASE WHEN sum_WCP1_WC_COV_PREM_BS_VAL = 0 THEN 'Y' ELSE 'N' END AS ESTIMATED_ZERO_PAYROLL_IND
        , CASE WHEN sum_WCP3_WC_COV_PREM_BS_VAL = 0 THEN 'Y'  
               WHEN sum_WCP3_WC_COV_PREM_BS_VAL IS NULL AND sum_WCP2_WC_COV_PREM_BS_VAL =0 THEN 'Y'
               ELSE 'N' END AS REPORTED_ZERO_PAYROLL_IND
        , CASE WHEN PYRL_RPT_EST_RPT_IND IS NOT NULL THEN PYRL_RPT_EST_RPT_IND
               WHEN PLCY_AUDT_TYP_CD in ('ETU', 'EST') THEN 'Y' 
               ELSE 'N' END AS ESTIMATED_PREMIUM_IND 
from PFT )


SELECT DISTINCT 
md5(cast(
    
    coalesce(cast(PAYMENT_PLAN_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(REPORTING_FREQUENCY_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(AUDIT_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(EMPLOYEE_LEASING_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(POLICY_15K_PROGRAM_IND as 
    varchar
), '') || '-' || coalesce(cast(ESTIMATED_ZERO_PAYROLL_IND as 
    varchar
), '') || '-' || coalesce(cast(REPORTED_ZERO_PAYROLL_IND as 
    varchar
), '') || '-' || coalesce(cast(ESTIMATED_PREMIUM_IND as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
, *
FROM ETL