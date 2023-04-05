

---- SRC LAYER ----
WITH
SRC_PP             as ( SELECT *     FROM     {{ ref( 'STG_POLICY_PERIOD' ) }} ),
SRC_CN             as ( SELECT *     FROM     {{ ref( 'STG_CUSTOMER_NAME' ) }} ),
SRC_PCE            as ( SELECT *     FROM     {{ ref( 'STG_CUSTOMER_NAME' ) }} ),
SRC_WCP1           as ( SELECT *     FROM     {{ ref( 'STG_WC_COVERAGE_PREMIUM' ) }} ),
SRC_WCP2           as ( SELECT *     FROM     {{ ref( 'STG_WC_COVERAGE_PREMIUM' ) }} ),
SRC_A1             as ( SELECT *     FROM     {{ ref( 'STG_EMPLOYER_QUESTIONNAIRE' ) }} ),
SRC_A2             as ( SELECT *     FROM     {{ ref( 'STG_EMPLOYER_QUESTIONNAIRE' ) }} ),
SRC_A3             as ( SELECT *     FROM     {{ ref( 'STG_EMPLOYER_QUESTIONNAIRE' ) }} ),
SRC_A4             as ( SELECT *     FROM     {{ ref( 'STG_EMPLOYER_QUESTIONNAIRE' ) }} ),
SRC_A5             as ( SELECT *     FROM     {{ ref( 'STG_EMPLOYER_QUESTIONNAIRE' ) }} ),
SRC_A6             as ( SELECT *     FROM     {{ ref( 'STG_EMPLOYER_QUESTIONNAIRE' ) }} ),
SRC_A7             as ( SELECT *     FROM     {{ ref( 'STG_EMPLOYER_QUESTIONNAIRE' ) }} ),
SRC_A8             as ( SELECT *     FROM     {{ ref( 'STG_EMPLOYER_QUESTIONNAIRE' ) }} ),
SRC_A9             as ( SELECT *     FROM     {{ ref( 'STG_EMPLOYER_QUESTIONNAIRE' ) }} ),
SRC_A10            as ( SELECT *     FROM     {{ ref( 'STG_EMPLOYER_QUESTIONNAIRE' ) }} ),
SRC_A11            as ( SELECT *     FROM     {{ ref( 'STG_EMPLOYER_QUESTIONNAIRE' ) }} ),
SRC_A12            as ( SELECT *     FROM     {{ ref( 'STG_EMPLOYER_QUESTIONNAIRE' ) }} ),
SRC_A13            as ( SELECT *     FROM     {{ ref( 'STG_EMPLOYER_QUESTIONNAIRE' ) }} ),
//SRC_PP             as ( SELECT *     FROM     STG_POLICY_PERIOD) ,
//SRC_CN             as ( SELECT *     FROM     STG_CUSTOMER_NAME) ,
//SRC_PCE            as ( SELECT *     FROM     STG_CUSTOMER_NAME) ,
//SRC_WCP1           as ( SELECT *     FROM     STG_WC_COVERAGE_PREMIUM) ,
//SRC_WCP2           as ( SELECT *     FROM     STG_WC_COVERAGE_PREMIUM) ,
//SRC_A1             as ( SELECT *     FROM     STG_EMPLOYER_QUESTIONNAIRE) ,
//SRC_A2             as ( SELECT *     FROM     STG_EMPLOYER_QUESTIONNAIRE) ,
//SRC_A3             as ( SELECT *     FROM     STG_EMPLOYER_QUESTIONNAIRE) ,
//SRC_A4             as ( SELECT *     FROM     STG_EMPLOYER_QUESTIONNAIRE) ,
//SRC_A5             as ( SELECT *     FROM     STG_EMPLOYER_QUESTIONNAIRE) ,
//SRC_A6             as ( SELECT *     FROM     STG_EMPLOYER_QUESTIONNAIRE) ,
//SRC_A7             as ( SELECT *     FROM     STG_EMPLOYER_QUESTIONNAIRE) ,
//SRC_A8             as ( SELECT *     FROM     STG_EMPLOYER_QUESTIONNAIRE) ,
//SRC_A9             as ( SELECT *     FROM     STG_EMPLOYER_QUESTIONNAIRE) ,
//SRC_A10            as ( SELECT *     FROM     STG_EMPLOYER_QUESTIONNAIRE) ,
//SRC_A11            as ( SELECT *     FROM     STG_EMPLOYER_QUESTIONNAIRE) ,
//SRC_A12            as ( SELECT *     FROM     STG_EMPLOYER_QUESTIONNAIRE) ,
//SRC_A13            as ( SELECT *     FROM     STG_EMPLOYER_QUESTIONNAIRE) ,

---- LOGIC LAYER ----


LOGIC_PP as ( SELECT 
		  PLCY_NO                                            as                                            PLCY_NO 
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT 
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT 
		FROM SRC_PP
            ),

LOGIC_CN as ( SELECT 
		  CUST_NM_DRV_UPCS_NM                                as                                CUST_NM_DRV_UPCS_NM 
		FROM SRC_CN
            ),

LOGIC_PCE as ( SELECT 
		  CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD 
		, CTL_ELEM_SUB_TYP_NM                                as                                CTL_ELEM_SUB_TYP_NM 
		FROM SRC_PCE
            ),

LOGIC_WCP1 as ( SELECT 
		  SUM(WC_COV_PREM_DRV_MNL_PREM_AMT) FOR PLCY_PRD_ID  as                                                    
		FROM SRC_WCP1
            ),

LOGIC_WCP2 as ( SELECT 
		  SUM(WC_COV_PREM_DRV_MNL_PREM_AMT) FOR PLCY_PRD_ID  as                                                    
		FROM SRC_WCP2
            ),

LOGIC_A1 as ( SELECT 
		  ANSWR_CRT_DTTM                                     as                                     ANSWR_CRT_DTTM 
		, ANSWR_TEXT                                         as                                         ANSWR_TEXT 
		, QSTN_TEXT                                          as                                          QSTN_TEXT 
		FROM SRC_A1
            ),

LOGIC_A2 as ( SELECT 
		  ANSWR_TEXT                                         as                                         ANSWR_TEXT 
		, QSTN_TEXT                                          as                                          QSTN_TEXT 
		FROM SRC_A2
            ),

LOGIC_A3 as ( SELECT 
		  ANSWR_TEXT                                         as                                         ANSWR_TEXT 
		, QSTN_TEXT                                          as                                          QSTN_TEXT 
		FROM SRC_A3
            ),

LOGIC_A4 as ( SELECT 
		  ANSWR_TEXT                                         as                                         ANSWR_TEXT 
		, QSTN_TEXT                                          as                                          QSTN_TEXT 
		FROM SRC_A4
            ),

LOGIC_A5 as ( SELECT 
		  ANSWR_TEXT                                         as                                         ANSWR_TEXT 
		, QSTN_TEXT                                          as                                          QSTN_TEXT 
		FROM SRC_A5
            ),

LOGIC_A6 as ( SELECT 
		  ANSWR_TEXT                                         as                                         ANSWR_TEXT 
		, QSTN_TEXT                                          as                                          QSTN_TEXT 
		FROM SRC_A6
            ),

LOGIC_A7 as ( SELECT 
		  ANSWR_TEXT                                         as                                         ANSWR_TEXT 
		, QSTN_TEXT                                          as                                          QSTN_TEXT 
		FROM SRC_A7
            ),

LOGIC_A8 as ( SELECT 
		  ANSWR_TEXT                                         as                                         ANSWR_TEXT 
		, QSTN_TEXT                                          as                                          QSTN_TEXT 
		FROM SRC_A8
            ),

LOGIC_A9 as ( SELECT 
		  ANSWR_TEXT                                         as                                         ANSWR_TEXT 
		, QSTN_TEXT                                          as                                          QSTN_TEXT 
		FROM SRC_A9
            ),

LOGIC_A10 as ( SELECT 
		  ANSWR_TEXT                                         as                                         ANSWR_TEXT 
		, QSTN_TEXT                                          as                                          QSTN_TEXT 
		FROM SRC_A10
            ),

LOGIC_A11 as ( SELECT 
		  ANSWR_TEXT                                         as                                         ANSWR_TEXT 
		, QSTN_TEXT                                          as                                          QSTN_TEXT 
		FROM SRC_A11
            ),

LOGIC_A12 as ( SELECT 
		  ANSWR_TEXT                                         as                                         ANSWR_TEXT 
		, QSTN_TEXT                                          as                                          QSTN_TEXT 
		FROM SRC_A12
            ),

LOGIC_A13 as ( SELECT 
		  ANSWR_TEXT                                         as                                         ANSWR_TEXT 
		, QSTN_TEXT                                          as                                          QSTN_TEXT 
		FROM SRC_A13
            )

---- RENAME LAYER ----
,

RENAME_PP         as ( SELECT 
		  PLCY_NO                                            as                                      POLICY_NUMBER
		, PLCY_PRD_EFF_DT                                    as                             POLICY_PERIOD_EFF_DATE
		, PLCY_PRD_END_DT                                    as                             POLICY_PERIOD_END_DATE 
				FROM     LOGIC_PP   ), 
RENAME_CN         as ( SELECT 
		  CUST_NM_DRV_UPCS_NM                                as                                      EMPLOYER_NAME 
				FROM     LOGIC_CN   ), 
RENAME_PCE        as ( SELECT 
		  CTL_ELEM_SUB_TYP_CD                                as                                   POLICY_TYPE_CODE
		, CTL_ELEM_SUB_TYP_NM                                as                                   POLICY_TYPE_DESC 
				FROM     LOGIC_PCE   ), 
RENAME_WCP1       as ( SELECT 
		  ESTIMATED_PREMIUM_AMOUNT                           as                           ESTIMATED_PREMIUM_AMOUNT 
				FROM     LOGIC_WCP1   ), 
RENAME_WCP2       as ( SELECT 
		  TRUE_UP_PREMIUM_AMOUNT                             as                             TRUE_UP_PREMIUM_AMOUNT 
				FROM     LOGIC_WCP2   ), 
RENAME_A1         as ( SELECT 
		  ANSWR_CRT_DTTM                                     as                        QUESTIONNAIRE_COMPLETED_DTM
		, ANSWR_TEXT                                         as                    ALL_YOU_NEED_TO_COMPLETE_ANSWER
		, QSTN_TEXT                                          as                  ALL_YOU_NEED_TO_COMPLETE_QUESTION 
				FROM     LOGIC_A1   ), 
RENAME_A2         as ( SELECT 
		  ANSWR_TEXT                                         as                            CEASE_OPERATIONS_ANSWER
		, QSTN_TEXT                                          as                          CEASE_OPERATIONS_QUESTION 
				FROM     LOGIC_A2   ), 
RENAME_A3         as ( SELECT 
		  ANSWR_TEXT                                         as                             SELL_OPERATIONS_ANSWER
		, QSTN_TEXT                                          as                           SELL_OPERATIONS_QUESTION 
				FROM     LOGIC_A3   ), 
RENAME_A4         as ( SELECT 
		  ANSWR_TEXT                                         as                              MERGE_BUSINESS_ANSWER
		, QSTN_TEXT                                          as                            MERGE_BUSINESS_QUESTION 
				FROM     LOGIC_A4   ), 
RENAME_A5         as ( SELECT 
		  ANSWR_TEXT                                         as                               MULTIPLE_FEIN_ANSWER
		, QSTN_TEXT                                          as                             MULTIPLE_FEIN_QUESTION 
				FROM     LOGIC_A5   ), 
RENAME_A6         as ( SELECT 
		  ANSWR_TEXT                                         as                 USED_PAYROLL_SERVICE_VENDOR_ANSWER
		, QSTN_TEXT                                          as               USED_PAYROLL_SERVICE_VENDOR_QUESTION 
				FROM     LOGIC_A6   ), 
RENAME_A7         as ( SELECT 
		  ANSWR_TEXT                                         as                         TEMP_SERVICE_AGENCY_ANSWER
		, QSTN_TEXT                                          as                       TEMP_SERVICE_AGENCY_QUESTION 
				FROM     LOGIC_A7   ), 
RENAME_A8         as ( SELECT 
		  ANSWR_TEXT                                         as                         PEO_LEASE_AGREEMENT_ANSWER
		, QSTN_TEXT                                          as                       PEO_LEASE_AGREEMENT_QUESTION 
				FROM     LOGIC_A8   ), 
RENAME_A9         as ( SELECT 
		  ANSWR_TEXT                                         as                SUBCONTRACT_LABOR_1099_FORMS_ANSWER
		, QSTN_TEXT                                          as              SUBCONTRACT_LABOR_1099_FORMS_QUESTION 
				FROM     LOGIC_A9   ), 
RENAME_A10        as ( SELECT 
		  ANSWR_TEXT                                         as                     OPERATIONS_OUTSIDE_OHIO_ANSWER
		, QSTN_TEXT                                          as                   OPERATIONS_OUTSIDE_OHIO_QUESTION 
				FROM     LOGIC_A10   ), 
RENAME_A11        as ( SELECT 
		  ANSWR_TEXT                                         as                       COVERAGE_OUTSIDE_OHIO_ANSWER
		, QSTN_TEXT                                          as                     COVERAGE_OUTSIDE_OHIO_QUESTION 
				FROM     LOGIC_A11   ), 
RENAME_A12        as ( SELECT 
		  ANSWR_TEXT                                         as                        CHANGE_IN_OPERATIONS_ANSWER
		, QSTN_TEXT                                          as                      CHANGE_IN_OPERATIONS_QUESTION 
				FROM     LOGIC_A12   ), 
RENAME_A13        as ( SELECT 
		  ANSWR_TEXT                                         as              REPORT_LESS_THAN_ESTIMATE_EXPLANATION
		, QSTN_TEXT                                          as                 REPORT_LESS_THAN_ESTIMATE_QUESTION 
				FROM     LOGIC_A13   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PP                             as ( SELECT * FROM    RENAME_PP 
                                            WHERE PLCY_PRD_EFF_DT >= '2018-01-01'  ),
FILTER_P                              as ( SELECT * FROM    RENAME_P 
                                            WHERE AGRE_TYP_CD = 'PLCY'  ),
FILTER_CN                             as ( SELECT * FROM    RENAME_CN 
                                            WHERE CUST_NM_TYP_CD = 'BUSN_LEGAL_NM' 
and VOID_IND = 'N' 
and CUST_NM_END_DT is null
(if more than one record per CUST_ID, select max(CUST_NM_ID)  ),
FILTER_PREM1                          as ( SELECT * FROM    RENAME_PREM1 
                                            WHERE VOID_IND = 'n' and PREM_TYP_CD = 'E'  ),
FILTER_WCP1                           as ( SELECT * FROM    RENAME_WCP1 
                                            WHERE WC_COV_PREM_VOID_IND = 'N'  ),
FILTER_PREM2                          as ( SELECT * FROM    RENAME_PREM2 
                                            WHERE VOID_IND = 'n' and PREM_TYP_CD = 'A'  ),
FILTER_WCP2                           as ( SELECT * FROM    RENAME_WCP2 
                                            WHERE WC_COV_PREM_VOID_IND = 'N'  ),
FILTER_PT                             as ( SELECT * FROM    RENAME_PT 
                                            WHERE VOID_IND = 'N' AND CTL_ELEM_TYP_CD = 'PLCY_TYP'  ),
FILTER_A1                             as ( SELECT * FROM    RENAME_A1 
                                            WHERE SRVC_OFRNG_CODE = 'TRUP' AND QSTN_CODE = 'ALL_RQR_DCM'  ),
FILTER_A2                             as ( SELECT * FROM    RENAME_A2 
                                            WHERE SRVC_OFRNG_CODE = 'TRUP' AND QSTN_CODE = 'CEASE_OPRTN'  ),
FILTER_A3                             as ( SELECT * FROM    RENAME_A3 
                                            WHERE SRVC_OFRNG_CODE = 'TRUP' AND QSTN_CODE = 'SELL_OPRTN'  ),
FILTER_A4                             as ( SELECT * FROM    RENAME_A4 
                                            WHERE SRVC_OFRNG_CODE = 'TRUP' AND QSTN_CODE = 'MERGE_BSNS'  ),
FILTER_A5                             as ( SELECT * FROM    RENAME_A5 
                                            WHERE SRVC_OFRNG_CODE = 'TRUP' AND QSTN_CODE = 'MULTI_FEIN_PLCY'  ),
FILTER_A6                             as ( SELECT * FROM    RENAME_A6 
                                            WHERE SRVC_OFRNG_CODE = 'TRUP' AND QSTN_CODE = 'PSV_USED'  ),
FILTER_A7                             as ( SELECT * FROM    RENAME_A7 
                                            WHERE SRVC_OFRNG_CODE = 'TRUP' AND QSTN_CODE = 'TEMP_SRVC_AGNCY'  ),
FILTER_A8                             as ( SELECT * FROM    RENAME_A8 
                                            WHERE SRVC_OFRNG_CODE = 'TRUP' AND QSTN_CODE = 'PEO_LEASE'  ),
FILTER_A9                             as ( SELECT * FROM    RENAME_A9 
                                            WHERE SRVC_OFRNG_CODE = 'TRUP' AND QSTN_CODE = 'SUB_CNTRC_1099'  ),
FILTER_A10                            as ( SELECT * FROM    RENAME_A10 
                                            WHERE SRVC_OFRNG_CODE = 'TRUP' AND QSTN_CODE = 'EXTRN_OHIO_OPRT'  ),
FILTER_A11                            as ( SELECT * FROM    RENAME_A11 
                                            WHERE SRVC_OFRNG_CODE = 'TRUP' AND QSTN_CODE = 'EXTRN_OHIO_CVRG'  ),
FILTER_A12                            as ( SELECT * FROM    RENAME_A12 
                                            WHERE SRVC_OFRNG_CODE = 'TRUP' AND QSTN_CODE = 'OPRT_CHNGS'  ),
FILTER_A13                            as ( SELECT * FROM    RENAME_A13 
                                            WHERE SRVC_OFRNG_CODE = 'TRUP' AND QSTN_CODE = 'RPT_VRBL_EXPLTN'  ),

---- JOIN LAYER ----

PREM1 as ( SELECT * 
				FROM  FILTER_PREM1
				LEFT JOIN FILTER_CN ON  FILTER_P.CUST_ID_ACCT_HLDR =  FILTER_C.CUST_ID 
						INNER JOIN INNER JOIN  FILTER_WCP1  using( PREM_PRD_ID )   ),
PREM2 as ( SELECT * 
				FROM  FILTER_PREM2
				INNER JOIN INNER JOIN  FILTER_WCP2  using( PREM_PRD_ID )   ),
PP as ( SELECT * 
				FROM  FILTER_PP
				INNER JOIN INNER JOIN  FILTER_P  using( AGRE_ID )  
						LEFT JOIN PREM1  using( PLCY_PRD_ID )  
						LEFT JOIN PREM2  using( PLCY_PRD_ID )  
								LEFT JOIN FILTER_PT ON  FILTER_PP.PLCY_PRD_ID =  FILTER_PT.PLCY_PRD_ID 
								LEFT JOIN FILTER_A1 ON  FILTER_PP.PLCY_NO =  FILTER_A1.PLCY_NMBR 
								LEFT JOIN FILTER_A2 ON  FILTER_PP.PLCY_NO =  FILTER_A2.PLCY_NMBR 
								LEFT JOIN FILTER_A3 ON  FILTER_PP.PLCY_NO =  FILTER_A3.PLCY_NMBR 
								LEFT JOIN FILTER_A4 ON  FILTER_PP.PLCY_NO =  FILTER_A4.PLCY_NMBR 
								LEFT JOIN FILTER_A5 ON  FILTER_PP.PLCY_NO =  FILTER_A5.PLCY_NMBR 
								LEFT JOIN FILTER_A6 ON  FILTER_PP.PLCY_NO =  FILTER_A6.PLCY_NMBR 
								LEFT JOIN FILTER_A7 ON  FILTER_PP.PLCY_NO =  FILTER_A7.PLCY_NMBR 
								LEFT JOIN FILTER_A8 ON  FILTER_PP.PLCY_NO =  FILTER_A8.PLCY_NMBR 
								LEFT JOIN FILTER_A9 ON  FILTER_PP.PLCY_NO =  FILTER_A9.PLCY_NMBR 
								LEFT JOIN FILTER_A10 ON  FILTER_PP.PLCY_NO =  FILTER_A10.PLCY_NMBR 
								LEFT JOIN FILTER_A11 ON  FILTER_PP.PLCY_NO =  FILTER_A11.PLCY_NMBR 
								LEFT JOIN FILTER_A12 ON  FILTER_PP.PLCY_NO =  FILTER_A12.PLCY_NMBR 
								LEFT JOIN FILTER_A13 ON  FILTER_PP.PLCY_NO =  FILTER_A13.PLCY_NMBR  )
SELECT * 
FROM PP