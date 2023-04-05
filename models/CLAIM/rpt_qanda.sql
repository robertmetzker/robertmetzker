with 
src_pp  AS ( select * from {{ source( 'DEV_EDW','STAGING.STG_POLICY_PERIOD') }}                where PLCY_PRD_EFF_DT >= '2018-01-01' ),
src_p   AS ( select * from {{ source( 'DEV_EDW','STAGING.STG_POLICY') }}                       where AGRE_TYP_CD = 'PLCY' ),
src_cn  AS ( select * from {{ source( 'DEV_EDW','STAGING.STG_CUSTOMER_NAME) }}  
                where 
                    CUST_NM_TYP_CD = 'BUSN_LEGAL_NM' 
                    and VOID_IND = 'N' 
                    and CUST_NM_END_DT is null),
src_prem  AS ( select * from {{ source( 'DEV_EDW.STAGING','STG_PREMIUM_PERIOD') }}             where VOID_IND = 'N' and PREM_TYP_CD in ('A','E') ),
src_covg  AS ( select * from {{ source( 'DEV_EDW.STAGING','STG_WC_COVERAGE_PREMIUM') }}        where WC_COV_PREM_VOID_IND = 'N' ),
src_pce   AS ( select * from {{ source( 'DEV_EDW.STAGING','STG_POLICY_CONTROL_ELEMENT') }}     where VOID_IND = 'N' AND CTL_ELEM_TYP_CD = 'PLCY_TYP'  ),
src_pp  AS ( select * from DEV_EDW.STAGING.STG_POLICY_PERIOD                where PLCY_PRD_EFF_DT >= '2018-01-01' ),
src_ans   AS (
    SELECT 
        PLCY_NMBR      as POLICY_NUMBER, 
        PRD_BGNG_DATE, 
        ANSWR_CRT_DTTM as QUESTIONNAIRE_COMPLETED_DT,
        QSTN_CODE,
        ANSWR_TEXT
    FROM 
        {{ ref( 'STG_TRUE_UP_QUESTIONNAIRE' ) }} 
    where srvc_ofrng_code = 'TRUP'),
src_qst     AS  (
    SELECT distinct
        QSTN_CODE,
        QSTN_TEXT
    FROM 
        {{ ref( 'STG_TRUE_UP_QUESTIONNAIRE') }}
    where srvc_ofrng_code = 'TRUP'),
-- src_pp  AS ( select * from DEV_EDW.STAGING.STG_POLICY_PERIOD                where PLCY_PRD_EFF_DT >= '2018-01-01' ),
-- src_p   AS ( select * from DEV_EDW.STAGING.STG_POLICY                       where AGRE_TYP_CD = 'PLCY' ),
-- src_cn  AS ( select * from DEV_EDW.STAGING.STG_CUSTOMER_NAME  
--                 where 
--                     CUST_NM_TYP_CD = 'BUSN_LEGAL_NM' 
--                     and VOID_IND = 'N' 
--                     and CUST_NM_END_DT is null),
-- src_prem  AS ( select * from DEV_EDW.STAGING.STG_PREMIUM_PERIOD             where VOID_IND = 'N' and PREM_TYP_CD in ('A','E') ),
-- src_covg  AS ( select * from DEV_EDW.STAGING.STG_WC_COVERAGE_PREMIUM        where WC_COV_PREM_VOID_IND = 'N' ),
-- src_pce   AS ( select * from DEV_EDW.STAGING.STG_POLICY_CONTROL_ELEMENT     where VOID_IND = 'N' AND CTL_ELEM_TYP_CD = 'PLCY_TYP'  ),
-- src_ans   AS (
--     SELECT 
--         PLCY_NMBR      as POLICY_NUMBER, 
--         PRD_BGNG_DATE, 
--         ANSWR_CRT_DTTM as QUESTIONNAIRE_COMPLETED_DT,
--         QSTN_CODE,
--         ANSWR_TEXT
--     FROM 
--         DEV_EDW_32600145.PUBLIC.STG_TRUE_UP_QUESTIONNAIRE 
--     where srvc_ofrng_code = 'TRUP'),
-- src_qst     AS  (
--     SELECT distinct
--         QSTN_CODE,
--         QSTN_TEXT
--     FROM 
--         DEV_EDW_32600145.PUBLIC.STG_TRUE_UP_QUESTIONNAIRE 
--     where srvc_ofrng_code = 'TRUP'),
A_PIVOT as (
    select *
    from SRC_ANS
    pivot( max( ANSWR_TEXT ) for QSTN_CODE in ( 
                                                'ALL_RQR_DCM'       ,
                                                'CEASE_OPRTN'       ,
                                                'SELL_OPRTN'        ,
                                                'MERGE_BSNS'        ,
                                                'MULTI_FEIN_PLCY'   ,
                                                'PSV_USED'          ,
                                                'TEMP_SRVC_AGNCY'   ,
                                                'PEO_LEASE'         ,
                                                'SUB_CNTRC_1099'    ,
                                                'EXTRN_OHIO_OPRT'   ,
                                                'EXTRN_OHIO_CVRG'   ,
                                                'OPRT_CHNGS'        ,
                                                'RPT_VRBL_EXPLTN'
                                                )
       )
    as p (  policy_number
		    , PRD_BGNG_DATE
            , QUESTIONNAIRE_COMPLETED_DTM
            , ALL_YOU_NEED_TO_COMPLETE_ANSWER
            , CEASE_OPERATIONS_ANSWER
            , SELL_OPERATIONS_ANSWER
            , MERGE_BUSINESS_ANSWER
            , MULTIPLE_FEIN_ANSWER
            , USED_PAYROLL_SERVICE_VENDOR_ANSWER
            , TEMP_SERVICE_AGENCY_ANSWER
            , PEO_LEASE_AGREEMENT_ANSWER
            , SUBCONTRACT_LABOR_1099_FORMS_ANSWER
            , OPERATIONS_OUTSIDE_OHIO_ANSWER
            , COVERAGE_OUTSIDE_OHIO_ANSWER
            , CHANGE_IN_OPERATIONS_ANSWER
            , REPORT_LESS_THAN_ESTIMATE_EXPLANATION
    )),    
Q_PIVOT as (
    select *
    from SRC_QST
    pivot( max( QSTN_TEXT ) for QSTN_CODE in ( 
                                                'ALL_RQR_DCM'       ,
                                                'CEASE_OPRTN'       ,
                                                'SELL_OPRTN'        ,
                                                'MERGE_BSNS'        ,
                                                'MULTI_FEIN_PLCY'   ,
                                                'PSV_USED'          ,
                                                'TEMP_SRVC_AGNCY'   ,
                                                'PEO_LEASE'         ,
                                                'SUB_CNTRC_1099'    ,
                                                'EXTRN_OHIO_OPRT'   ,
                                                'EXTRN_OHIO_CVRG'   ,
                                                'OPRT_CHNGS'        ,
                                                'RPT_VRBL_EXPLTN'
                                                )
       )
    as p (  
              ALL_YOU_NEED_TO_COMPLETE_QUESTION
            , CEASE_OPERATIONS_QUESTION
            , SELL_OPERATIONS_QUESTION
            , MERGE_BUSINESS_QUESTION
            , MULTIPLE_FEIN_QUESTION
            , USED_PAYROLL_SERVICE_VENDOR_QUESTION
            , TEMP_SERVICE_AGENCY_QUESTION
            , PEO_LEASE_AGREEMENT_QUESTION
            , SUBCONTRACT_LABOR_1099_FORMS_QUESTION
            , OPERATIONS_OUTSIDE_OHIO_QUESTION
            , COVERAGE_OUTSIDE_OHIO_QUESTION
            , CHANGE_IN_OPERATIONS_QUESTION
            , REPORT_LESS_THAN_ESTIMATE_QUESTION
    )),
q_and_a as (
    SELECt * 
    	,rank() over (partition by A_PIVOT.policy_number, A_PIVOT.PRD_BGNG_DATE 
        		order by A_PIVOT.QUESTIONNAIRE_COMPLETED_DTM desc ) as ANS_RANK    
    FROM 
        A_PIVOT,
        Q_PIVOT
    QUALIFY ANS_RANK = 1
        ),
policy_info as (
    SELECT
                 src_pp.PLCY_NO                     as      POLICY_NUMBER
        ,        src_cn.CUST_NM_DRV_UPCS_NM         as      EMPLOYER_NAME
        ,        src_pp.PLCY_PRD_EFF_DT             as      POLICY_PERIOD_EFF_DATE
        ,        src_pp.PLCY_PRD_END_DT             as      POLICY_PERIOD_END_DATE
        ,        src_pce.CTL_ELEM_SUB_TYP_CD        as      POLICY_TYPE_CODE
        ,        src_pce.CTL_ELEM_SUB_TYP_NM        as      POLICY_TYPE_DESC
        ,        sum( case when  src_prem.PREM_TYP_CD = 'E' 
                        then  src_covg.WC_COV_PREM_DRV_MNL_PREM_AMT 
                    else 0 END )                  as      ESTIMATED_PREMIUM_AMOUNT
        ,        sum( case when  src_prem.PREM_TYP_CD = 'A' 
                        then  src_covg.WC_COV_PREM_DRV_MNL_PREM_AMT 
                    else 0 END )                  as      TRUE_UP_PREMIUM_AMOUNT
    FROM
        src_p
            INNER JOIN src_pp   ON ( src_p.AGRE_ID = src_pp.AGRE_ID )
            LEFT JOIN  src_cn   ON ( src_p.CUST_ID_ACCT_HLDR = src_cn.CUST_ID )
            LEFT JOIN  src_prem ON ( src_pp.PLCY_PRD_ID = src_prem.PLCY_PRD_ID )
            LEFT JOIN  src_covg ON ( src_prem.PREM_PRD_ID = src_covg.WC_COV_PREM_ID )
            LEFT JOIN src_pce   ON ( src_pp.PLCY_PRD_ID = src_pce.PLCY_PRD_ID )
    group BY
          POLICY_NUMBER
        , EMPLOYER_NAME
        , POLICY_PERIOD_EFF_DATE
        , POLICY_PERIOD_END_DATE
        , POLICY_TYPE_CODE
        , POLICY_TYPE_DESC
)
SELECT           
          p.POLICY_NUMBER
        , p.EMPLOYER_NAME
        , p.POLICY_PERIOD_EFF_DATE
        , p.POLICY_PERIOD_END_DATE
        , p.POLICY_TYPE_CODE
        , p.POLICY_TYPE_DESC
        , p.ESTIMATED_PREMIUM_AMOUNT
        , p.TRUE_UP_PREMIUM_AMOUNT
        -- , qa.ANS_RANK
        , qa.QUESTIONNAIRE_COMPLETED_DTM
        , qa.ALL_YOU_NEED_TO_COMPLETE_ANSWER
        , qa.CEASE_OPERATIONS_ANSWER
        , qa.SELL_OPERATIONS_ANSWER
        , qa.MERGE_BUSINESS_ANSWER
        , qa.MULTIPLE_FEIN_ANSWER
        , qa.USED_PAYROLL_SERVICE_VENDOR_ANSWER
        , qa.TEMP_SERVICE_AGENCY_ANSWER
        , qa.PEO_LEASE_AGREEMENT_ANSWER
        , qa.SUBCONTRACT_LABOR_1099_FORMS_ANSWER
        , qa.OPERATIONS_OUTSIDE_OHIO_ANSWER
        , qa.COVERAGE_OUTSIDE_OHIO_ANSWER
        , qa.CHANGE_IN_OPERATIONS_ANSWER
        , qa.REPORT_LESS_THAN_ESTIMATE_EXPLANATION
        , qa.ALL_YOU_NEED_TO_COMPLETE_QUESTION
        , qa.CEASE_OPERATIONS_QUESTION
        , qa.SELL_OPERATIONS_QUESTION
        , qa.MERGE_BUSINESS_QUESTION
        , qa.MULTIPLE_FEIN_QUESTION
        , qa.USED_PAYROLL_SERVICE_VENDOR_QUESTION
        , qa.TEMP_SERVICE_AGENCY_QUESTION
        , qa.PEO_LEASE_AGREEMENT_QUESTION
        , qa.SUBCONTRACT_LABOR_1099_FORMS_QUESTION
        , qa.OPERATIONS_OUTSIDE_OHIO_QUESTION
        , qa.COVERAGE_OUTSIDE_OHIO_QUESTION
        , qa.CHANGE_IN_OPERATIONS_QUESTION
        , qa.REPORT_LESS_THAN_ESTIMATE_QUESTION
FROM
    policy_info        as p
    LEFT JOIN q_and_a  as qa       ON ( p.POLICY_NUMBER = qa.POLICY_NUMBER 
    								and p.POLICY_PERIOD_EFF_DATE = qa.PRD_BGNG_DATE)
);