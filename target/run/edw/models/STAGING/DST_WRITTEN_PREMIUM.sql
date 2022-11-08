

      create or replace  table DEV_EDW.STAGING.DST_WRITTEN_PREMIUM  as
      (---- SRC LAYER ----
WITH
SRC_PP             as ( SELECT *     FROM     STAGING.STG_POLICY_PERIOD ),
SRC_WCP            as ( SELECT *     FROM     STAGING.STG_WC_COVERAGE_PREMIUM ),
SRC_EMP            as ( SELECT *     FROM     STAGING.STG_POLICY_PERIOD_PARTICIPATION ),
SRC_PREM           as ( SELECT *     FROM     STAGING.STG_PREMIUM_PERIOD ),
SRC_IND            as ( SELECT *     FROM     STAGING.STG_POLICY_PERIOD_PARTICIPATION ),
SRC_CLS            as ( SELECT *     FROM     STAGING.STG_WC_CLASS ),
SRC_WCRT           as ( SELECT *     FROM     STAGING.STG_WC_CLASS_RATE_TIER ),
SRC_RP             as ( SELECT *     FROM     STAGING.STG_RATING_PLAN_HISTORY ),
SRC_U              as ( SELECT *     FROM     STAGING.STG_USERS ),
SRC_PR             as ( SELECT *     FROM     STAGING.STG_PAYROLL_REPORT ),
SRC_PA             as ( SELECT *     FROM     STAGING.STG_POLICY_AUDIT ),
SRC_PASH           as ( SELECT *     FROM     STAGING.STG_POLICY_AUDIT_STATUS_HISTORY ),
SRC_PT             as ( SELECT *     FROM     STAGING.STG_POLICY_CONTROL_ELEMENT ),
SRC_PSRH           as ( SELECT *     FROM     STAGING.STG_POLICY_STATUS_REASON_HISTORY ),
SRC_WCSX           as ( SELECT *     FROM     STAGING.STG_WC_CLASS_SIC_XREF ),
SRC_P              as ( SELECT *     FROM     STAGING.STG_POLICY  ),
SRC_PPPD           as ( SELECT *     FROM     STAGING.STG_POLICY_PERIOD_PREMIUM_DRV ),
SRC_RE             as ( SELECT *     FROM     STAGING.STG_RATING_ELEMENT ),
//SRC_PP             as ( SELECT *     FROM     STG_POLICY_PERIOD) ,
//SRC_WCP            as ( SELECT  B.PLCY_PRD_ID, A.*     FROM     STAGING.STG_WC_COVERAGE_PREMIUM A LEFT JOIN STAGING.STG_PREMIUM_PERIOD B USING(PREM_PRD_ID)),
//SRC_EMP            as ( SELECT *     FROM     STG_POLICY_PERIOD_PARTICIPATION) ,
//SRC_PREM           as ( SELECT *     FROM     STG_PREMIUM_PERIOD) ,
//SRC_IND            as ( SELECT *     FROM     STG_POLICY_PERIOD_PARTICIPATION) ,
//SRC_CLS            as ( SELECT *     FROM     STG_WC_CLASS) ,
//SRC_WCRT           as ( SELECT *     FROM     STG_WC_CLASS_RATE_TIER) ,
//SRC_RP             as ( SELECT *     FROM     STG_RATING_PLAN_HISTORY) ,
//SRC_U              as ( SELECT *     FROM     STG_USERS) ,
//SRC_PR             as ( SELECT *     FROM     STG_PAYROLL_REPORT) ,
//SRC_PA             as ( SELECT *     FROM     STG_POLICY_AUDIT) ,
//SRC_PASH           as ( SELECT *     FROM     STG_POLICY_AUDIT_STATUS_HISTORY) ,
//SRC_PT             as ( SELECT *     FROM     STG_POLICY_CONTROL_ELEMENT) ,
//SRC_PSRH           as ( SELECT *     FROM     STG_POLICY_STATUS_REASON_HISTORY) ,
//SRC_WCSX           as ( SELECT *     FROM     STG_WC_CLASS_SIC_XREF) ,
//SRC_P              as ( SELECT *     FROM     STG_POLICY) ,
//SRC_PPPD           as ( SELECT *     FROM     STG_POLICY_PERIOD_PREMIUM_DRV) ,
//SRC_RE             as ( SELECT *     FROM     STG_RATING_ELEMENT) ,

---- LOGIC LAYER ----


LOGIC_PP as ( SELECT 
		  TRIM( PLCY_NO )                                    as                                            PLCY_NO 
		, AGRE_ID                                            as                                            AGRE_ID 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT 
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT 
    , VOID_IND                                           as                                           VOID_IND 
    , case when PLCY_PRD_EFF_DT = min(PLCY_PRD_EFF_DT) over (partition by PLCY_NO) then 'Y' else 'N' END 
                                                          as                                    NEW_POLICY_IND 
		FROM SRC_PP
    WHERE PLCY_PRD_EFF_DT <> PLCY_PRD_END_DT
            ),

LOGIC_WCP as ( SELECT 
		  CUST_ID_PTCP_BUSN_INS                              as                              CUST_ID_PTCP_BUSN_INS 
		, WC_COV_PREM_ID                                     as                                     WC_COV_PREM_ID 
		, TRIM( RT_TYP_NM )                                  as                                          RT_TYP_NM 
		, WC_COV_PREM_EFF_DT                                 as                                 WC_COV_PREM_EFF_DT 
		, WC_COV_PREM_END_DT                                 as                                 WC_COV_PREM_END_DT 
		, PTCP_ID_COV                                        as                                        PTCP_ID_COV 
		, CUST_ID_COV                                        as                                        CUST_ID_COV 
		, WC_CLS_SUFX_ID                                     as                                     WC_CLS_SUFX_ID 
		, WC_COV_PREM_BS_UNT                                 as                                 WC_COV_PREM_BS_UNT 
		, WC_COV_PREM_BS_VAL                                 as                                 WC_COV_PREM_BS_VAL 
		, WC_COV_PREM_PURE_RT                                as                                WC_COV_PREM_PURE_RT 
		, WC_COV_PREM_PURE_PREM_AMT                          as                          WC_COV_PREM_PURE_PREM_AMT 
		, WC_COV_PREM_BWC_ADMN_RT                            as                            WC_COV_PREM_BWC_ADMN_RT 
		, WC_COV_PREM_IC_ADMN_RT                             as                             WC_COV_PREM_IC_ADMN_RT 
		, WC_COV_PREM_DWRF_RT                                as                                WC_COV_PREM_DWRF_RT 
		, WC_COV_PREM_DWRF_II_RT                             as                             WC_COV_PREM_DWRF_II_RT 
		, WC_COV_PREM_DRV_MNL_PREM_AMT                       as                       WC_COV_PREM_DRV_MNL_PREM_AMT 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, cast( AUDIT_USER_CREA_DTM as DATE )                as                                AUDIT_USER_CREA_DTM 
    , TRIM( WC_COV_PREM_VOID_IND )                       as                               WC_COV_PREM_VOID_IND 
    , PREM_PRD_ID                                        as                                        PREM_PRD_ID 
    , RISK_ID                                            as                                            RISK_ID 
    , AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
    , PYRL_RPT_ID                                        as                                        PYRL_RPT_ID 
    , max(AUDIT_USER_CREA_DTM) over(PARTITION BY  PLCY_PRD_ID) as                                     MAX_DATE 
    , AUDIT_USER_CREA_DTM                                as                                         ACT_CRT_DT 
		FROM SRC_WCP
    ---- Data Extraction below is to restrict the entries that exists more than once for a key column. this avoids the overlapping Premium Calcualtion Effective and end dates for a Premium record.
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY PREM_PRD_ID, RISK_ID, WC_CLS_SUFX_ID, CUST_ID_COV ORDER BY  WC_COV_PREM_ID DESC )) =1
            ) ,

LOGIC_EMP as ( SELECT DISTINCT
      CUST_ID                                            as                                             CUST_ID 
    , TRIM( CUST_NO )                                    as                                             CUST_NO 
		FROM SRC_EMP
            ),

LOGIC_PREM as ( SELECT 
      PREM_PRD_ID                                        as                                        PREM_PRD_ID 
    , PREM_PRD_EFF_DT                                    as                                    PREM_PRD_EFF_DT 
    , PREM_PRD_END_DT                                    as                                    PREM_PRD_END_DT 
    , TRIM( PREM_TYP_CD )                                as                                        PREM_TYP_CD 
    , CASE WHEN PREM_TYP_CD = 'E' THEN 1
           WHEN PREM_TYP_CD = 'R' THEN 2
           WHEN PREM_TYP_CD = 'A' THEN 3 END             as                                DERIVED_PREM_TYP_CD 
    , TRIM( PREM_TYP_NM )                                as                                        PREM_TYP_NM 
    , TRIM( VOID_IND )                                   as                                           VOID_IND 
    , PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
    , cast( AUDIT_USER_CREA_DTM as DATE )                as                                AUDIT_USER_CREA_DTM 
		FROM SRC_PREM
            ),

LOGIC_IND as ( SELECT 
		  PTCP_ID                                            as                                            PTCP_ID 
		, TRIM( CUST_NO )                                    as                                            CUST_NO 
		, TRIM( COV_TYP_CD )                                 as                                         COV_TYP_CD 
		, TRIM( COV_TYP_NM )                                 as                                         COV_TYP_NM 
		, TRIM( TTL_TYP_CD )                                 as                                         TTL_TYP_CD 
		, TRIM( TTL_TYP_NM )                                 as                                         TTL_TYP_NM 
		, TRIM( PPPIE_COV_IND )                              as                                      PPPIE_COV_IND 
		FROM SRC_IND
            ),

LOGIC_CLS as ( SELECT 
      WC_CLS_ID                                          as                                          WC_CLS_ID
    , TRIM( WC_CLS_CLS_CD )                              as                                      WC_CLS_CLS_CD 
    , TRIM( WC_CLS_SUFX_CLS_SUFX )                       as                               WC_CLS_SUFX_CLS_SUFX 
    , IFF(WC_CLS_SUFX_CLS_SUFX = 970 , 'RT_2' , 'RT_1')  as                       DERIVED_WC_CLS_SUFX_CLS_SUFX 
    , TRIM( WC_CLS_SUFX_NM )                             as                                     WC_CLS_SUFX_NM 
    , WC_CLS_SUFX_EFF_DT                                 as                                 WC_CLS_SUFX_EFF_DT 
    , WC_CLS_SUFX_END_DT                                 as                                 WC_CLS_SUFX_END_DT 
    , WC_CLS_SUFX_ID                                     as                                     WC_CLS_SUFX_ID 
		FROM SRC_CLS
            ),

LOGIC_WCRT as ( SELECT 
		  WC_CLS_RT_TIER_RT                                  as                                  WC_CLS_RT_TIER_RT 
    , WC_CLS_RT_TIER_TYP_CD	                             as                              WC_CLS_RT_TIER_TYP_CD	
		, WC_CLS_ID                                          as                                          WC_CLS_ID 
    , WC_CLS_RT_TIER_EFF_DT                              as                              WC_CLS_RT_TIER_EFF_DT
		, TRIM( WC_CLS_RT_TIER_VOID_IND )                    as                            WC_CLS_RT_TIER_VOID_IND 
		FROM SRC_WCRT
            ),

LOGIC_RP as ( SELECT 
      PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, TRIM( RATING_PLAN_TYPE_CODE )                      as                              RATING_PLAN_TYPE_CODE 
		, TRIM( RATING_PLAN_TYPE_DESC )                      as                              RATING_PLAN_TYPE_DESC 
		, TRIM( EXPRN_MOD_TYPE_DESC )                        as                                EXPRN_MOD_TYPE_DESC 
		, RATING_PLAN_RATE                                   as                                   RATING_PLAN_RATE 
    , PPRE_EFF_DT                                        as                                        PPRE_EFF_DT
    , RATING_PLAN_EFFECTIVE_DATE                         as                         RATING_PLAN_EFFECTIVE_DATE
    , RATING_PLAN_END_DATE                               as                               RATING_PLAN_END_DATE
		FROM SRC_RP
            ),

LOGIC_U as ( SELECT 
		  TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		, TRIM( USER_DRV_UPCS_NM )                           as                                   USER_DRV_UPCS_NM 
		, USER_ID                                            as                                            USER_ID 
		FROM SRC_U
            ),

LOGIC_PR as ( SELECT 
		  PYRL_RPT_ID                                        as                                        PYRL_RPT_ID 
		, TRIM( PYRL_RPT_EST_RPT_IND )                       as                               PYRL_RPT_EST_RPT_IND 
		, PYRL_RPT_RECV_DT                                   as                                   PYRL_RPT_RECV_DT 
    , PYRL_RPT_STS_TYP_NM                                as                                PYRL_RPT_STS_TYP_NM 
		FROM SRC_PR
            ),

LOGIC_PA as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PLCY_PRD_AUDT_DTL_COMP_DT                          as                          PLCY_PRD_AUDT_DTL_COMP_DT 
		, TRIM( PLCY_AUDT_TYP_CD )                           as                                   PLCY_AUDT_TYP_CD 
		, TRIM( PLCY_AUDT_TYP_NM )                           as                                   PLCY_AUDT_TYP_NM 
    , PLCY_PRD_AUDT_DTL_ID                               as                               PLCY_PRD_AUDT_DTL_ID 
	 	FROM SRC_PA
            ),

LOGIC_PASH as ( SELECT 
      PLCY_PRD_AUDT_DTL_ID                               as                                PLCY_PRD_AUDT_DTL_ID 
    , PLCY_AUDT_STS_TYP_CD                               as                                PLCY_AUDT_STS_TYP_CD 
    , TRIM( PLCY_AUDT_STS_TYP_NM )                       as                                PLCY_AUDT_STS_TYP_NM 
    , TRIM( PLCY_AUDT_STS_RSN_TYP_NM )                   as                            PLCY_AUDT_STS_RSN_TYP_NM 
    , HIST_EFF_DTM                                      as                                         HIST_EFF_DTM1    
    , CASE WHEN HIST_EFF_DTM::DATE = LAG(HIST_END_DTM::DATE,1, '12/31/2099') OVER (PARTITION BY PLCY_PRD_AUDT_DTL_ID ORDER BY HIST_EFF_DTM, NVL(HIST_END_DTM, CURRENT_DATE))
                        THEN HIST_EFF_DTM::DATE+1 ELSE HIST_EFF_DTM::DATE END      
	    					                                         as                                       HIST_EFF_DTM 
    , cast( HIST_END_DTM as DATE )                       as                                       HIST_END_DTM 
        FROM SRC_PASH WHERE DATE(HIST_EFF_DTM) <> DATE( NVL(HIST_END_DTM, CURRENT_DATE))
    -----------------------------------------------------------------------------------------------------------------------------------------------				  
                 -- THIS IS TO FILTER INTRA DAY HISTORY CHANGES. i.e. to pick the latest history record if multiple row exists for a day.
	  ------------------------------------------------------------------------------------------------------------------------------------------------	 
      QUALIFY(ROW_NUMBER() OVER (PARTITION BY PLCY_PRD_AUDT_DTL_ID, HIST_EFF_DTM::DATE ORDER BY HIST_EFF_DTM DESC, NVL(HIST_END_DTM, CURRENT_DATE)DESC )) =1  
            ),

LOGIC_PT as ( SELECT 
      PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
    , TRIM( CTL_ELEM_TYP_CD )                            as                                    CTL_ELEM_TYP_CD
    , TRIM( CTL_ELEM_SUB_TYP_NM )                        as                                CTL_ELEM_SUB_TYP_NM
    , TRIM( CTL_ELEM_SUB_TYP_CD )                        as                                CTL_ELEM_SUB_TYP_CD
    , VOID_IND                                           as                                           VOID_IND
		FROM SRC_PT
            ),

LOGIC_PSRH as ( SELECT  
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, TRIM( PLCY_STS_TYP_CD )                            as                                    PLCY_STS_TYP_CD 
		, TRIM( PLCY_STS_TYP_NM )                            as                                    PLCY_STS_TYP_NM 
		, TRIM( PLCY_STS_RSN_TYP_CD )                        as                                PLCY_STS_RSN_TYP_CD 
		, TRIM( PLCY_STS_RSN_TYP_NM )                        as                                PLCY_STS_RSN_TYP_NM 
    , MOST_RCNT_PLCY_STS_RSN_IND                         as                         MOST_RCNT_PLCY_STS_RSN_IND
		, PLCY_STS_TRANS_DT                                  as                                  PLCY_STS_TRANS_DT
    , PLCY_STS_RSN_TRANS_DT                              as                              PLCY_STS_RSN_TRANS_DT
    , AGRE_ID                                            as                                            AGRE_ID
		FROM SRC_PSRH
            ),

LOGIC_WCSX as ( SELECT 
		  TRIM( WC_CLS_SIC_XREF_CLS_CD )                     as                             WC_CLS_SIC_XREF_CLS_CD 
		, TRIM( SIC_TYP_CD )                                 as                                         SIC_TYP_CD 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_WCSX
            ),

LOGIC_P as ( SELECT 
      AGRE_ID                                            as                                            AGRE_ID
    , TRIM( CUST_NO )                                    as                                            CUST_NO
    , CUST_ID_ACCT_HLDR                                  as                                  CUST_ID_ACCT_HLDR
		FROM SRC_P
            ),

LOGIC_PPPD as ( SELECT 
      PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
    , PLCY_PRD_PREM_DRV_EFF_DTM                          as                          PLCY_PRD_PREM_DRV_EFF_DTM
    , PLCY_PRD_PREM_DRV_END_DTM                          as                          PLCY_PRD_PREM_DRV_END_DTM
    , TRIM( PREM_TYP_CD )                                as                                        PREM_TYP_CD
    , TRIM( PREM_TYP_NM )                                as                                        PREM_TYP_NM
    , TRIM( RT_ELEM_TYP_CD )                             as                                     RT_ELEM_TYP_CD
    , TRIM( RT_ELEM_TYP_NM )                             as                                     RT_ELEM_TYP_NM
    , PLCY_PRD_PREM_DRV_PREM_AMT                         as                         PLCY_PRD_PREM_DRV_PREM_AMT
    , AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
    , cast( AUDIT_USER_CREA_DTM as DATE )                as                                AUDIT_USER_CREA_DTM
    , AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
    FROM SRC_PPPD
            ),

LOGIC_RE as ( SELECT 
		  RT_ELEM_TYP_NM_EFF_DT                              as                              RT_ELEM_TYP_NM_EFF_DT 
		, RT_ELEM_TYP_NM_END_DT                              as                              RT_ELEM_TYP_NM_END_DT 
		, TRIM( RT_ELEM_TYP_CD )                             as                                     RT_ELEM_TYP_CD 
		FROM SRC_RE
            )

---- RENAME LAYER ----
,
RENAME_PP         as ( SELECT 
      PLCY_NO                                            as                                            PLCY_NO
    , AGRE_ID                                            as                                            AGRE_ID
    , PLCY_PRD_ID                                        as                                     PP_PLCY_PRD_ID
    , PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT
    , PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT
    , VOID_IND                                           as                                        PP_VOID_IND
    , NEW_POLICY_IND                                     as                                     NEW_POLICY_IND
                    FROM     LOGIC_PP   ), 
RENAME_WCP        as ( SELECT 
      CUST_ID_PTCP_BUSN_INS                              as                               EMPLOYER_CUSTOMER_ID
    , WC_COV_PREM_ID                                     as                                     WCP_COV_PRD_ID
    , RT_TYP_NM                                          as                                          RT_TYP_NM
    , WC_COV_PREM_EFF_DT                                 as                                 WC_COV_PREM_EFF_DT
    , WC_COV_PREM_END_DT                                 as                                 WC_COV_PREM_END_DT
    , PTCP_ID_COV                                        as                                        PTCP_ID_COV
    , CUST_ID_COV                                        as                         COVERED_INDIVIDUAL_CUST_ID
    , WC_CLS_SUFX_ID                                     as                                     WC_CLS_SUFX_ID
    , WC_COV_PREM_BS_UNT                                 as                                     EMPLOYEE_COUNT
    , WC_COV_PREM_BS_VAL                                 as                                    EXPOSURE_AMOUNT
    , WC_COV_PREM_PURE_RT                                as                                  PURE_PREMIUM_RATE
    , WC_COV_PREM_PURE_PREM_AMT                          as                                PURE_PREMIUM_AMOUNT
    , WC_COV_PREM_BWC_ADMN_RT                            as                            BWC_ASSESSMENT_FEE_RATE
    , WC_COV_PREM_IC_ADMN_RT                             as                             IC_ASSESSMENT_FEE_RATE
    , WC_COV_PREM_DWRF_RT                                as                                      DWRF_FEE_RATE
    , WC_COV_PREM_DWRF_II_RT                             as                                   DWRF_II_FEE_RATE
    , WC_COV_PREM_DRV_MNL_PREM_AMT                       as                       TOTAL_DERIVED_PREMIUM_AMOUNT
    , AUDIT_USER_ID_CREA                                 as                                     CREATE_USER_ID
    , AUDIT_USER_CREA_DTM                                as                 PREMIUM_CALCULATION_EFFECTIVE_DATE
    , WC_COV_PREM_VOID_IND                               as                               WC_COV_PREM_VOID_IND
    , PREM_PRD_ID                                        as                                    WCP_PREM_PRD_ID
    , RISK_ID                                            as                                        WCP_RISK_ID
    , AUDIT_USER_CREA_DTM                                as                            WCP_AUDIT_USER_CREA_DTM
    , AUDIT_USER_UPDT_DTM                                as                            WCP_AUDIT_USER_UPDT_DTM
    , PYRL_RPT_ID                                        as                                        PYRL_RPT_ID
    , MAX_DATE                                           as                                           MAX_DATE
    , ACT_CRT_DT                                         as                                         ACT_CRT_DT
                    FROM     LOGIC_WCP   ), 
RENAME_EMP        as ( SELECT 
      CUST_ID                                            as                                        EMP_CUST_ID
    , CUST_NO                                            as                           EMPLOYER_CUSTOMER_NUMBER
                    FROM     LOGIC_EMP   ), 
RENAME_PREM       as ( SELECT 
      PREM_PRD_ID                                        as                                        PREM_PRD_ID
    , PREM_PRD_EFF_DT                                    as                                    PREM_PRD_EFF_DT
    , PREM_PRD_END_DT                                    as                                    PREM_PRD_END_DT
    , PREM_TYP_CD                                        as                                        PREM_TYP_CD
    , DERIVED_PREM_TYP_CD                                as                                DERIVED_PREM_TYP_CD
    , PREM_TYP_NM                                        as                                        PREM_TYP_NM
    , VOID_IND                                           as                                           VOID_IND
    , PLCY_PRD_ID                                        as                                   PREM_PLCY_PRD_ID
    , AUDIT_USER_CREA_DTM                                as                           PREM_AUDIT_USER_CREA_DTM
                    FROM     LOGIC_PREM   ), 
RENAME_IND        as ( SELECT 
      PTCP_ID                                            as                                        IND_PTCP_ID
    , CUST_NO                                            as                 COVERED_INDIVIDUAL_CUSTOMER_NUMBER
    , COV_TYP_CD                                         as                                         COV_TYP_CD
    , COV_TYP_NM                                         as                                         COV_TYP_NM
    , TTL_TYP_CD                                         as                                         TTL_TYP_CD
    , TTL_TYP_NM                                         as                                         TTL_TYP_NM
    , PPPIE_COV_IND                                      as                                      PPPIE_COV_IND
                    FROM     LOGIC_IND   ), 
RENAME_CLS        as ( SELECT 
      WC_CLS_ID                                          as                                          WC_CLS_ID
    , WC_CLS_CLS_CD                                      as                       WRITTEN_PREMIUM_ELEMENT_CODE
    , WC_CLS_SUFX_CLS_SUFX                               as                WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE
    , DERIVED_WC_CLS_SUFX_CLS_SUFX                       as                       DERIVED_WC_CLS_SUFX_CLS_SUFX
    , WC_CLS_SUFX_NM                                     as                       WRITTEN_PREMIUM_ELEMENT_DESC
    , WC_CLS_SUFX_EFF_DT                                 as             WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE
    , WC_CLS_SUFX_END_DT                                 as                   WRITTEN_PREMIUM_ELEMENT_END_DATE
    , WC_CLS_SUFX_ID                                     as                                 CLS_WC_CLS_SUFX_ID
                    FROM     LOGIC_CLS   ), 
RENAME_WCRT       as ( SELECT 
      WC_CLS_RT_TIER_RT                                  as                               CLASS_CODE_BASE_RATE
    , WC_CLS_RT_TIER_TYP_CD	                             as                              WC_CLS_RT_TIER_TYP_CD	
    , WC_CLS_ID                                          as                                     WCRT_WC_CLS_ID
    , WC_CLS_RT_TIER_EFF_DT                              as                              WC_CLS_RT_TIER_EFF_DT
    , WC_CLS_RT_TIER_VOID_IND                            as                            WC_CLS_RT_TIER_VOID_IND
                    FROM     LOGIC_WCRT   ), 
RENAME_RP         as ( SELECT 
		  PLCY_PRD_ID                                        as                                     RP_PLCY_PRD_ID
    , RATING_PLAN_TYPE_CODE                              as                                   RATING_PLAN_CODE
		, RATING_PLAN_TYPE_DESC                              as                                   RATING_PLAN_DESC
		, EXPRN_MOD_TYPE_DESC                                as                                EXPRN_MOD_TYPE_DESC
		, RATING_PLAN_RATE                                   as                                   RATING_PLAN_RATE 
    , PPRE_EFF_DT                                        as                                        PPRE_EFF_DT
    , RATING_PLAN_EFFECTIVE_DATE                         as                         RATING_PLAN_EFFECTIVE_DATE
    , RATING_PLAN_END_DATE                               as                               RATING_PLAN_END_DATE
				FROM     LOGIC_RP   ), 
RENAME_U          as ( SELECT 
      USER_LGN_NM                                        as                             CREATE_USER_LOGIN_NAME
    , USER_DRV_UPCS_NM                                   as                                   CREATE_USER_NAME
    , USER_ID                                            as                                            USER_ID
                    FROM     LOGIC_U   ), 
RENAME_PR         as ( SELECT 
      PYRL_RPT_ID                                        as                                     PR_PYRL_RPT_ID
    , PYRL_RPT_EST_RPT_IND                               as                               PYRL_RPT_EST_RPT_IND
    , PYRL_RPT_RECV_DT                                   as                                   PYRL_RPT_RECV_DT
    , PYRL_RPT_STS_TYP_NM                                as                                PYRL_RPT_STS_TYP_NM 
                    FROM     LOGIC_PR   ), 
RENAME_PA         as ( SELECT 
      PLCY_PRD_ID                                        as                                    PA_PLCY_PRD_ID
    , VOID_IND                                           as                                       PA_VOID_IND
    , PLCY_PRD_AUDT_DTL_COMP_DT                          as                         PLCY_PRD_AUDT_DTL_COMP_DT
    , PLCY_AUDT_TYP_CD                                   as                                  PLCY_AUDT_TYP_CD
    , PLCY_AUDT_TYP_NM                                   as                                  PLCY_AUDT_TYP_NM
    , PLCY_PRD_AUDT_DTL_ID                               as                              PLCY_PRD_AUDT_DTL_ID
                    FROM     LOGIC_PA   ), 
RENAME_PASH       as ( SELECT 
      PLCY_PRD_AUDT_DTL_ID                               as                              PLCY_PRD_AUDT_DTL_ID
    , PLCY_AUDT_STS_TYP_CD                               as                              PLCY_AUDT_STS_TYP_CD
    , PLCY_AUDT_STS_TYP_NM                               as                              PLCY_AUDT_STS_TYP_NM
    , PLCY_AUDT_STS_RSN_TYP_NM                           as                          PLCY_AUDT_STS_RSN_TYP_NM
    , HIST_EFF_DTM                                       as                                AUDIT_STS_EFF_DATE
    , HIST_END_DTM                                       as                                AUDIT_STS_END_DATE
    , HIST_EFF_DTM1                                      as                                     HIST_EFF_DTM1
                    FROM     LOGIC_PASH   ), 
RENAME_PASH2       as ( SELECT 
      PLCY_PRD_AUDT_DTL_ID                               as                        PASH2_PLCY_PRD_AUDT_DTL_ID
    , PLCY_AUDT_STS_TYP_CD                               as                        PASH2_PLCY_AUDT_STS_TYP_CD
    , PLCY_AUDT_STS_TYP_NM                               as                        PASH2_PLCY_AUDT_STS_TYP_NM
    , PLCY_AUDT_STS_RSN_TYP_NM                           as                    PASH2_PLCY_AUDT_STS_RSN_TYP_NM
    , HIST_EFF_DTM                                       as                          PASH2_AUDIT_STS_EFF_DATE
    , HIST_END_DTM                                       as                          PASH2_AUDIT_STS_END_DATE
                    FROM     LOGIC_PASH   ),
RENAME_PT         as ( SELECT 
      PLCY_PRD_ID                                        as                                    PT_PLCY_PRD_ID
    , CTL_ELEM_TYP_CD                                    as                                PT_CTL_ELEM_TYP_CD
    , CTL_ELEM_SUB_TYP_CD                                as                                  POLICY_TYPE_CODE
    , CTL_ELEM_SUB_TYP_NM                                as                                  POLICY_TYPE_DESC
    , VOID_IND                                           as                                       PT_VOID_IND
                    FROM     LOGIC_PT   ), 
RENAME_PSRH       as ( SELECT 
      PLCY_PRD_ID                                        as                                  PSRH_PLCY_PRD_ID
    , PLCY_STS_TYP_CD                                    as                                   PLCY_STS_TYP_CD
    , PLCY_STS_TYP_NM                                    as                                   PLCY_STS_TYP_NM
    , PLCY_STS_RSN_TYP_CD                                as                               PLCY_STS_RSN_TYP_CD
    , PLCY_STS_RSN_TYP_NM                                as                               PLCY_STS_RSN_TYP_NM
    , PLCY_STS_TYP_CD                                    as                                 POLICY_ACTIVE_IND
    , MOST_RCNT_PLCY_STS_RSN_IND                         as                        MOST_RCNT_PLCY_STS_RSN_IND
    , PLCY_STS_TRANS_DT                                  as                                 PLCY_STS_TRANS_DT
    , PLCY_STS_RSN_TRANS_DT                              as                             PLCY_STS_RSN_TRANS_DT
    , AGRE_ID                                            as                                      PSRH_AGRE_ID	
                    FROM     LOGIC_PSRH   ), 
RENAME_WCSX       as ( SELECT 
      WC_CLS_SIC_XREF_CLS_CD                             as                            WC_CLS_SIC_XREF_CLS_CD
    , SIC_TYP_CD                                         as                                        SIC_TYP_CD
    , VOID_IND                                           as                                     WCSX_VOID_IND
                    FROM     LOGIC_WCSX   ), 
RENAME_P          as ( SELECT 
      AGRE_ID                                            as                                         P_AGRE_ID
    , CUST_ID_ACCT_HLDR                                  as                              EMPLOYER_CUSTOMER_ID
    , CUST_NO                                            as                          EMPLOYER_CUSTOMER_NUMBER
    , CUST_ID_ACCT_HLDR                                  as                        COVERED_INDIVIDUAL_CUST_ID
    , CUST_NO                                            as                COVERED_INDIVIDUAL_CUSTOMER_NUMBER
                    FROM     LOGIC_P   ), 
RENAME_PPPD       as ( SELECT 
      PLCY_PRD_ID                                        as                                       PLCY_PRD_ID
    , PLCY_PRD_PREM_DRV_EFF_DTM                          as                                   PREM_PRD_EFF_DT
    , PLCY_PRD_PREM_DRV_END_DTM                          as                                   PREM_PRD_END_DT
    , PREM_TYP_CD                                        as                                       PREM_TYP_CD
    , PREM_TYP_NM                                        as                                       PREM_TYP_NM
    , PLCY_PRD_PREM_DRV_EFF_DTM                          as                                WC_COV_PREM_EFF_DT
    , PLCY_PRD_PREM_DRV_END_DTM                          as                                WC_COV_PREM_END_DT
    , RT_ELEM_TYP_CD                                     as                      WRITTEN_PREMIUM_ELEMENT_CODE
    , RT_ELEM_TYP_NM                                     as                      WRITTEN_PREMIUM_ELEMENT_DESC
    , PLCY_PRD_PREM_DRV_PREM_AMT                         as                      TOTAL_DERIVED_PREMIUM_AMOUNT
    , AUDIT_USER_ID_CREA                                 as                                    CREATE_USER_ID
    , AUDIT_USER_CREA_DTM                                as                               AUDIT_USER_CREA_DTM
    , AUDIT_USER_UPDT_DTM                                as                               AUDIT_USER_UPDT_DTM
                  FROM     LOGIC_PPPD   ), 
RENAME_RE         as ( SELECT 
      RT_ELEM_TYP_NM_EFF_DT                              as            WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE
    , RT_ELEM_TYP_NM_END_DT                              as                  WRITTEN_PREMIUM_ELEMENT_END_DATE
    , RT_ELEM_TYP_CD                                     as                                 RE_RT_ELEM_TYP_CD
                   FROM     LOGIC_RE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PREM                           as ( SELECT * FROM    RENAME_PREM   ),
FILTER_WCP                            as ( SELECT * FROM    RENAME_WCP 
                                            WHERE EXPOSURE_AMOUNT IS NOT NULL AND (MAX_DATE= ACT_CRT_DT OR (WCP_AUDIT_USER_CREA_DTM) <> COALESCE(DATE(WCP_AUDIT_USER_UPDT_DTM), '2999-12-31'))), 
FILTER_PP                             as ( SELECT * FROM    RENAME_PP  WHERE PP_VOID_IND='N' ),
FILTER_EMP                            as ( SELECT * FROM    RENAME_EMP   ),
FILTER_IND                            as ( SELECT * FROM    RENAME_IND   ),
FILTER_U                              as ( SELECT * FROM    RENAME_U   ),
FILTER_CLS                            as ( SELECT * FROM    RENAME_CLS   ),
FILTER_WCRT                           as ( SELECT * FROM    RENAME_WCRT 
                                            WHERE WC_CLS_RT_TIER_VOID_IND = 'N'  ),
FILTER_PA                             as ( SELECT * FROM    RENAME_PA 
                                            WHERE PA_VOID_IND = 'N' AND PLCY_PRD_AUDT_DTL_COMP_DT IS NOT NULL 
                                            QUALIFY (ROW_NUMBER() OVER (PARTITION BY PA_PLCY_PRD_ID ORDER BY PLCY_PRD_AUDT_DTL_COMP_DT)) = 1 ),

FILTER_PASH                          as ( SELECT PASH.PLCY_PRD_AUDT_DTL_ID,PASH.PLCY_AUDT_STS_TYP_CD,PASH.PLCY_AUDT_STS_TYP_NM,PASH.PLCY_AUDT_STS_RSN_TYP_NM,PASH.AUDIT_STS_EFF_DATE,PASH.AUDIT_STS_END_DATE,
                                                 CAST(MIN ( CASE WHEN PASH.PLCY_AUDT_STS_TYP_CD = 'COMP' THEN PASH.HIST_EFF_DTM1 END) OVER (PARTITION BY PA.pa_PLCY_PRD_ID)  AS DATE) AS POLICY_AUDIT_COMPLETE_DATE
                                            FROM  RENAME_PASH  PASH
                                               left join FILTER_PA PA on PA.PLCY_PRD_AUDT_DTL_ID=PASH.PLCY_PRD_AUDT_DTL_ID ),
FILTER_PASH2                          as ( SELECT * FROM    RENAME_PASH2 
                                            WHERE PASH2_AUDIT_STS_END_DATE IS NULL AND  PASH2_PLCY_AUDT_STS_TYP_CD NOT IN ('VOID', 'SEL', 'ASGN')  ),
FILTER_PR                             as ( SELECT * FROM    RENAME_PR   ),
FILTER_RP                             as ( SELECT * FROM    RENAME_RP  ),
FILTER_PT                             as ( SELECT * FROM    RENAME_PT 
                                            WHERE PT_VOID_IND = 'N' AND PT_CTL_ELEM_TYP_CD = 'PLCY_TYP'  ),
FILTER_PSRH                           as ( SELECT * FROM    RENAME_PSRH 
                                            WHERE MOST_RCNT_PLCY_STS_RSN_IND = 'Y'  ),
FILTER_WCSX                           as ( SELECT * FROM    RENAME_WCSX 
                                            WHERE WCSX_VOID_IND = 'N'  ),
FILTER_PPPD                           as ( SELECT * FROM    RENAME_PPPD 
                                            WHERE WRITTEN_PREMIUM_ELEMENT_DESC not in ('BWC ADMINISTRATIVE COST', 'IC ADMINISTRATIVE COST', 'DWRF', 'DWRF II', 'POLICY MINIMUM PREMIUM', 'ESTIMATED ANNUAL PREMIUM', 'TOTAL AMOUNT DUE', 'TOTAL REPORTED PREMIUM', 'TOTAL DEFFERED REPORTED ASSESSMENT', 'TOTAL REPORTED AMOUNT DUE')  
                                            -----------------------------------------------------------------------------------------------------------------------------------------------				  
                                             -- THIS IS TO FILTER INTRA DAY HISTORY CHANGES. i.e. to pick the latest history record if multiple row exists for a day.
			                                      ------------------------------------------------------------------------------------------------------------------------------------------------	 
                                            QUALIFY(ROW_NUMBER() OVER (PARTITION BY PLCY_PRD_ID, AUDIT_USER_CREA_DTM::DATE ORDER BY AUDIT_USER_CREA_DTM DESC, NVL(AUDIT_USER_UPDT_DTM, CURRENT_DATE) DESC )) =1  
                                          ),
FILTER_P                              as ( SELECT * FROM    RENAME_P   ),
FILTER_RE                             as ( SELECT * FROM    RENAME_RE   ),

---- JOIN LAYER ----

WCP_SRT as ( SELECT  * , IFF(RATING_PLAN_CODE ='CLS_RT_TIER', 'BASE_RATE', RATING_PLAN_CODE) AS DERIVED_RATING_PLAN_CODE
                   , IFF(RATING_PLAN_CODE ='CLS_RT_TIER', 'BASE RATE', RATING_PLAN_DESC) AS DERIVED_RATING_PLAN_DESC
                   , MAX(PLCY_PRD_AUDT_DTL_COMP_DT) OVER (PARTITION BY PREM_PLCY_PRD_ID) AS EXCLUDE_DATE
                  , MIN(AUDIT_STS_EFF_DATE) OVER(PARTITION BY FILTER_PA.PA_PLCY_PRD_ID) AS MIN_STS_DT
           FROM  FILTER_PREM
                    INNER JOIN FILTER_WCP ON  FILTER_PREM.PREM_PRD_ID = FILTER_WCP.WCP_PREM_PRD_ID 
                    INNER JOIN FILTER_PP ON  FILTER_PREM.PREM_PLCY_PRD_ID =  FILTER_PP.PP_PLCY_PRD_ID
                    LEFT JOIN FILTER_PA  ON  FILTER_PREM.PREM_PLCY_PRD_ID =  FILTER_PA.PA_PLCY_PRD_ID AND FILTER_PREM.PREM_TYP_CD = 'A'
                    LEFT JOIN FILTER_RP ON  FILTER_PREM.PREM_PLCY_PRD_ID =  FILTER_RP.RP_PLCY_PRD_ID AND FILTER_RP.PPRE_EFF_DT = FILTER_PREM.PREM_PRD_EFF_DT 
                                        AND FILTER_WCP.WCP_AUDIT_USER_CREA_DTM >= FILTER_RP.RATING_PLAN_EFFECTIVE_DATE AND FILTER_WCP.WCP_AUDIT_USER_CREA_DTM < coalesce(DATE(FILTER_RP.RATING_PLAN_END_DATE), '2999-12-31')
                    LEFT JOIN FILTER_PT ON  FILTER_PREM.PREM_PLCY_PRD_ID =  FILTER_PT.PT_PLCY_PRD_ID
                    LEFT JOIN FILTER_PSRH  ON  FILTER_PP.AGRE_ID= FILTER_PSRH.PSRH_AGRE_ID
                    LEFT JOIN FILTER_EMP ON  FILTER_WCP.EMPLOYER_CUSTOMER_ID =  FILTER_EMP.EMP_CUST_ID 
                    LEFT JOIN FILTER_IND ON  FILTER_WCP.PTCP_ID_COV =  FILTER_IND.IND_PTCP_ID 
                    LEFT JOIN FILTER_U ON  FILTER_WCP.CREATE_USER_ID =  FILTER_U.USER_ID 
                    LEFT JOIN FILTER_CLS ON  FILTER_WCP.WC_CLS_SUFX_ID =  FILTER_CLS.CLS_WC_CLS_SUFX_ID
                    LEFT JOIN FILTER_PR ON  FILTER_WCP.PYRL_RPT_ID =  FILTER_PR.PR_PYRL_RPT_ID AND PYRL_RPT_STS_TYP_NM ='COMPLETED'
                    LEFT JOIN FILTER_WCRT ON FILTER_CLS.WC_CLS_ID = FILTER_WCRT.WCRT_WC_CLS_ID 
                                          AND FILTER_WCRT.WC_CLS_RT_TIER_EFF_DT >= FILTER_WCP.WC_COV_PREM_EFF_DT 
                                          AND FILTER_WCRT.WC_CLS_RT_TIER_EFF_DT <= DATE(FILTER_WCP.WC_COV_PREM_END_DT)-1
                                          AND FILTER_CLS.DERIVED_WC_CLS_SUFX_CLS_SUFX = FILTER_WCRT.WC_CLS_RT_TIER_TYP_CD
                    LEFT JOIN FILTER_WCSX ON FILTER_CLS.WC_CLS_ID = FILTER_WCSX.WC_CLS_SIC_XREF_CLS_CD
                    LEFT JOIN FILTER_PASH ON FILTER_PA.PLCY_PRD_AUDT_DTL_ID = FILTER_PASH.PLCY_PRD_AUDT_DTL_ID AND DERIVED_PREM_TYP_CD =  3 
                                                                                                               AND FILTER_WCP.WCP_AUDIT_USER_CREA_DTM BETWEEN FILTER_PASH.AUDIT_STS_EFF_DATE AND coalesce(DATE(FILTER_PASH.AUDIT_STS_END_DATE), '2999-12-31')
                    LEFT JOIN FILTER_PASH2 ON FILTER_PA.PLCY_PRD_AUDT_DTL_ID = FILTER_PASH2. PASH2_PLCY_PRD_AUDT_DTL_ID
                  )--,
, WCP AS (SELECT *
      ,  MAX(DERIVED_PREM_TYP_CD) OVER(PARTITION BY PREM_PLCY_PRD_ID, PREM_PRD_EFF_DT ORDER BY PREM_PLCY_PRD_ID, PREM_PRD_EFF_DT) DRVD_PRM_ORDR
         FROM WCP_SRT 
         WHERE (AUDIT_STS_EFF_DATE=MIN_STS_DT OR  PREM_TYP_CD <> 'A')  )

, WCP_FINAL AS (
SELECT *   
  , ROW_NUMBER() OVER(PARTITION BY PREM_PLCY_PRD_ID, WCP_RISK_ID, PREM_PRD_EFF_DT, COVERED_INDIVIDUAL_CUST_ID, WC_CLS_SUFX_ID,WCP_AUDIT_USER_CREA_DTM::DATE ORDER BY WCP_AUDIT_USER_CREA_DTM DESC, NVL(WCP_AUDIT_USER_UPDT_DTM, CURRENT_DATE) DESC,  WCP_COV_PRD_ID DESC) ROWN
 FROM WCP
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--1. Exclude all records with Audited Premium Type (PREM_TYP_CD = 'A') where the Exposure Amount is null. This indicates that the audit is not yet completed.
--2. Exclude all records with Estimated Premium Type (PREM_TYP_CD = 'E') that were created on or after the Audit was completed. The Audited records take precedence in the hierarchy.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
WHERE ((PREM_TYP_CD = 'A') OR (PREM_TYP_CD = 'E' AND PREM_AUDIT_USER_CREA_DTM < NVL(EXCLUDE_DATE, CURRENT_DATE)) OR (PREM_TYP_CD = 'R' AND PYRL_RPT_STS_TYP_NM ='COMPLETED'))
      AND DRVD_PRM_ORDR = DERIVED_PREM_TYP_CD   )

, PPPD as ( SELECT *
                FROM  FILTER_PPPD
                    INNER JOIN FILTER_PP ON  FILTER_PPPD.PLCY_PRD_ID = FILTER_PP.PP_PLCY_PRD_ID 
                    INNER JOIN FILTER_P ON  FILTER_PP.AGRE_ID = FILTER_P.P_AGRE_ID
                    LEFT JOIN FILTER_PT ON  FILTER_PPPD.PLCY_PRD_ID =  FILTER_PT.PT_PLCY_PRD_ID 
                    LEFT JOIN FILTER_PSRH ON  FILTER_PP.AGRE_ID= FILTER_PSRH.PSRH_AGRE_ID
                    LEFT JOIN FILTER_U ON  FILTER_PPPD.CREATE_USER_ID =  FILTER_U.USER_ID 
                    LEFT JOIN FILTER_RE ON  FILTER_PPPD.WRITTEN_PREMIUM_ELEMENT_CODE =  FILTER_RE.RE_RT_ELEM_TYP_CD  
                    WHERE FILTER_PPPD.WC_COV_PREM_EFF_DT <=  NVL(WC_COV_PREM_END_DT, CURRENT_DATE) -- SHODDY SOURCE DATA EX: PLCY_PRD_ID = 13567461
          )
                
---- UNION LAYER ----
,
ETL AS (
SELECT
    PLCY_NO
  , AGRE_ID
  , PP_PLCY_PRD_ID AS PLCY_PRD_ID 
  , PLCY_PRD_EFF_DT
  , PLCY_PRD_END_DT
  , EMPLOYER_CUSTOMER_ID
  , EMPLOYER_CUSTOMER_NUMBER
  , PREM_PRD_ID
  , PREM_PRD_EFF_DT  
  , PREM_PRD_END_DT
  , PREM_TYP_CD
  , PREM_TYP_NM
  ,  CASE WHEN MIN(PREMIUM_CALCULATION_EFFECTIVE_DATE) OVER (PARTITION BY PLCY_PRD_ID, WCP_RISK_ID, PREM_PRD_EFF_DT, COVERED_INDIVIDUAL_CUST_ID, WC_CLS_SUFX_ID ORDER BY PREM_PRD_EFF_DT, DERIVED_PREM_TYP_CD ) = PREMIUM_CALCULATION_EFFECTIVE_DATE
                                        AND  PREM_TYP_CD = 'E' THEN 'ORIGINAL ESTIMATE'
          WHEN PREM_TYP_CD = 'R' AND PYRL_RPT_EST_RPT_IND = 'Y' THEN 'ESTIMATED PAYROLL REPORT'
          WHEN PREM_TYP_CD = 'R' AND PYRL_RPT_EST_RPT_IND = 'N' THEN 'PAYROLL REPORT'
          WHEN PREM_TYP_CD = 'A' AND PLCY_AUDT_TYP_NM = 'FIELD' AND LAG(PREM_TYP_CD) OVER (PARTITION BY  PLCY_PRD_ID, WCP_RISK_ID, PREM_PRD_EFF_DT, COVERED_INDIVIDUAL_CUST_ID, WC_CLS_SUFX_ID ORDER BY PREM_PRD_EFF_DT) = 'E' 
               AND PLCY_AUDT_STS_TYP_NM <> 'COMPLETED' THEN 'FIELD AUDIT ASSIGNED'
          WHEN PREM_TYP_CD = 'A' AND PLCY_AUDT_TYP_NM = 'FIELD' AND LAG(PREM_TYP_CD) OVER (PARTITION BY  PLCY_PRD_ID, WCP_RISK_ID, PREM_PRD_EFF_DT, COVERED_INDIVIDUAL_CUST_ID, WC_CLS_SUFX_ID ORDER BY PREM_PRD_EFF_DT) = 'E' 
               THEN 'FIELD AUDIT COMPLETED'
          WHEN PREM_TYP_CD = 'A' AND PLCY_AUDT_TYP_NM = 'TRUE-UP' AND LAG(PREM_TYP_CD) OVER (PARTITION BY  PLCY_PRD_ID, WCP_RISK_ID, PREM_PRD_EFF_DT, COVERED_INDIVIDUAL_CUST_ID, WC_CLS_SUFX_ID ORDER BY PREM_PRD_EFF_DT) = 'E' 
               AND PLCY_AUDT_STS_TYP_NM <> 'COMPLETED' THEN 'TRUE UP AUDIT ASSIGNED'
          WHEN PREM_TYP_CD = 'A' AND PLCY_AUDT_TYP_NM = 'TRUE-UP' AND LAG(PREM_TYP_CD) OVER (PARTITION BY  PLCY_PRD_ID, WCP_RISK_ID, PREM_PRD_EFF_DT, COVERED_INDIVIDUAL_CUST_ID, WC_CLS_SUFX_ID ORDER BY PREM_PRD_EFF_DT) = 'E' 
               THEN 'TRUE UP AUDIT COMPLETED'
          WHEN EXPOSURE_AMOUNT <> LAG(EXPOSURE_AMOUNT) OVER (PARTITION BY  PLCY_PRD_ID, WCP_RISK_ID, PREM_PRD_EFF_DT, COVERED_INDIVIDUAL_CUST_ID, WC_CLS_SUFX_ID ORDER BY PREM_PRD_EFF_DT) 
                                     AND PURE_PREMIUM_RATE <> LAG(PURE_PREMIUM_RATE) OVER (PARTITION BY  PLCY_PRD_ID, WCP_RISK_ID, PREM_PRD_EFF_DT, COVERED_INDIVIDUAL_CUST_ID, WC_CLS_SUFX_ID ORDER BY PREM_PRD_EFF_DT) 
               THEN 'MULTIPLE ADJUSTMENTS'
          WHEN RATING_PLAN_RATE <> LAG(RATING_PLAN_RATE) OVER (PARTITION BY  PLCY_PRD_ID, WCP_RISK_ID, PREM_PRD_EFF_DT, COVERED_INDIVIDUAL_CUST_ID, WC_CLS_SUFX_ID ORDER BY PREM_PRD_EFF_DT) 
                                  OR  RATING_PLAN_CODE <> LAG(RATING_PLAN_CODE) OVER (PARTITION BY  PLCY_PRD_ID, WCP_RISK_ID, PREM_PRD_EFF_DT, COVERED_INDIVIDUAL_CUST_ID, WC_CLS_SUFX_ID ORDER BY PREM_PRD_EFF_DT) 
                THEN 'RATING PLAN ADJUSTMENT'
          WHEN CLASS_CODE_BASE_RATE <> LAG(CLASS_CODE_BASE_RATE) OVER (PARTITION BY  PLCY_PRD_ID, WCP_RISK_ID, PREM_PRD_EFF_DT, COVERED_INDIVIDUAL_CUST_ID, WC_CLS_SUFX_ID ORDER BY PREM_PRD_EFF_DT) 
               THEN 'BASE RATE ADJUSTMENT'
          WHEN PURE_PREMIUM_RATE <> LAG(PURE_PREMIUM_RATE) OVER (PARTITION BY  PLCY_PRD_ID, WCP_RISK_ID, PREM_PRD_EFF_DT, COVERED_INDIVIDUAL_CUST_ID, WC_CLS_SUFX_ID ORDER BY PREM_PRD_EFF_DT) 
               THEN 'PURE RATE ADJUSTMENT'
          WHEN EXPOSURE_AMOUNT <> LAG(EXPOSURE_AMOUNT) OVER (PARTITION BY  PLCY_PRD_ID, WCP_RISK_ID, PREM_PRD_EFF_DT, COVERED_INDIVIDUAL_CUST_ID, WC_CLS_SUFX_ID ORDER BY PREM_PRD_EFF_DT) 
               THEN 'EXPOSURE ADJUSTMENT' 
          ELSE 'UNKNOWN' END AS PREMIUM_CALCULATION_TYPE_DESC 
  , WCP_COV_PRD_ID
  , RT_TYP_NM
  , WC_COV_PREM_EFF_DT
  , WC_COV_PREM_END_DT
  , WCP_RISK_ID AS RISK_ID
  , PTCP_ID_COV
  , COVERED_INDIVIDUAL_CUST_ID
  , IND_PTCP_ID
  , COVERED_INDIVIDUAL_CUSTOMER_NUMBER
  , COV_TYP_CD
  , COV_TYP_NM
  , TTL_TYP_CD
  , TTL_TYP_NM
  , PPPIE_COV_IND
  , WC_CLS_SUFX_ID
  , WRITTEN_PREMIUM_ELEMENT_CODE
  , WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE
  , WRITTEN_PREMIUM_ELEMENT_DESC
  , WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE
  , WRITTEN_PREMIUM_ELEMENT_END_DATE
  , CLASS_CODE_BASE_RATE
  , EMPLOYEE_COUNT
  , EXPOSURE_AMOUNT
  , ROUND(CLASS_CODE_BASE_RATE * EXPOSURE_AMOUNT / 100, 2) AS BASE_PREMIUM_AMOUNT
  , IFF(EXPRN_MOD_TYPE_DESC IS NULL, DERIVED_RATING_PLAN_DESC, DERIVED_RATING_PLAN_DESC||'-'||EXPRN_MOD_TYPE_DESC) AS RATING_PLAN_DESC
  , EXPRN_MOD_TYPE_DESC
  , DERIVED_RATING_PLAN_CODE AS RATING_PLAN_CODE
  , RATING_PLAN_RATE
  , PURE_PREMIUM_RATE
  , PURE_PREMIUM_AMOUNT
  , BWC_ASSESSMENT_FEE_RATE
  , ROUND(BWC_ASSESSMENT_FEE_RATE * EXPOSURE_AMOUNT / 100) AS BWC_ASSESSMENT_FEE_AMOUNT
  , IC_ASSESSMENT_FEE_RATE
  , ROUND(BWC_ASSESSMENT_FEE_RATE * EXPOSURE_AMOUNT / 100) AS IC_ASSESSMENT_FEE_AMOUNT
  , DWRF_FEE_RATE
  , ROUND(BWC_ASSESSMENT_FEE_RATE * EXPOSURE_AMOUNT / 100) AS DWRF_FEE_AMOUNT
  , DWRF_II_FEE_RATE
  , ROUND(BWC_ASSESSMENT_FEE_RATE * EXPOSURE_AMOUNT / 100) AS DWRF_II_FEE_AMOUNT
  , TOTAL_DERIVED_PREMIUM_AMOUNT
  , CREATE_USER_ID
  , CREATE_USER_LOGIN_NAME
  , CREATE_USER_NAME
  , PREMIUM_CALCULATION_EFFECTIVE_DATE 
  , LEAD(WCP_AUDIT_USER_CREA_DTM) OVER (PARTITION BY PLCY_PRD_ID, WCP_RISK_ID, PREM_PRD_EFF_DT, COVERED_INDIVIDUAL_CUST_ID, WC_CLS_SUFX_ID ORDER BY WCP_AUDIT_USER_CREA_DTM) - 1 AS PREMIUM_CALCULATION_END_DATE
  , CASE WHEN PREMIUM_CALCULATION_END_DATE IS NULL THEN 'Y' ELSE 'N' END AS CURRENT_PREMIUM_CALCULATION_IND
  , POLICY_TYPE_CODE
  , POLICY_TYPE_DESC
  , CASE WHEN POLICY_TYPE_CODE = 'PEC' THEN 'Y' ELSE 'N' END AS PEC_POLICY_IND
  , NEW_POLICY_IND
  , PSRH_PLCY_PRD_ID
  , CASE WHEN PLCY_STS_TYP_CD IS NULL THEN 'N/A' ELSE PLCY_STS_TYP_CD END AS PLCY_STS_TYP_CD
  , CASE WHEN PLCY_STS_TYP_NM IS NULL THEN 'N/A' ELSE PLCY_STS_TYP_NM END AS PLCY_STS_TYP_NM
  , CASE WHEN PLCY_STS_RSN_TYP_CD IS NULL THEN 'N/A' ELSE PLCY_STS_RSN_TYP_CD END AS PLCY_STS_RSN_TYP_CD
  , CASE WHEN PLCY_STS_RSN_TYP_NM IS NULL THEN 'N/A' ELSE PLCY_STS_RSN_TYP_NM END AS PLCY_STS_RSN_TYP_NM
  , CASE WHEN PLCY_STS_TYP_CD IN ('ACT', 'EXP') THEN 'Y' ELSE 'N' END AS POLICY_ACTIVE_IND 
  , PLCY_STS_TRANS_DT
  , PLCY_STS_RSN_TRANS_DT
  , NULL AS RE_RT_ELEM_TYP_CD
  , SIC_TYP_CD
  , PLCY_AUDT_TYP_CD as PLCY_AUDT_TYP_CD
  , PLCY_AUDT_TYP_NM as PLCY_AUDT_TYP_NM
  , POLICY_AUDIT_COMPLETE_DATE
FROM WCP_FINAL
WHERE ROWN= 1
UNION 
SELECT    
    PLCY_NO
  , AGRE_ID
  , PLCY_PRD_ID
  , PLCY_PRD_EFF_DT
  , PLCY_PRD_END_DT
  , EMPLOYER_CUSTOMER_ID
  , EMPLOYER_CUSTOMER_NUMBER
  , NULL AS PREM_PRD_ID
  , PREM_PRD_EFF_DT
  , PREM_PRD_END_DT
  , PREM_TYP_CD
  , PREM_TYP_NM
  , 'DISCOUNT OR CREDIT' AS PREMIUM_CALCULATION_TYPE_DESC
  , NULL AS WCP_COV_PRD_ID
  , 'EMPLOYEE' AS RT_TYP_NM
  , WC_COV_PREM_EFF_DT
  , WC_COV_PREM_END_DT
  , NULL AS RISK_ID
  , NULL AS PTCP_ID_COV
  , COVERED_INDIVIDUAL_CUST_ID
  , NULL AS IND_PTCP_ID
  , COVERED_INDIVIDUAL_CUSTOMER_NUMBER
  , NULL AS COV_TYP_CD
  , NULL AS COV_TYP_NM
  , NULL AS TTL_TYP_CD
  , NULL AS TTL_TYP_NM
  , NULL AS PPPIE_COV_IND
  , NULL AS WC_CLS_SUFX_ID
  , WRITTEN_PREMIUM_ELEMENT_CODE
  , NULL AS WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE
  , WRITTEN_PREMIUM_ELEMENT_DESC
  , WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE
  , WRITTEN_PREMIUM_ELEMENT_END_DATE
  , NULL AS CLASS_CODE_BASE_RATE
  , NULL AS EMPLOYEE_COUNT
  , NULL AS EXPOSURE_AMOUNT
  , NULL AS BASE_PREMIUM_AMOUNT
  , NULL AS RATING_PLAN_DESC
  , NULL AS EXPRN_MOD_TYPE_DESC
  , NULL AS RATING_PLAN_CODE
  , NULL AS RATING_PLAN_RATE
  , NULL AS PURE_PREMIUM_RATE
  , NULL AS PURE_PREMIUM_AMOUNT
  , NULL AS BWC_ASSESSMENT_FEE_RATE
  , NULL AS BWC_ASSESSMENT_FEE_AMOUNT
  , NULL AS IC_ASSESSMENT_FEE_RATE
  , NULL AS IC_ASSESSMENT_FEE_AMOUNT
  , NULL AS DWRF_FEE_RATE
  , NULL AS DWRF_FEE_AMOUNT
  , NULL AS DWRF_II_FEE_RATE
  , NULL AS DWRF_II_FEE_AMOUNT
  , TOTAL_DERIVED_PREMIUM_AMOUNT
  , CREATE_USER_ID
  , CREATE_USER_LOGIN_NAME
  , CREATE_USER_NAME
  , CAST(AUDIT_USER_CREA_DTM AS DATE) AS PREMIUM_CALCULATION_EFFECTIVE_DATE
  , CAST(AUDIT_USER_UPDT_DTM AS DATE) AS PREMIUM_CALCULATION_END_DATE
  , CASE WHEN AUDIT_USER_UPDT_DTM IS NULL THEN 'Y' ELSE 'N' END AS CURRENT_PREMIUM_CALCULATION_IND
  , POLICY_TYPE_CODE
  , POLICY_TYPE_DESC
  , CASE WHEN POLICY_TYPE_CODE = 'PEC' THEN 'Y' ELSE 'N' END AS PEC_POLICY_IND
  , NEW_POLICY_IND
  , PSRH_PLCY_PRD_ID
  , CASE WHEN PLCY_STS_TYP_CD IS NULL THEN 'N/A' ELSE PLCY_STS_TYP_CD END AS PLCY_STS_TYP_CD
  , CASE WHEN PLCY_STS_TYP_NM IS NULL THEN 'N/A' ELSE PLCY_STS_TYP_NM END AS PLCY_STS_TYP_NM
  , CASE WHEN PLCY_STS_RSN_TYP_CD IS NULL THEN 'N/A' ELSE PLCY_STS_RSN_TYP_CD END AS PLCY_STS_RSN_TYP_CD
  , CASE WHEN PLCY_STS_RSN_TYP_NM IS NULL THEN 'N/A' ELSE PLCY_STS_RSN_TYP_NM END AS PLCY_STS_RSN_TYP_NM
  , CASE WHEN PLCY_STS_TYP_CD IN ('ACT', 'EXP') THEN 'Y' ELSE 'N' END AS POLICY_ACTIVE_IND
  , PLCY_STS_TRANS_DT
  , PLCY_STS_RSN_TRANS_DT
  , RE_RT_ELEM_TYP_CD
  , NULL AS SIC_TYP_CD
  , NULL AS PLCY_AUDT_TYP_CD
  , NULL AS PLCY_AUDT_TYP_NM
  , NULL AS POLICY_AUDIT_COMPLETE_DATE
FROM PPPD 
  )
  
SELECT 
	PLCY_NO
, AGRE_ID
, PLCY_PRD_ID
, PLCY_PRD_EFF_DT
, PLCY_PRD_END_DT
, EMPLOYER_CUSTOMER_ID
, EMPLOYER_CUSTOMER_NUMBER
, PREM_PRD_ID
, PREM_PRD_EFF_DT
, PREM_PRD_END_DT
, PREM_TYP_CD
, PREM_TYP_NM
, PREMIUM_CALCULATION_TYPE_DESC
, WCP_COV_PRD_ID
, RT_TYP_NM
, WC_COV_PREM_EFF_DT
, WC_COV_PREM_END_DT
, RISK_ID
, PTCP_ID_COV
, COVERED_INDIVIDUAL_CUST_ID
, IND_PTCP_ID
, COVERED_INDIVIDUAL_CUSTOMER_NUMBER
, COV_TYP_CD
, COV_TYP_NM
, TTL_TYP_CD
, TTL_TYP_NM
, PPPIE_COV_IND
, WC_CLS_SUFX_ID
, WRITTEN_PREMIUM_ELEMENT_CODE
, WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE
, WRITTEN_PREMIUM_ELEMENT_DESC
, WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE
, WRITTEN_PREMIUM_ELEMENT_END_DATE
, CLASS_CODE_BASE_RATE
, EMPLOYEE_COUNT
, EXPOSURE_AMOUNT
, BASE_PREMIUM_AMOUNT
, RATING_PLAN_CODE
, RATING_PLAN_DESC
, EXPRN_MOD_TYPE_DESC
, RATING_PLAN_RATE
, PURE_PREMIUM_RATE
, PURE_PREMIUM_AMOUNT
, BWC_ASSESSMENT_FEE_RATE
, BWC_ASSESSMENT_FEE_AMOUNT
, IC_ASSESSMENT_FEE_RATE
, IC_ASSESSMENT_FEE_AMOUNT
, DWRF_FEE_RATE
, DWRF_FEE_AMOUNT
, DWRF_II_FEE_RATE
, DWRF_II_FEE_AMOUNT
, TOTAL_DERIVED_PREMIUM_AMOUNT
, CREATE_USER_ID
, CREATE_USER_LOGIN_NAME
, CREATE_USER_NAME
, PREMIUM_CALCULATION_EFFECTIVE_DATE
, PREMIUM_CALCULATION_END_DATE
, CURRENT_PREMIUM_CALCULATION_IND
, POLICY_TYPE_CODE
, POLICY_TYPE_DESC
, PEC_POLICY_IND
, NEW_POLICY_IND
, PSRH_PLCY_PRD_ID
, PLCY_STS_TYP_CD
, PLCY_STS_TYP_NM
, PLCY_STS_RSN_TYP_CD
, PLCY_STS_RSN_TYP_NM
, POLICY_ACTIVE_IND
, PLCY_STS_TRANS_DT
, PLCY_STS_RSN_TRANS_DT
, RE_RT_ELEM_TYP_CD
, SIC_TYP_CD
, POLICY_AUDIT_COMPLETE_DATE
, PLCY_AUDT_TYP_CD
, PLCY_AUDT_TYP_NM
 FROM ETL
      );
    