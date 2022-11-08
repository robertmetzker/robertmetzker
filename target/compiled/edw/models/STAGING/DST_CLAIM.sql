---- SRC LAYER ----


WITH
SRC_C as ( SELECT *     from     STAGING.STG_CLAIM ),
SRC_CTH as ( SELECT *     from     STAGING.STG_CLAIM_TYPE_HISTORY ),
SRC_CSH as ( SELECT *     from     STAGING.STG_CLAIM_STATUS_HISTORY ),
SRC_FF as ( SELECT *     from     STAGING.STG_CLAIM_PROFILE ),
--SRC_CE as ( SELECT *     from     STAGING.STG_CLAIM_PROFILE ),
--SRC_CEW as ( SELECT *     from     STAGING.STG_CLAIM_PROFILE ),
--SRC_CHCW as ( SELECT *     from     STAGING.STG_CLAIM_PROFILE ),
SRC_CAN as ( SELECT *     from     STAGING.STG_CLAIM_ALIAS_NUMBER ),
--SRC_SB as ( SELECT *     from     STAGING.STG_CLAIM_PROFILE ),
--SRC_PREM1 as ( SELECT *     from     STAGING.STG_CLAIM_PROFILE ),
--SRC_PREM2 as ( SELECT *     from     STAGING.STG_CLAIM_PROFILE ),
--SRC_KE as ( SELECT *     from     STAGING.STG_CLAIM_PROFILE ),
--SRC_KT as ( SELECT *     from     STAGING.STG_CLAIM_PROFILE ),
--SRC_KSTT as ( SELECT *     from     STAGING.STG_CLAIM_PROFILE ),
--SRC_KSTP as ( SELECT *     from     STAGING.STG_CLAIM_PROFILE ),
--SRC_KRSN as ( SELECT *     from     STAGING.STG_CLAIM_PROFILE ),
SRC_CH as ( SELECT *     from     STAGING.STG_CLAIM_HISTORY ),
--SRC_CHCLMT as ( SELECT *     from     STAGING.STG_CLAIM_HISTORY ),
--SRC_CHEMPLR as ( SELECT *     from     STAGING.STG_CLAIM_HISTORY ),
SRC_CS as ( SELECT *     from     STAGING.STG_CLAIM_SUMMARY ),
SRC_WC as ( SELECT *     from     STAGING.STG_WC_CLASS_SIC_XREF ),
--SRC_SCSH as ( SELECT *     from     STAGING.STG_CLAIM_STATUS_HISTORY ),
SRC_UCS as ( SELECT *     from     STAGING.STG_USERS ),
SRC_ASG as ( SELECT *     from     STAGING.STG_ASSIGNMENT ),
SRC_CDM as ( SELECT *     from     STAGING.STG_CLAIM_DISABILITY_MANAGEMENT ),
--SRC_CDMEND as ( SELECT *     from     STAGING.STG_CLAIM_DISABILITY_MANAGEMENT ),
--SRC_CSST as ( SELECT *     from     STAGING.STG_CLAIM_STATUS_HISTORY ),
--SRC_U as ( SELECT *     from     STAGING.STG_USERS ),
SRC_CIS as ( SELECT *     from     STAGING.STG_CLAIM_ICD_STATUS ),
--SRC_CRD as ( SELECT *     from     STAGING.STG_CLAIM_STATUS_HISTORY ),
SRC_PART as ( SELECT *     from     STAGING.STG_PARTICIPATION ),
--SRC_BC as ( SELECT *     from     STAGING.STG_BUSINESS_CUSTOMER ),
SRC_P as ( SELECT *     from     STAGING.STG_POLICY ),
SRC_IW as ( SELECT *     from     STAGING.STG_PERSON ),
SRC_CPH as ( SELECT *     from     STAGING.DSV_CLAIM_POLICY_HISTORY ),
SRC_PSR as ( SELECT *     from     STAGING.STG_POLICY_STATUS_REASON_HISTORY ),
SRC_BILL as ( SELECT *     from     STAGING.STG_INVOICE_HEADER ),
SRC_IL as ( SELECT *     from     STAGING.STG_INVOICE_LINE ),
SRC_RI as ( SELECT *     from     STAGING.STG_CUSTOMER_ROLE_IDENTIFIER ),
SRC_CFT as ( SELECT *     from     STAGING.STG_CLAIM_FINANCIAL_TRANSACTION ),
SRC_COD as ( SELECT *     FROM     STAGING.STG_CLAIM_OTHER_DATE ),
SRC_CPP as ( SELECT *     from     STAGING.STG_CLAIM_PROVIDER_PARTICIPATION ),
SRC_CP as ( SELECT *     from     STAGING.STG_CLAIM_PARTICIPATION ),
//SRC_C as ( SELECT *     from     STG_CLAIM) ,
//SRC_CTH as ( SELECT *     from     STG_CLAIM_TYPE_HISTORY) ,
//SRC_CSH as ( SELECT *     from     STG_CLAIM_STATUS_HISTORY) ,
//SRC_FF as ( SELECT *     from     STG_CLAIM_PROFILE) ,
//SRC_CE as ( SELECT *     from     STG_CLAIM_PROFILE) ,
//SRC_CEW as ( SELECT *     from     STG_CLAIM_PROFILE) ,
//SRC_CHCW as ( SELECT *     from     STG_CLAIM_PROFILE) ,
//SRC_CAN as ( SELECT *     from     STG_CLAIM_ALIAS_NUMBER) ,
//SRC_SB as ( SELECT *     from     STG_CLAIM_PROFILE) ,
//SRC_PREM1 as ( SELECT *     from     STG_CLAIM_PROFILE) ,
//SRC_PREM2 as ( SELECT *     from     STG_CLAIM_PROFILE) ,
//SRC_KE as ( SELECT *     from     STG_CLAIM_PROFILE) ,
//SRC_KT as ( SELECT *     from     STG_CLAIM_PROFILE) ,
//SRC_KSTT as ( SELECT *     from     STG_CLAIM_PROFILE) ,
//SRC_KSTP as ( SELECT *     from     STG_CLAIM_PROFILE) ,
//SRC_KRSN as ( SELECT *     from     STG_CLAIM_PROFILE) ,
//SRC_CH as ( SELECT *     from     STG_CLAIM_HISTORY) ,
//SRC_CHCLMT as ( SELECT *     from     STG_CLAIM_HISTORY) ,
//SRC_CHEMPLR as ( SELECT *     from     STG_CLAIM_HISTORY) ,
//SRC_CS as ( SELECT *     from     STG_CLAIM_SUMMARY) ,
//SRC_WC as ( SELECT *     from     STG_WC_CLASS_SIC_XREF) ,
//SRC_SCSH as ( SELECT *     from     STG_CLAIM_STATUS_HISTORY) ,
//SRC_UCS as ( SELECT *     from     STG_USERS) ,
//SRC_ASG as ( SELECT *     from     STG_ASSIGNMENT) ,
//SRC_CDM as ( SELECT *     from     STG_CLAIM_DISABILITY_MANAGEMENT) ,
//SRC_CDMEND as ( SELECT *     from     STG_CLAIM_DISABILITY_MANAGEMENT) ,
//SRC_CSST as ( SELECT *     from     STG_CLAIM_STATUS_HISTORY) ,
//SRC_U as ( SELECT *     from     STG_USERS) ,
//SRC_CIS as ( SELECT *     from     STG_CLAIM_ICD_STATUS) ,
//SRC_CRD as ( SELECT *     from     STG_CLAIM_STATUS_HISTORY) ,
//SRC_PART as ( SELECT *     from     STG_PARTICIPATION) ,
//SRC_BC as ( SELECT *     from     STG_BUSINESS_CUSTOMER) ,
//SRC_IW as ( SELECT *     from     STG_PERSON) ,
//SRC_CPH as ( SELECT *     from     STG_CLAIM_POLICY_HISTORY) ,
//SRC_PSR as ( SELECT *     from     STG_POLICY_STATUS_REASON_HISTORY) ,
//SRC_BILL as ( SELECT *     from     STG_INVOICE_HEADER) ,
//SRC_IL as ( SELECT *     from     STG_INVOICE_LINE) ,
//SRC_RI as ( SELECT *     from     STG_CUSTOMER_ROLE_IDENTIFIER) ,
//SRC_CFT as ( SELECT *     from     STG_CLAIM_FINANCIAL_TRANSACTION) ,
//SRC_CPP as ( SELECT *     from     STG_CLAIM_PROVIDER_PARTICIPATION) ,
//SRC_CP as ( SELECT *     from     STG_CLAIM_PARTICIPATION) ,


---- LOGIC LAYER ----

LOGIC_C as ( SELECT 
		  TRIM( CLM_NO )                                     as                                             CLM_NO 
		, TRIM( CLM_TYP_CD )                                 as                                         CLM_TYP_CD 
		, TRIM( CLM_TYP_NM )                                 as                                         CLM_TYP_NM 
		, TRIM( CLM_LOSS_DESC )                              as                                      CLM_LOSS_DESC 
		, TRIM( OCCR_SRC_TYP_NM )                            as                                    OCCR_SRC_TYP_NM 
		, TRIM( OCCR_MEDA_TYP_NM )                           as                                   OCCR_MEDA_TYP_NM 
		, TRIM( OCCR_PRMS_TYP_NM )                           as                                   OCCR_PRMS_TYP_NM 
		, TRIM( CLM_CTRPH_INJR_IND )                         as                                 CLM_CTRPH_INJR_IND 
		, TRIM( CLM_OCCR_LOC_STR_1 )                         as                                 CLM_OCCR_LOC_STR_1 
		, TRIM( CLM_OCCR_LOC_STR_2 )                         as                                 CLM_OCCR_LOC_STR_2 
		, TRIM( CLM_OCCR_LOC_NM )                            as                                    CLM_OCCR_LOC_NM 
		, TRIM( CLM_OCCR_LOC_CITY_NM )                       as                               CLM_OCCR_LOC_CITY_NM 
		, TRIM( CLM_OCCR_LOC_POST_CD )                       as                               CLM_OCCR_LOC_POST_CD 
		, TRIM( CLM_OCCR_LOC_CNTY_NM )                       as                               CLM_OCCR_LOC_CNTY_NM 
		, TRIM( CLM_OCCR_LOC_STT_CD )                        as                                CLM_OCCR_LOC_STT_CD 
		, TRIM( CLM_OCCR_LOC_STT_NM )                        as                                CLM_OCCR_LOC_STT_NM 
		, TRIM( CLM_OCCR_LOC_CNTRY_NM )                      as                              CLM_OCCR_LOC_CNTRY_NM 
		, TRIM( CLM_OCCR_LOC_COMT )                          as                                  CLM_OCCR_LOC_COMT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( CLM_REL_SNPSHT_IND )                         as                                 CLM_REL_SNPSHT_IND 
		, TRIM( NOI_CTG_TYP_NM )                             as                                     NOI_CTG_TYP_NM 
		, TRIM( NOI_TYP_NM )                                 as                                         NOI_TYP_NM 
		, CLM_OCCR_RPT_DATE                                  as                                  CLM_OCCR_RPT_DATE 
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, CLM_FST_DCSN_DATE                                  as                                  CLM_FST_DCSN_DATE 
		, CLM_CLMT_LST_WK_DATE                               as                               CLM_CLMT_LST_WK_DATE 
		, TRIM( CLM_CLMT_JOB_TTL  )                          as                                   CLM_CLMT_JOB_TTL
		, TRIM( CTRPH_INJR_TYP_CD )                          as                                  CTRPH_INJR_TYP_CD 
		, TRIM( CTRPH_INJR_TYP_NM )                          as                                  CTRPH_INJR_TYP_NM 
		--, TRIM( CLM_REL_SNPSHT_IND )                         as                                 CLM_REL_SNPSHT_IND  
 


				from SRC_C
            ),
LOGIC_CTH as ( SELECT 
AGRE_ID                                            as                                            AGRE_ID 
		from SRC_CTH
            group by 1 
            having count(distinct CLM_TYP_CD) > 1
            ),


LOGIC_CSH as ( SELECT 
		  TRIM( CLM_STT_TYP_CD )                             as                                     CLM_STT_TYP_CD 
		, TRIM( CLM_STT_TYP_NM )                             as                                     CLM_STT_TYP_NM 
		, TRIM( CLM_STS_TYP_CD )                             as                                     CLM_STS_TYP_CD 
		, TRIM( CLM_STS_TYP_NM )                             as                                     CLM_STS_TYP_NM 
		, TRIM( CLM_TRANS_RSN_TYP_CD )                       as                               CLM_TRANS_RSN_TYP_CD 
		, TRIM( CLM_TRANS_RSN_TYP_NM )                       as                               CLM_TRANS_RSN_TYP_NM 
		, AGRE_ID                                            as                                            AGRE_ID 
		, CLM_CLM_STS_STT_DT                                 as                                 CLM_CLM_STS_STT_DT 
		, HIST_END_DTM                                       as                                       HIST_END_DTM 
		from SRC_CSH
            ),
LOGIC_FF as ( SELECT 
		  TRIM( CLM_PRFL_ANSW_TEXT )                         as                                 CLM_PRFL_ANSW_TEXT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_FF
            ),
LOGIC_CE as ( SELECT 
		  TRIM( CLM_PRFL_ANSW_TEXT )                         as                                 CLM_PRFL_ANSW_TEXT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_FF --was SRC_CE
            ),
LOGIC_CEW as ( SELECT 
		  TRIM( CLM_PRFL_ANSW_TEXT )                         as                                 CLM_PRFL_ANSW_TEXT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_FF --was SRC_CEW
            ),
LOGIC_CHCW as ( SELECT 
		  TRIM( CLM_PRFL_ANSW_TEXT )                         as                                 CLM_PRFL_ANSW_TEXT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_FF --was SRC_CHCW
            ),
LOGIC_CAN as ( SELECT 
		 CASE WHEN TRIM( CLM_ALIAS_NO_NO ) IS NOT NULL
              THEN 'Y' ELSE 'N' END                          as                                 COMBINED_CLAIM_IND 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, TRIM( CLM_ALIAS_NO_TYP_CD )                        as                                CLM_ALIAS_NO_TYP_CD 
		, TRIM( CLM_ALIAS_NO_NO )                            as                                    CLM_ALIAS_NO_NO 
		from SRC_CAN
            ),
LOGIC_SB as ( SELECT 
		  TRIM( CLM_PRFL_ANSW_TEXT )                         as                                 CLM_PRFL_ANSW_TEXT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_FF --was SRC_SB
            ),
LOGIC_PREM1 as ( SELECT 
		  TRIM( CLM_PRFL_ANSW_TEXT )                         as                                 CLM_PRFL_ANSW_TEXT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
		, TRIM( CLM_PRFL_CTG_TYP_CD )                        as                                CLM_PRFL_CTG_TYP_CD 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_FF --was SRC_PREM1
            ),
LOGIC_PREM2 as ( SELECT 
		  TRIM( CLM_PRFL_ANSW_TEXT )                         as                                 CLM_PRFL_ANSW_TEXT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
		, TRIM( CLM_PRFL_CTG_TYP_CD )                        as                                CLM_PRFL_CTG_TYP_CD 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_FF --was SRC_PREM2
            ),
LOGIC_KE as ( SELECT 
		  TRIM( CLM_PRFL_ANSW_TEXT )                         as                                 CLM_PRFL_ANSW_TEXT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_FF --was SRC_KE
            ),
LOGIC_KT as ( SELECT 
		  TRIM( CLM_PRFL_ANSW_TEXT )                         as                                 CLM_PRFL_ANSW_TEXT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_FF --was SRC_KT
            ),
LOGIC_KSTT as ( SELECT 
		  TRIM( CLM_PRFL_ANSW_TEXT )                         as                                 CLM_PRFL_ANSW_TEXT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_FF --was SRC_KSTT
            ),
LOGIC_KSTP as ( SELECT 
		  TRIM( CLM_PRFL_ANSW_TEXT )                         as                                 CLM_PRFL_ANSW_TEXT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_FF --was SRC_KSTP
            ),
LOGIC_KRSN as ( SELECT 
		  TRIM( CLM_PRFL_ANSW_TEXT )                         as                                 CLM_PRFL_ANSW_TEXT 
		, TRIM( PRFL_SEL_VAL_TYP_NM )                        as                                PRFL_SEL_VAL_TYP_NM 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_FF --was SRC_KRSN
            ),
LOGIC_CH as ( SELECT 
		  CLM_OCCR_RPT_DATE                                  as                                  CLM_OCCR_RPT_DATE 
		, cast( CLM_CLMT_NTF_DT as DATE )                    as                                    CLM_CLMT_NTF_DT 
		, cast( CLM_EMPLR_NTF_DT as DATE )                   as                                   CLM_EMPLR_NTF_DT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( CLM_REL_SNPSHT_IND )                         as                                 CLM_REL_SNPSHT_IND 
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       as                                       HIST_END_DTM 	
		from SRC_CH
            ),
LOGIC_CHCLMT  as ( SELECT 
		  CLM_OCCR_RPT_DATE                                  as                                  CLM_OCCR_RPT_DATE 
		, cast( CLM_CLMT_NTF_DT as DATE )                    as                                    CLM_CLMT_NTF_DT 
		, cast( CLM_EMPLR_NTF_DT as DATE )                   as                                   CLM_EMPLR_NTF_DT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( CLM_REL_SNPSHT_IND )                         as                                 CLM_REL_SNPSHT_IND 
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       as                                       HIST_END_DTM 		
		from SRC_CH --was SRC_CHCLMT
            ),
LOGIC_CHEMPLR as ( SELECT 
		  CLM_OCCR_RPT_DATE                                  as                                  CLM_OCCR_RPT_DATE 
		, cast( CLM_CLMT_NTF_DT as DATE )                    as                                    CLM_CLMT_NTF_DT 
		, cast( CLM_EMPLR_NTF_DT as DATE )                   as                                   CLM_EMPLR_NTF_DT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( CLM_REL_SNPSHT_IND )                         as                                 CLM_REL_SNPSHT_IND 
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       as                                       HIST_END_DTM 	
		from SRC_CH --was SRC_CHEMPLR
            ),			
LOGIC_CS as ( SELECT 
		  TRIM( CS_CLS_CD )                                  as                                          CS_CLS_CD 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, TRIM( CS_CLM_NO )                                  as                                          CS_CLM_NO 
		, CS_EFF_DATE                                        as                                        CS_EFF_DATE 
		, CS_END_DATE                                        as                                        CS_END_DATE 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_CS
            ),
LOGIC_WC as ( SELECT 
       --    CAST(TRIM(REGEXP_REPLACE(SIC_TYP_CD, '[^[:digit:]]', ' ')) AS INT) as               INDUSTRY_GROUP_CODE
		  SIC_TYP_CD                              		     as                                INDUSTRY_GROUP_CODE				 
		, TRIM( WC_CLS_SIC_XREF_CLS_CD )                     as                             WC_CLS_SIC_XREF_CLS_CD 
		from SRC_WC
            ),
LOGIC_SCSH as ( SELECT 
		  cast( HIST_EFF_DTM as DATE )                       as                                       HIST_EFF_DTM 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( CLM_TRANS_RSN_TYP_CD )                       as                               CLM_TRANS_RSN_TYP_CD 
		, TRIM( CSTS_CLM_STS_TYP_CD )                        as                                CSTS_CLM_STS_TYP_CD 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                as                                  AUDIT_USER_ID_UPDT 
		, AUDIT_USER_ID_CREA                                as                                  AUDIT_USER_ID_CREA 
               from SRC_CSH --was SRC_SCSH
            ),
LOGIC_UCS as ( SELECT 
		  TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		, USER_ID                                            as                                            USER_ID 
		from SRC_UCS
            ),
LOGIC_ASG as ( SELECT 
		  ASGN_EFF_DT                                        as                                        ASGN_EFF_DT 
		, TRIM( ORG_UNT_NM )                                 as                                         ORG_UNT_NM 
		, TRIM( ORG_UNT_ABRV_NM )                            as                                    ORG_UNT_ABRV_NM 
        , APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD                 
        , ASGN_PRI_OWNR_IND                                  as                                  ASGN_PRI_OWNR_IND  
        , ASGN_CNTX_ID                                       as                                        ASG_AGRE_ID 
		, FIRST_VALUE(ASGN_EFF_DT) OVER(PARTITION BY ASGN_CNTX_ID, APP_CNTX_TYP_CD, ASGN_PRI_OWNR_IND ORDER BY ASGN_EFF_DT) AS FIRST_ASGN_EFF_DT
        , LAST_VALUE(ASGN_EFF_DT) OVER(PARTITION BY ASGN_CNTX_ID, APP_CNTX_TYP_CD, ASGN_PRI_OWNR_IND ORDER BY ASGN_EFF_DT) AS LAST_ASGN_EFF_DT
        , DRVD_PRI_OWNR_IND                                   as                                       DRVD_PRI_OWNR_IND
              from SRC_ASG
              

            ),
LOGIC_CDM as ( SELECT 
		  CLM_DISAB_MANG_EFF_DT                              as                              CLM_DISAB_MANG_EFF_DT 
		, CLM_DISAB_MANG_END_DT                              as                              CLM_DISAB_MANG_END_DT 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 

		, TRIM( CLM_DISAB_MANG_DISAB_TYP_CD )                as                        CLM_DISAB_MANG_DISAB_TYP_CD 
		, AGRE_ID                                            as                                            AGRE_ID 
		from SRC_CDM
            ),
LOGIC_CDMEND as ( SELECT 
		  CLM_DISAB_MANG_EFF_DT                              as                              CLM_DISAB_MANG_EFF_DT 
		, CLM_DISAB_MANG_END_DT                              as                              CLM_DISAB_MANG_END_DT 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, TRIM( CLM_DISAB_MANG_DISAB_TYP_CD )                as                        CLM_DISAB_MANG_DISAB_TYP_CD 
		, AGRE_ID                                            as                                            AGRE_ID 
		from SRC_CDM --was SRC_CDMEND
            ),
LOGIC_CSST as ( SELECT 
		  CLM_CLM_STS_EFF_DT                                 as                                 CLM_CLM_STS_EFF_DT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( CLM_TRANS_RSN_TYP_CD )                       as                               CLM_TRANS_RSN_TYP_CD 
		, MIN ( CASE WHEN CLM_TRANS_RSN_TYP_CD IN ('SETLINDM', 'SETLBOTH') THEN CLM_CLM_STS_EFF_DT END) OVER (PARTITION BY AGRE_ID) 
		                                                     AS                                 INTL_STLD_INDM_DATE
		, MIN( CASE WHEN CLM_TRANS_RSN_TYP_CD IN ('SETLMED', 'SETLBOTH') THEN CLM_CLM_STS_EFF_DT END) OVER (PARTITION BY AGRE_ID )
                                                       		AS                                 INTL_STLD_MDCL_DATE  
		from SRC_CSH --was SRC_CSST
            ),
LOGIC_U as ( SELECT 
		  TRIM(USER_ID  )                                    as                                            USER_ID 
		, TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		from SRC_UCS --was SRC_U
            ),
LOGIC_CIS as ( SELECT 
		  TRIM( ICD_CD )                                     as                                             ICD_CD 
		, TRIM( ICD_VER_CD )                                 as                                         ICD_VER_CD 
		, VOID_IND                                           as                                           VOID_IND 
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND 
		, AGRE_ID                                            as                                            AGRE_ID 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID 
		from SRC_CIS
            ),
LOGIC_CRD as ( SELECT 
		  AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( CLM_STT_TYP_CD )                             as                                     CLM_STT_TYP_CD 
		, CLM_CLM_STS_STT_DT                                 as                                 CLM_CLM_STS_STT_DT 
		from SRC_CSH --was SRC_CRD
            ),
LOGIC_PART as ( SELECT 
		  TRIM( CUST_NO )                                    as                                            CUST_NO 
		, CUST_ID                                            as                                            CUST_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( PTCP_TYP_CD )                                as                                        PTCP_TYP_CD 
		, CP_VOID_IND                                        as                                        CP_VOID_IND 
		from SRC_PART
            ),
-- LOGIC_BC as ( SELECT 
-- 		  TRIM(CUST_ID  )                                    as                                            CUST_ID 
-- 		, TRIM( CUST_NO )                                    as                                            CUST_NO 
-- 		from SRC_BC
--             ),
LOGIC_P as ( SELECT 
		  PLCY_ORIG_DT                                       as                                       PLCY_ORIG_DT 
		, AGRE_ID                                            as                                            AGRE_ID 
		FROM SRC_P
            ),			
LOGIC_IW as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, PRSN_BIRTH_DATE                                    as                                    PRSN_BIRTH_DATE 
		from SRC_IW
            ),
LOGIC_CPH as ( SELECT 
		  TRIM( EMP_CUST_NO )                                as                                        EMP_CUST_NO 
		, TRIM( PLCY_NO )                                    as                                            PLCY_NO 
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO 
		, INS_PARTICIPANT                                    as                                    INS_PARTICIPANT 
		, CRNT_PLCY_IND                                      as                                      CRNT_PLCY_IND 
		, CLM_AGRE_ID                                        as                                        CLM_AGRE_ID 
		, PLCY_AGRE_ID                                       as                                       PLCY_AGRE_ID 
		FROM SRC_CPH
            ),
LOGIC_PSR as ( SELECT 
		  TRIM( PLCY_TYP_CODE )                              as                                      PLCY_TYP_CODE 
		, TRIM( PLCY_STS_TYP_CD )                            as                                    PLCY_STS_TYP_CD 
		, TRIM( PLCY_STS_RSN_TYP_CD )                        as                                PLCY_STS_RSN_TYP_CD 
		, CASE WHEN PLCY_STS_TYP_CD IN ('EXP', 'ACT') THEN 'Y'
		       WHEN PLCY_NO IS NULL THEN NULL ELSE 'N' END       
		                                                	 as                                  POLICY_ACTIVE_IND 
		, TRIM( PLCY_NO )                                    as                                            PLCY_NO 
		, PLCY_PRD_EFF_DATE                                  as                                  PLCY_PRD_EFF_DATE 
		, PLCY_PRD_END_DATE                                  as                                  PLCY_PRD_END_DATE 
		, TRIM( CRNT_PLCY_PRD_STS_RSN_IND )                  as                          CRNT_PLCY_PRD_STS_RSN_IND 
		, TRIM( MOST_RCNT_PLCY_STS_RSN_IND )                 as                         MOST_RCNT_PLCY_STS_RSN_IND
		, AGRE_ID                                            as                                            AGRE_ID
		from SRC_PSR
              ),
LOGIC_BILL as ( SELECT
        
		 TRIM( INVOICE_STATUS_DESC  )                        as                               INVOICE_STATUS_DESC  
		, TRIM( SRVCN_PEACH_NUMBER )                         as                                 SRVCN_PEACH_NUMBER 
		, TRIM( CLAIM_NUMBER )                               as                                       CLAIM_NUMBER 
		, SERVICE_FROM                                       as                                       SERVICE_FROM 
		, TRIM( INVOICE_NUMBER )                             as                                     INVOICE_NUMBER 
		, TOTAL_APPROVED                                     as                                     TOTAL_APPROVED
		, INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID
		from SRC_BILL
            ),
LOGIC_IL as ( SELECT 

		 INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID 
		, LINE_SEQUENCE                                      as                                      LINE_SEQUENCE 
		from SRC_IL
            ),
LOGIC_RI as ( SELECT 
		  TRIM( CUST_ROLE_ID_VAL_STR )                       as                               CUST_ROLE_ID_VAL_STR 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( CRI_VOID_IND )                               as                                       CRI_VOID_IND 
		, TRIM( ID_TYP_CD )                                  as                                          ID_TYP_CD 
		from SRC_RI
            ),
LOGIC_CFT as ( SELECT 
                                                 
          SUM(CASE WHEN BNFT_TYP_NM ='NWWL' THEN CFT_AMT - CFT_DRV_BAL_AMT ELSE 0 END) 
                     		                                 as                                TOTAL_NWWL_PAID_AMT
         ,SUM(CASE WHEN BNFT_TYP_NM ILIKE '%SCHEDULED LOSS%' THEN CFT_AMT - CFT_DRV_BAL_AMT ELSE 0 END)
                                                     		 as                            TOTAL_SCH_LOSS_PAID_AMT                                              
		, TRIM( CLM_NO )                                     as                                             CLM_NO 

		from SRC_CFT		
		       group by 
			    CLM_NO
            ),
LOGIC_COD as ( SELECT 
		  CLM_OTHR_DT_EFF_DT                                 as                                 CLM_OTHR_DT_EFF_DT 
		, CLM_OTHR_DT_END_DT                                 as                                 CLM_OTHR_DT_END_DT 
		, VOID_IND                                           as                                           VOID_IND 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( CLM_DT_TYP_NM )                              as                                      CLM_DT_TYP_NM 
		FROM SRC_COD
	        ),			
LOGIC_CPP as ( SELECT 
		  AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( PROV_PTCP_TYP_NM )                           as                                   PROV_PTCP_TYP_NM 
		, TRIM( CUST_NO )                                    as                                            CUST_NO 
		, CLM_NO                                             as                                             CLM_NO
		  from SRC_CPP
            ),
LOGIC_CP as ( SELECT 
		  TRIM( PTCP_TYP_CD )                                as                                        PTCP_TYP_CD 
		, TRIM( CLM_PTCP_PRI_IND )                           as                                   CLM_PTCP_PRI_IND 
		, TRIM( CP_VOID_IND )                                as                                        CP_VOID_IND 
		, TRIM( CLM_NO )                                     as                                             CLM_NO 
		, CUST_ID                                            as                                            CUST_ID 
		, CLM_PTCP_EFF_DT                                    as                                    CLM_PTCP_EFF_DT 
		, CLM_PTCP_END_DT                                    as                                    CLM_PTCP_END_DT 
		from SRC_CP
            )

---- RENAME LAYER ----
,

RENAME_C as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM
		, CLM_LOSS_DESC                                      as                                      CLM_LOSS_DESC
		, OCCR_SRC_TYP_NM                                    as                                    OCCR_SRC_TYP_NM
		, OCCR_MEDA_TYP_NM                                   as                                   OCCR_MEDA_TYP_NM
		, OCCR_PRMS_TYP_NM                                   as                                   OCCR_PRMS_TYP_NM
		, CLM_CTRPH_INJR_IND                                 as                                 CLM_CTRPH_INJR_IND
		, CLM_OCCR_LOC_STR_1                                 as                                 CLM_OCCR_LOC_STR_1
		, CLM_OCCR_LOC_STR_2                                 as                                 CLM_OCCR_LOC_STR_2
		, CLM_OCCR_LOC_NM                                    as                                    CLM_OCCR_LOC_NM
		, CLM_OCCR_LOC_CITY_NM                               as                               CLM_OCCR_LOC_CITY_NM
		, CLM_OCCR_LOC_POST_CD                               as                               CLM_OCCR_LOC_POST_CD
		, CLM_OCCR_LOC_CNTY_NM                               as                               CLM_OCCR_LOC_CNTY_NM
		, CLM_OCCR_LOC_STT_CD                                as                                CLM_OCCR_LOC_STT_CD
		, CLM_OCCR_LOC_STT_NM                                as                                CLM_OCCR_LOC_STT_NM
		, CLM_OCCR_LOC_CNTRY_NM                              as                              CLM_OCCR_LOC_CNTRY_NM
		, CLM_OCCR_LOC_COMT                                  as                                   CLM_OCCR_LOC_CMT
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND
		, NOI_CTG_TYP_NM                                     as                                     NOI_CTG_TYP_NM
		, NOI_TYP_NM                                         as                                         NOI_TYP_NM
		, CLM_OCCR_RPT_DATE                                  as                                    CLAIM_FILE_DATE
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, CLM_FST_DCSN_DATE                                  as                                  CLM_FST_DCSN_DATE 
		, CLM_CLMT_LST_WK_DATE                               as                               CLM_CLMT_LST_WK_DATE 
		, CLM_CLMT_JOB_TTL                                   as                                   CLM_CLMT_JOB_TTL
		, CTRPH_INJR_TYP_CD                                  as                                  CTRPH_INJR_TYP_CD
		, CTRPH_INJR_TYP_NM                                  as                                  CTRPH_INJR_TYP_NM
		--, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND  
				FROM     LOGIC_C   ), 
RENAME_CTH as ( SELECT                                            
		 AGRE_ID                                            as                                        CTH_AGRE_ID 
		from LOGIC_CTH
            ),	
RENAME_CSH as ( SELECT 
		  CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD
		, CLM_STT_TYP_NM                                     as                                     CLM_STT_TYP_NM
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD
		, CLM_STS_TYP_NM                                     as                                     CLM_STS_TYP_NM
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD
		, CLM_TRANS_RSN_TYP_NM                               as                               CLM_TRANS_RSN_TYP_NM
		, CLM_CLM_STS_STT_DT                                 as                             CSH_CLM_CLM_STS_STT_DT
		, CLM_STT_TYP_CD                                     as                                 CSH_CLM_STT_TYP_CD
		, AGRE_ID                                            as                                        CSH_AGRE_ID
		, HIST_END_DTM                                       as                                   CSH_HIST_END_DTM 
				FROM     LOGIC_CSH   ), 
RENAME_FF as ( SELECT 
		  CLM_PRFL_ANSW_TEXT                                 as                             FIREFIGHTER_CANCER_IND
		, AGRE_ID                                            as                                         FF_AGRE_ID
		, VOID_IND                                           as                                        FF_VOID_IND
		, PRFL_STMT_ID                                       as                                    FF_PRFL_STMT_ID
		, AUDIT_USER_CREA_DTM                                as                             FF_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                             FF_AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_FF   ), 
RENAME_CE as ( SELECT 
		  CLM_PRFL_ANSW_TEXT                                 as                                 COVID_EXPOSURE_IND
		, AGRE_ID                                            as                                         CE_AGRE_ID
		, VOID_IND                                           as                                        CE_VOID_IND
		, PRFL_STMT_ID                                       as                                    CE_PRFL_STMT_ID
		, AUDIT_USER_CREA_DTM                                as                             CE_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                             CE_AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_CE   ), 
RENAME_CEW as ( SELECT 
		  CLM_PRFL_ANSW_TEXT                                 as                         COVID_EMERGENCY_WORKER_IND
		, AGRE_ID                                            as                                        CEW_AGRE_ID
		, VOID_IND                                           as                                       CEW_VOID_IND
		, PRFL_STMT_ID                                       as                                   CEW_PRFL_STMT_ID
		, AUDIT_USER_CREA_DTM                                as                            CEW_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                            CEW_AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_CEW   ), 
RENAME_CHCW as ( SELECT 
		  CLM_PRFL_ANSW_TEXT                                 as                       COVID_HEALTH_CARE_WORKER_IND
		, AGRE_ID                                            as                                       CHCW_AGRE_ID
		, VOID_IND                                           as                                      CHCW_VOID_IND
		, PRFL_STMT_ID                                       as                                  CHCW_PRFL_STMT_ID
		, AUDIT_USER_CREA_DTM                                as                           CHCW_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                           CHCW_AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_CHCW   ), 
RENAME_CAN as ( SELECT 
		  CLM_ALIAS_NO_NO                                    as                                 COMBINED_CLAIM_IND
		, AGRE_ID                                            as                                        CAN_AGRE_ID
		, VOID_IND                                           as                                       CAN_VOID_IND
		, CLM_ALIAS_NO_TYP_CD                                as                                CLM_ALIAS_NO_TYP_CD
		, CLM_ALIAS_NO_NO                                    as                                    CLM_ALIAS_NO_NO 
				FROM     LOGIC_CAN   ), 
RENAME_SB as ( SELECT 
		  CLM_PRFL_ANSW_TEXT                                 as                                          SB223_IND
		, AGRE_ID                                            as                                         SB_AGRE_ID
		, VOID_IND                                           as                                        SB_VOID_IND
		, PRFL_STMT_ID                                       as                                    SB_PRFL_STMT_ID
		, AUDIT_USER_CREA_DTM                                as                             SB_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                             SB_AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_SB   ), 
RENAME_PREM1 as ( SELECT 
		  CLM_PRFL_ANSW_TEXT                                 as                              EMPLOYER_PREMISES_IND
		, AGRE_ID                                            as                                      PREM1_AGRE_ID
		, VOID_IND                                           as                                     PREM1_VOID_IND
		, PRFL_STMT_ID                                       as                                 PREM1_PRFL_STMT_ID
		, CLM_PRFL_CTG_TYP_CD                                as                          PREM1_CLM_PRFL_CTG_TYP_CD
		, AUDIT_USER_CREA_DTM                                as                          PREM1_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                          PREM1_AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_PREM1   ), 
RENAME_PREM2 as ( SELECT 
		  CLM_PRFL_ANSW_TEXT                                 as                             ACCIDENT_PREMISES_TEXT
		, AGRE_ID                                            as                                      PREM2_AGRE_ID
		, VOID_IND                                           as                                     PREM2_VOID_IND
		, PRFL_STMT_ID                                       as                                 PREM2_PRFL_STMT_ID
		, CLM_PRFL_CTG_TYP_CD                                as                          PREM2_CLM_PRFL_CTG_TYP_CD
		, AUDIT_USER_CREA_DTM                                as                          PREM2_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                          PREM2_AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_PREM2   ), 
RENAME_KE as ( SELECT 
		  CLM_PRFL_ANSW_TEXT                                 as                          K_PROGRAM_ENROLLMENT_DESC
		, AGRE_ID                                            as                                         KE_AGRE_ID
		, VOID_IND                                           as                                        KE_VOID_IND
		, PRFL_STMT_ID                                       as                                    KE_PRFL_STMT_ID
		, AUDIT_USER_CREA_DTM                                as                             KE_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                             KE_AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_KE   ), 
RENAME_KT as ( SELECT 
		  CLM_PRFL_ANSW_TEXT                                 as                                K_PROGRAM_TYPE_DESC
		, AGRE_ID                                            as                                         KT_AGRE_ID
		, VOID_IND                                           as                                        KT_VOID_IND
		, PRFL_STMT_ID                                       as                                    KT_PRFL_STMT_ID
		, AUDIT_USER_CREA_DTM                                as                             KT_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                             KT_AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_KT   ), 
RENAME_KSTT as ( SELECT 
		  CLM_PRFL_ANSW_TEXT                                 as                               K_PROGRAM_START_DATE
		, AGRE_ID                                            as                                       KSTT_AGRE_ID
		, VOID_IND                                           as                                      KSTT_VOID_IND
		, PRFL_STMT_ID                                       as                                  KSTT_PRFL_STMT_ID
		, AUDIT_USER_CREA_DTM                                as                           KSTT_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                           KSTT_AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_KSTT   ), 
RENAME_KSTP as ( SELECT 
		  CLM_PRFL_ANSW_TEXT                                 as                                 K_PROGRAM_END_DATE
		, AGRE_ID                                            as                                       KSTP_AGRE_ID
		, VOID_IND                                           as                                      KSTP_VOID_IND
		, PRFL_STMT_ID                                       as                                  KSTP_PRFL_STMT_ID
		, AUDIT_USER_CREA_DTM                                as                           KSTP_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                           KSTP_AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_KSTP   ), 
RENAME_KRSN as ( SELECT 
		  CLM_PRFL_ANSW_TEXT                                 as                              K_PROGRAM_REASON_CODE
		, PRFL_SEL_VAL_TYP_NM                                as                              K_PROGRAM_REASON_DESC
		, AGRE_ID                                            as                                       KRSN_AGRE_ID
		, VOID_IND                                           as                                      KRSN_VOID_IND
		, PRFL_STMT_ID                                       as                                  KRSN_PRFL_STMT_ID
		, AUDIT_USER_CREA_DTM                                as                           KRSN_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                           KRSN_AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_KRSN   ), 
RENAME_CH as ( SELECT 
		  CLM_OCCR_RPT_DATE                                  as                            CLAIM_INITIAL_FILE_DATE
		, CLM_CLMT_NTF_DT                                    as                                    CLM_CLMT_NTF_DT
		, CLM_EMPLR_NTF_DT                                   as                                   CLM_EMPLR_NTF_DT
		, AGRE_ID                                            as                                    CH_AGRE_ID
		, CLM_REL_SNPSHT_IND                                 as                              CH_CLM_REL_SNPSHT_IND
		, HIST_EFF_DTM                                       as                                    CH_HIST_EFF_DTM 
		, HIST_END_DTM                                       as                                       HIST_END_DTM 	
		FROM     LOGIC_CH   ), 
RENAME_CHCLMT as ( SELECT 
		  CLM_OCCR_RPT_DATE                                  as                        CLMT_CLAIM_INITIAL_FILE_DATE
		, CLM_CLMT_NTF_DT                                    as                                CLMT_CLM_CLMT_NTF_DT
		, CLM_EMPLR_NTF_DT                                   as                               CLMT_CLM_EMPLR_NTF_DT
		, AGRE_ID                                            as                                     CLMT_CH_AGRE_ID
		, CLM_REL_SNPSHT_IND                                 as                           CLMT_CH_CLM_REL_SNPSHT_IND
		, HIST_EFF_DTM                                       as                                 CLMT_CH_HIST_EFF_DTM 
		, HIST_END_DTM                                       as                                 CLMT_CH_HIST_END_DTM 	
		FROM     LOGIC_CHCLMT   ), 
RENAME_CHEMPLR as ( SELECT 
		  CLM_OCCR_RPT_DATE                                  as                         EMPLR_CLAIM_INITIAL_FILE_DATE
		, CLM_CLMT_NTF_DT                                    as                                 EMPLR_CLM_CLMT_NTF_DT
		, CLM_EMPLR_NTF_DT                                   as                                EMPLR_CLM_EMPLR_NTF_DT
		, AGRE_ID                                            as                                      EMPLR_CH_AGRE_ID
		, CLM_REL_SNPSHT_IND                                 as                           EMPLR_CH_CLM_REL_SNPSHT_IND
		, HIST_EFF_DTM                                       as                                 EMPLR_CH_HIST_EFF_DTM 
		, HIST_END_DTM                                       as                                 EMPLR_CH_HIST_END_DTM 	
		FROM     LOGIC_CHEMPLR   ), 				
RENAME_CS as ( SELECT 
		  CS_CLS_CD                                          as                                          CS_CLS_CD
		, VOID_IND                                           as                                        CS_VOID_IND
		, CS_CLM_NO                                          as                                          CS_CLM_NO
		, CS_EFF_DATE                                        as                                        CS_EFF_DATE
		, AUDIT_USER_CREA_DTM                                as                             CS_AUDIT_USER_CREA_DTM 
		, CS_END_DATE                                        as                                        CS_END_DATE 
		, AUDIT_USER_UPDT_DTM                                as                             CS_AUDIT_USER_UPDT_DTM 
		FROM     LOGIC_CS   ), 
RENAME_WC as ( SELECT 
		  INDUSTRY_GROUP_CODE                                as                                INDUSTRY_GROUP_CODE
		, WC_CLS_SIC_XREF_CLS_CD                             as                             WC_CLS_SIC_XREF_CLS_CD 
				FROM     LOGIC_WC   ), 
RENAME_SCSH as ( SELECT 
		  HIST_EFF_DTM                                       as                           FIRST_DETERMINATION_DATE
		, AGRE_ID                                            as                                       SCSH_AGRE_ID
		, CLM_TRANS_RSN_TYP_CD                               as                          SCSH_CLM_TRANS_RSN_TYP_CD
		, CSTS_CLM_STS_TYP_CD                                as                           SCSH_CSTS_CLM_STS_TYP_CD
		, AUDIT_USER_UPDT_DTM                                as                           SCSH_AUDIT_USER_UPDT_DTM
		, AUDIT_USER_CREA_DTM                                as                           SCSH_AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                            SCSH_AUDIT_USER_ID_UPDT
		, AUDIT_USER_ID_CREA                                 as                            SCSH_AUDIT_USER_ID_CREA
				FROM     LOGIC_SCSH   ), 
RENAME_UCS as ( SELECT 
		  USER_LGN_NM                                        as                          DETERMINATION_USER_LGN_NM
		, USER_ID                                            as                                        UCS_USER_ID 
				FROM     LOGIC_UCS   ), 
RENAME_ASG as ( SELECT 
		  ASGN_EFF_DT                                        as                                        ASGN_EFF_DT
		, ORG_UNT_NM                                         as                                         ORG_UNT_NM
		, ORG_UNT_ABRV_NM                                    as                                    ORG_UNT_ABRV_NM 
        , APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD    
        , ASGN_PRI_OWNR_IND                                  as                                  ASGN_PRI_OWNR_IND  
        , ASG_AGRE_ID                                        as                                        ASG_AGRE_ID 
        , FIRST_ASGN_EFF_DT                                  as                                  FIRST_ASGN_EFF_DT 
        , LAST_ASGN_EFF_DT                                   as                                   LAST_ASGN_EFF_DT 
		, DRVD_PRI_OWNR_IND                                  as                                  DRVD_PRI_OWNR_IND		
				FROM     LOGIC_ASG   ), 
RENAME_CDM as ( SELECT 
		  CLM_DISAB_MANG_EFF_DT                              as                                      CDM_ARTW_DATE
		, CLM_DISAB_MANG_END_DT                              as                                      CDM_ERTW_DATE
		, VOID_IND                                           as                                       CDM_VOID_IND
		, CLM_DISAB_MANG_END_DT                              as                              CLM_DISAB_MANG_END_DT
		, CLM_DISAB_MANG_EFF_DT                              as                              CLM_DISAB_MANG_EFF_DT
		, CLM_DISAB_MANG_DISAB_TYP_CD                        as                        CLM_DISAB_MANG_DISAB_TYP_CD
		, AGRE_ID                                            as                                        CDM_AGRE_ID 
				FROM     LOGIC_CDM   ), 
RENAME_CDMEND as ( SELECT 
		  CLM_DISAB_MANG_EFF_DT                              as                                      END_ARTW_DATE
		, CLM_DISAB_MANG_END_DT                              as                                      END_ERTW_DATE
		, VOID_IND                                           as                                   END_CDM_VOID_IND
		, CLM_DISAB_MANG_END_DT                              as                           END_CLM_DISAB_MANG_END_DT
		, CLM_DISAB_MANG_EFF_DT                              as                           END_CLM_DISAB_MANG_EFF_DT
		, CLM_DISAB_MANG_DISAB_TYP_CD                        as                     END_CLM_DISAB_MANG_DISAB_TYP_CD
		, AGRE_ID                                            as                                     END_CDM_AGRE_ID 
				FROM     LOGIC_CDMEND   ), 
RENAME_CSST as ( SELECT 
		  CLM_CLM_STS_EFF_DT                                 as                                CLM_CLM_STS_EFF_DT
		, AGRE_ID                                            as                                       CSST_AGRE_ID
		, CLM_TRANS_RSN_TYP_CD                               as                          CSST_CLM_TRANS_RSN_TYP_CD
        , INTL_STLD_INDM_DATE                                as                               INTL_STLD_INDM_DATE
        , INTL_STLD_MDCL_DATE                                as                               INTL_STLD_MDCL_DATE
		FROM     LOGIC_CSST 	), 
RENAME_U as ( SELECT 
		  USER_ID                                            as                                            USER_ID
		, USER_LGN_NM                                        as                                  ENTRY_USER_LGN_NM 
				FROM     LOGIC_U   ), 
RENAME_CIS as ( SELECT 
		  ICD_CD                                             as                                     PRIMARY_ICD_CD
		, ICD_VER_CD                                         as                            ICD_CODE_VERSION_NUMBER
		, VOID_IND                                           as                                       CIS_VOID_IND
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND
		, AGRE_ID                                            as                                        CIS_AGRE_ID
		, AUDIT_USER_CREA_DTM                                as                            CIS_AUDIT_USER_CREA_DTM
		, CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID 
				FROM     LOGIC_CIS   ), 
RENAME_CRD as ( SELECT 
		  AGRE_ID                                            as                                        CRD_AGRE_ID
		, CLM_STT_TYP_CD                                     as                                 CRD_CLM_STT_TYP_CD
		, CLM_CLM_STS_STT_DT                                 as                                 CLAIM_RELEASE_DATE 
				FROM     LOGIC_CRD   ), 
RENAME_PART as ( SELECT 
		  CUST_NO                                            as                                            CUST_NO
		, CUST_ID                                            as                                            CUST_ID
		, AGRE_ID                                            as                                       PART_AGRE_ID
		, PTCP_TYP_CD                                        as                                   PART_PTCP_TYP_CD
		, CP_VOID_IND                                        as                                   PART_CP_VOID_IND 
				FROM     LOGIC_PART   ), 
-- RENAME_BC as ( SELECT 
-- 		  CUST_ID                                            as                                         BC_CUST_ID
-- 		, CUST_NO                                            as                                        EMP_CUST_NO 
-- 				FROM     LOGIC_BC   ), 
RENAME_P          as ( SELECT 
		  PLCY_ORIG_DT                                       as                                   PLCY_ORIG_EFF_DT
		, AGRE_ID                                            as                                          P_AGRE_ID 
				FROM     LOGIC_P   ), 
RENAME_IW as ( SELECT 
		  CUST_ID                                            as                                         IW_CUST_ID
		, PRSN_BIRTH_DATE                                    as                                    PRSN_BIRTH_DATE 
				FROM     LOGIC_IW   ), 

RENAME_CPH        as ( SELECT 
		  EMP_CUST_NO                                        as                                        EMP_CUST_NO
		, PLCY_NO                                            as                                        CPH_PLCY_NO
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO
		, INS_PARTICIPANT                                    as                                    INS_PARTICIPANT
		, CRNT_PLCY_IND                                      as                                      CRNT_PLCY_IND
		, CLM_AGRE_ID                                        as                                        CPH_AGRE_ID
		, PLCY_AGRE_ID                                       as                                    CPH_PLCY_AGRE_ID 
				FROM     LOGIC_CPH   ), 				
RENAME_PSR as ( SELECT 
		  PLCY_TYP_CODE                                      as                                   POLICY_TYPE_CODE
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND
		, PLCY_NO                                            as                                        PSR_PLCY_NO
		, PLCY_PRD_EFF_DATE                                  as                                  PLCY_PRD_EFF_DATE
		, PLCY_PRD_END_DATE                                  as                                  PLCY_PRD_END_DATE
		, CRNT_PLCY_PRD_STS_RSN_IND                          as                          CRNT_PLCY_PRD_STS_RSN_IND 
		, MOST_RCNT_PLCY_STS_RSN_IND	                     as                         MOST_RCNT_PLCY_STS_RSN_IND
		, AGRE_ID                                            as                                        PSR_AGRE_ID	
				FROM     LOGIC_PSR   ),
RENAME_BILL as ( SELECT 

		  INVOICE_STATUS_DESC                                as                                INVOICE_STATUS_DESC
		, SRVCN_PEACH_NUMBER                                 as                                 SRVCN_PEACH_NUMBER
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		, SERVICE_FROM                                       as                                       SERVICE_FROM
		, INVOICE_NUMBER                                     as                                     INVOICE_NUMBER 
		, TOTAL_APPROVED                                     as                                     TOTAL_APPROVED 
		, INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID
		FROM     LOGIC_BILL   ), 
RENAME_IL as ( SELECT 

		  INVOICE_HEADER_ID                                  as                             LINE_INVOICE_HEADER_ID
		, LINE_SEQUENCE                                      as                                      LINE_SEQUENCE 
				FROM     LOGIC_IL   ), 
RENAME_RI as ( SELECT 
		  CUST_ROLE_ID_VAL_STR                               as                                             MCO_NO
		, CUST_ID                                            as                                         RI_CUST_ID
		, CRI_VOID_IND                                       as                                       CRI_VOID_IND
		, ID_TYP_CD                                          as                                          ID_TYP_CD 
				FROM     LOGIC_RI   ), 
RENAME_CFT as ( SELECT 
		  TOTAL_NWWL_PAID_AMT                                as                                TOTAL_NWWL_PAID_AMT
		, TOTAL_SCH_LOSS_PAID_AMT                            as                            TOTAL_SCH_LOSS_PAID_AMT
		, CLM_NO                                             as                                         CFT_CLM_NO

				FROM     LOGIC_CFT   ), 
RENAME_COD        as ( SELECT 
		  CLM_OTHR_DT_EFF_DT                                 as                        CATASTROPHIC_EFFECTIVE_DATE
		, CLM_OTHR_DT_END_DT                                 as                       CATASTROPHIC_EXPIRATION_DATE
		, VOID_IND                                           as                                       COD_VOID_IND
		, AGRE_ID                                            as                                        COD_AGRE_ID
		, CLM_DT_TYP_NM                                      as                                      CLM_DT_TYP_NM 
				FROM     LOGIC_COD   ), 
RENAME_CPP as ( SELECT 
		  AGRE_ID                                            as                                        CPP_AGRE_ID
		, PROV_PTCP_TYP_NM                                   as                                   PROV_PTCP_TYP_NM 
		, CUST_NO                                            as                                        CPP_CUST_NO 
		, CLM_NO                                             as                                         CPP_CLM_NO
				  FROM     LOGIC_CPP   ), 

RENAME_CP as ( SELECT 
		  PTCP_TYP_CD                                        as                                     CP_PTCP_TYP_CD
		, CLM_PTCP_PRI_IND                                   as                                CP_CLM_PTCP_PRI_IND
		, CP_VOID_IND                                        as                                     CP_CP_VOID_IND
		, CLM_NO                                             as                                          CP_CLM_NO
		, CUST_ID                                            as                                         CP_CUST_ID
		, CLM_PTCP_EFF_DT                                    as                                 CP_CLM_PTCP_EFF_DT
		, CLM_PTCP_END_DT                                    as                                 CP_CLM_PTCP_END_DT 
				FROM     LOGIC_CP   )
---- FILTER LAYER (uses aliases) ----
,
FILTER_C                              as ( SELECT * from    RENAME_C 
                                            WHERE CLM_REL_SNPSHT_IND = 'N'  ),
FILTER_FF                             as ( SELECT * from    RENAME_FF 
                                            WHERE FF_PRFL_STMT_ID = 6334002 and FF_VOID_IND = 'N'
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY FF_AGRE_ID ORDER BY COALESCE (FF_AUDIT_USER_UPDT_DTM, '2999-12-31 00:00:00') DESC, FF_AUDIT_USER_CREA_DTM DESC )) =1 ),
FILTER_CE                             as ( SELECT * from    RENAME_CE 
                                            WHERE CE_PRFL_STMT_ID = 6380000 and CE_VOID_IND = 'N'
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY CE_AGRE_ID ORDER BY COALESCE (CE_AUDIT_USER_UPDT_DTM, '2999-12-31 00:00:00') DESC, CE_AUDIT_USER_CREA_DTM DESC )) =1 ),
FILTER_CEW                            as ( SELECT * from    RENAME_CEW 
                                            WHERE CEW_PRFL_STMT_ID = 6380001 and CEW_VOID_IND = 'N'
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY CEW_AGRE_ID ORDER BY COALESCE (CEW_AUDIT_USER_UPDT_DTM, '2999-12-31 00:00:00') DESC, CEW_AUDIT_USER_CREA_DTM DESC )) =1 ),
FILTER_CHCW                           as ( SELECT * from    RENAME_CHCW 
                                            WHERE CHCW_PRFL_STMT_ID = 6380002 and CHCW_VOID_IND = 'N'
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY CHCW_AGRE_ID ORDER BY COALESCE (CHCW_AUDIT_USER_UPDT_DTM, '2999-12-31 00:00:00') DESC, CHCW_AUDIT_USER_CREA_DTM DESC )) =1 ),
FILTER_SB                             as ( SELECT * from    RENAME_SB 
                                            WHERE SB_PRFL_STMT_ID = 6000355 and SB_VOID_IND = 'N'
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY SB_AGRE_ID ORDER BY COALESCE (SB_AUDIT_USER_UPDT_DTM, '2999-12-31 00:00:00') DESC, SB_AUDIT_USER_CREA_DTM DESC )) =1),
FILTER_CAN                            as ( SELECT * from    RENAME_CAN 
                                            WHERE CAN_VOID_IND = 'N' and CLM_ALIAS_NO_TYP_CD = 'DUPEXPRDCLM'  ),
FILTER_PREM1                          as ( SELECT * from    RENAME_PREM1 
                                            WHERE PREM1_PRFL_STMT_ID = 6000310 and PREM1_VOID_IND = 'N' and PREM1_CLM_PRFL_CTG_TYP_CD = 'JUR'
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY PREM1_AGRE_ID ORDER BY COALESCE (PREM1_AUDIT_USER_UPDT_DTM, '2999-12-31 00:00:00') DESC, PREM1_AUDIT_USER_CREA_DTM DESC )) =1 ),
FILTER_PREM2                          as ( SELECT * from    RENAME_PREM2 
                                            WHERE PREM2_PRFL_STMT_ID = 6000312 and PREM2_VOID_IND = 'N' and PREM2_CLM_PRFL_CTG_TYP_CD = 'JUR'
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY PREM2_AGRE_ID ORDER BY COALESCE (PREM2_AUDIT_USER_UPDT_DTM, '2999-12-31 00:00:00') DESC, PREM2_AUDIT_USER_CREA_DTM DESC )) =1 ),
FILTER_KE                             as ( SELECT * from    RENAME_KE 
                                            WHERE KE_PRFL_STMT_ID = 6000349 and KE_VOID_IND = 'N'
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY KE_AGRE_ID ORDER BY COALESCE (KE_AUDIT_USER_UPDT_DTM, '2999-12-31 00:00:00') DESC, KE_AUDIT_USER_CREA_DTM DESC )) =1 ),
FILTER_KT                             as ( SELECT * from    RENAME_KT 
                                            WHERE KT_PRFL_STMT_ID = 6000260 and KT_VOID_IND = 'N'
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY KT_AGRE_ID ORDER BY COALESCE (KT_AUDIT_USER_UPDT_DTM, '2999-12-31 00:00:00') DESC, KT_AUDIT_USER_CREA_DTM DESC )) =1 ),
FILTER_KSTT                           as ( SELECT * from    RENAME_KSTT 
                                            WHERE KSTT_PRFL_STMT_ID = 6000350 and KSTT_VOID_IND = 'N'
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY KSTT_AGRE_ID ORDER BY COALESCE (KSTT_AUDIT_USER_UPDT_DTM, '2999-12-31 00:00:00') DESC, KSTT_AUDIT_USER_CREA_DTM DESC )) =1 ),
FILTER_KSTP                           as ( SELECT * from    RENAME_KSTP 
                                            WHERE KSTP_PRFL_STMT_ID = 6000351 and KSTP_VOID_IND = 'N'
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY KSTP_AGRE_ID ORDER BY COALESCE (KSTP_AUDIT_USER_UPDT_DTM, '2999-12-31 00:00:00') DESC, KSTP_AUDIT_USER_CREA_DTM DESC )) =1 ),
FILTER_KRSN                           as ( SELECT * from    RENAME_KRSN 
                                            WHERE KRSN_PRFL_STMT_ID = 6000352 and KRSN_VOID_IND = 'N'
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY KRSN_AGRE_ID ORDER BY COALESCE (KRSN_AUDIT_USER_UPDT_DTM, '2999-12-31 00:00:00') DESC, KRSN_AUDIT_USER_CREA_DTM DESC )) =1 ),
FILTER_CSH                            as ( SELECT * from    RENAME_CSH 
                                            WHERE CSH_HIST_END_DTM is null  ),
FILTER_PART                           as ( SELECT * from    RENAME_PART 
                                            WHERE PART_PTCP_TYP_CD = 'CLMT' AND PART_CP_VOID_IND = 'N'  ),
FILTER_CIS                            as ( SELECT * from    RENAME_CIS 
                                            WHERE CIS_VOID_IND = 'N' AND CLM_ICD_STS_PRI_IND = 'Y'
		QUALIFY (ROW_NUMBER()OVER (PARTITION BY CIS_AGRE_ID ORDER BY CIS_AUDIT_USER_CREA_DTM DESC, CLM_ICD_STS_ID DESC) ) =1 ),
FILTER_U                              as ( SELECT * from    RENAME_U   ),
FILTER_CRD                            as ( SELECT * from    RENAME_CRD 
                                            WHERE CRD_CLM_STT_TYP_CD <> 'INCOMP'
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY CRD_AGRE_ID ORDER BY CLAIM_RELEASE_DATE))=1  ),
FILTER_CPH                            as ( SELECT * from    RENAME_CPH 
                                            WHERE CRNT_PLCY_IND = 'Y'  ),
FILTER_IW                             as ( SELECT * from    RENAME_IW   ),
-- FILTER_BC                             as ( SELECT * from    RENAME_BC   ),
FILTER_P                             as ( SELECT * from    RENAME_P   ),
FILTER_PSR                            as ( SELECT * from    RENAME_PSR 
                                            WHERE CRNT_PLCY_PRD_STS_RSN_IND = 'Y' and MOST_RCNT_PLCY_STS_RSN_IND = 'Y'  ),
FILTER_CH                             as ( SELECT * from    RENAME_CH 
                                            WHERE CH_CLM_REL_SNPSHT_IND = 'N'  
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY CH_AGRE_ID ORDER BY CLAIM_INITIAL_FILE_DATE))=1 ),
FILTER_CHCLMT                             as ( SELECT * from    RENAME_CHCLMT 
                                            WHERE CLMT_CH_CLM_REL_SNPSHT_IND = 'N'  
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY CLMT_CH_AGRE_ID ORDER BY  CLMT_CH_HIST_EFF_DTM desc, CLMT_CH_HIST_END_DTM desc))=1 ),
FILTER_CHEMPLR                             as ( SELECT * from    RENAME_CHEMPLR 
                                            WHERE EMPLR_CH_CLM_REL_SNPSHT_IND = 'N'  
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY EMPLR_CH_AGRE_ID ORDER BY EMPLR_CH_HIST_EFF_DTM desc, EMPLR_CH_HIST_END_DTM desc))=1 ),
FILTER_CS                             as ( SELECT * from    RENAME_CS 
                                            WHERE CS_VOID_IND = 'N' 
                                         QUALIFY (ROW_NUMBER()OVER(PARTITION BY CS_CLM_NO ORDER BY CS_EFF_DATE desc, CS_END_DATE DESC, CS_AUDIT_USER_CREA_DTM DESC, CS_AUDIT_USER_UPDT_DTM DESC))=1 ),
FILTER_SCSH                           as ( SELECT * from    RENAME_SCSH 
                                            WHERE SCSH_CSTS_CLM_STS_TYP_CD||SCSH_CLM_TRANS_RSN_TYP_CD not in ('INCOMPOCCRCLMREC', 'PNDCOVCOVNOTVRFY', 'ACPTREFILE', 'PNDINSTTJURSUSPD', 'DNYREFILE', 'PNDCOVNOTVRFY', 'PNDCOVVRFY', 'PNDINSTTJURPND', 'PNDCOVCONV'
    , 'EXPROCCRDUP', 'PNDHEAR', 'PNDSUSPDIC'  )  
QUALIFY (ROW_NUMBER()OVER(PARTITION BY SCSH_AGRE_ID ORDER BY FIRST_DETERMINATION_DATE , SCSH_AUDIT_USER_CREA_DTM ))=1 ),


FILTER_UCS                            as ( SELECT * from    RENAME_UCS   ),
FILTER_ASG                            as ( SELECT * from    RENAME_ASG 
                                            WHERE APP_CNTX_TYP_CD = 'CLAIM' AND ASGN_PRI_OWNR_IND ='Y' AND DRVD_PRI_OWNR_IND = 'Y'),
                                            
FILTER_CDM                            as ( SELECT * from    RENAME_CDM 
                                            WHERE CDM_VOID_IND = 'N'  
                                          QUALIFY (ROW_NUMBER()OVER(PARTITION BY CDM_AGRE_ID ORDER BY CLM_DISAB_MANG_EFF_DT DESC, NVL(CLM_DISAB_MANG_END_DT,CURRENT_DATE) DESC))=1 ),
                                          
FILTER_CDMEND                         as ( SELECT * from    RENAME_CDMEND 
                                            WHERE END_CDM_VOID_IND = 'N'  
                                            QUALIFY (ROW_NUMBER()OVER(PARTITION BY END_CDM_AGRE_ID ORDER BY END_CLM_DISAB_MANG_EFF_DT DESC, NVL(END_CLM_DISAB_MANG_END_DT,CURRENT_DATE) DESC)) =1),
                                            
FILTER_CSST                           as ( SELECT * from    RENAME_CSST 
                                            WHERE CSST_CLM_TRANS_RSN_TYP_CD IN ('SETLINDM', 'SETLMED', 'SETLBOTH')  
		QUALIFY (ROW_NUMBER()OVER(PARTITION BY CSST_AGRE_ID ORDER BY CLM_CLM_STS_EFF_DT )) =1),
		
                                              
FILTER_CTH                            as ( SELECT * from    RENAME_CTH   ),
FILTER_CPP                            as ( SELECT * from    RENAME_CPP 
                                            WHERE PROV_PTCP_TYP_NM = 'PHYSICIAN OF RECORD'  ),
FILTER_BILL                           as ( SELECT * from    RENAME_BILL 
                                            WHERE INVOICE_STATUS_DESC = 'PAID' AND TOTAL_APPROVED > 0   ),
FILTER_IL                             as ( SELECT * from    RENAME_IL   ),
FILTER_CP                             as ( SELECT * from    RENAME_CP 
                                            WHERE CP_PTCP_TYP_CD = 'MCO' AND CP_CLM_PTCP_PRI_IND = 'Y' AND CP_CP_VOID_IND = 'N' and  CP_CLM_PTCP_END_DT is null  -- I have added to elimiate Duplicate but need DA Validate
		 QUALIFY ROW_NUMBER() OVER(PARTITION BY CP_CLM_NO ORDER BY CP_CLM_PTCP_EFF_DT DESC, NVL(CP_CLM_PTCP_END_DT,CURRENT_DATE) DESC)=1
                                             ),
FILTER_RI                             as ( SELECT * from    RENAME_RI RI
                                                   left join FILTER_CP CP on CP.CP_CUST_ID=RI.RI_CUST_ID
                                            WHERE CRI_VOID_IND ='N' AND ID_TYP_CD = 'MCO'  
		QUALIFY ROW_NUMBER() OVER(PARTITION BY CP.CP_CLM_NO 
		ORDER BY CP.CP_CLM_PTCP_EFF_DT DESC, NVL(CP.CP_CLM_PTCP_END_DT,CURRENT_DATE) DESC)=1
		),
FILTER_CFT                            as ( SELECT * from    RENAME_CFT   ),
FILTER_WC                             as ( SELECT * from    RENAME_WC   ),
FILTER_COD                            as ( SELECT * FROM    RENAME_COD 
                                            WHERE COD_VOID_IND = 'N' AND CLM_DT_TYP_NM ='CATASTROPHIC DATES' 
											 QUALIFY (ROW_NUMBER() OVER (PARTITION BY COD_AGRE_ID ORDER BY CATASTROPHIC_EFFECTIVE_DATE DESC , NVL(CATASTROPHIC_EXPIRATION_DATE, '1999-01-01') DESC)) =1 ),
FILTER_JOIN_BILL_CPP                  as ( SELECT CLAIM_NUMBER,
		MIN(CASE WHEN CPP.CPP_CUST_NO=BILL.SRVCN_PEACH_NUMBER AND CPP.PROV_PTCP_TYP_NM = 'PHYSICIAN OF RECORD' THEN BILL.SERVICE_FROM end ) as FIRST_POR_VISIT_DATE,
		COUNT(DISTINCT BILL.INVOICE_NUMBER) as INVOICE_DISTINCT_COUNT,
		COUNT(DISTINCT BILL.INVOICE_NUMBER||IL.LINE_SEQUENCE) as INVOICE_LINE_DISTINCT_COUNT
		from  FILTER_BILL BILL
		left join FILTER_CPP CPP on 
		CPP.CPP_CLM_NO=BILL.CLAIM_NUMBER AND CPP.PROV_PTCP_TYP_NM = 'PHYSICIAN OF RECORD' 
		left join FILTER_IL IL ON 
		BILL.INVOICE_HEADER_ID =  IL.LINE_INVOICE_HEADER_ID
		group by CLAIM_NUMBER
		),


---- JOIN LAYER ----

CS as ( SELECT * 
				FROM  FILTER_CS
				LEFT JOIN FILTER_WC ON  FILTER_CS.CS_CLS_CD =  FILTER_WC.WC_CLS_SIC_XREF_CLS_CD  ),
PART as ( SELECT * 
				FROM  FILTER_PART
				LEFT JOIN FILTER_IW ON  FILTER_PART.CUST_ID =  FILTER_IW.IW_CUST_ID  ),
CPH as ( SELECT * 
				FROM  FILTER_CPH
				--LEFT JOIN FILTER_BC ON  FILTER_CPH.INS_PARTICIPANT =  FILTER_BC.BC_CUST_ID
				LEFT JOIN FILTER_P ON  FILTER_CPH.CPH_PLCY_AGRE_ID =  FILTER_P.P_AGRE_ID 
				LEFT JOIN FILTER_PSR ON  FILTER_CPH.CPH_PLCY_AGRE_ID =  FILTER_PSR.PSR_AGRE_ID),
SCSH as ( SELECT * 
				FROM  FILTER_SCSH
				LEFT JOIN FILTER_UCS ON  NVL(FILTER_SCSH.SCSH_AUDIT_USER_ID_UPDT,FILTER_SCSH.SCSH_AUDIT_USER_ID_CREA) =  FILTER_UCS.UCS_USER_ID  ),
C as ( SELECT * 
				FROM  FILTER_C
				LEFT JOIN FILTER_FF ON  FILTER_C.AGRE_ID =  FILTER_FF.FF_AGRE_ID 
								LEFT JOIN FILTER_CE ON  FILTER_C.AGRE_ID =  FILTER_CE.CE_AGRE_ID 
								LEFT JOIN FILTER_CEW ON  FILTER_C.AGRE_ID =  FILTER_CEW.CEW_AGRE_ID 
								LEFT JOIN FILTER_CHCW ON  FILTER_C.AGRE_ID =  FILTER_CHCW.CHCW_AGRE_ID 
								LEFT JOIN FILTER_SB ON  FILTER_C.AGRE_ID =  FILTER_SB.SB_AGRE_ID 
								LEFT JOIN FILTER_CAN ON  FILTER_C.CLM_NO =  FILTER_CAN.CLM_ALIAS_NO_NO 
								LEFT JOIN FILTER_PREM1 ON  FILTER_C.AGRE_ID =  FILTER_PREM1.PREM1_AGRE_ID 
								LEFT JOIN FILTER_PREM2 ON  FILTER_C.AGRE_ID =  FILTER_PREM2.PREM2_AGRE_ID 
								LEFT JOIN FILTER_KE ON  FILTER_C.AGRE_ID =  FILTER_KE.KE_AGRE_ID 
								LEFT JOIN FILTER_KT ON  FILTER_C.AGRE_ID =  FILTER_KT.KT_AGRE_ID 
								LEFT JOIN FILTER_KSTT ON  FILTER_C.AGRE_ID =  FILTER_KSTT.KSTT_AGRE_ID 
								LEFT JOIN FILTER_KSTP ON  FILTER_C.AGRE_ID =  FILTER_KSTP.KSTP_AGRE_ID 
								LEFT JOIN FILTER_KRSN ON  FILTER_C.AGRE_ID =  FILTER_KRSN.KRSN_AGRE_ID 
								LEFT JOIN FILTER_CSH ON  FILTER_C.AGRE_ID =  FILTER_CSH.CSH_AGRE_ID 
						        LEFT JOIN PART ON  FILTER_C.AGRE_ID = PART.PART_AGRE_ID 
								LEFT JOIN FILTER_CIS ON  FILTER_C.AGRE_ID =  FILTER_CIS.CIS_AGRE_ID 
								LEFT JOIN FILTER_U ON  FILTER_C.AUDIT_USER_ID_CREA =  FILTER_U.USER_ID 
								LEFT JOIN FILTER_CRD ON  FILTER_C.AGRE_ID =  FILTER_CRD.CRD_AGRE_ID 
						        LEFT JOIN CPH ON  FILTER_C.AGRE_ID = CPH.CPH_AGRE_ID  
								LEFT JOIN FILTER_CH ON  FILTER_C.AGRE_ID =  FILTER_CH.CH_AGRE_ID 
                                LEFT JOIN FILTER_CHCLMT ON  FILTER_C.AGRE_ID =  FILTER_CHCLMT.CLMT_CH_AGRE_ID 
                                LEFT JOIN FILTER_CHEMPLR ON  FILTER_C.AGRE_ID =  FILTER_CHEMPLR.EMPLR_CH_AGRE_ID 
								--LEFT JOIN FILTER_CS ON  FILTER_C.CLM_NO =  FILTER_CS.CS_CLM_NO 
								LEFT JOIN CS ON  FILTER_C.CLM_NO =  CS.CS_CLM_NO
						        LEFT JOIN SCSH ON  FILTER_C.AGRE_ID = SCSH.SCSH_AGRE_ID 
								LEFT JOIN FILTER_ASG ON  FILTER_C.AGRE_ID =  FILTER_ASG.ASG_AGRE_ID 
								LEFT JOIN FILTER_CDM ON  FILTER_C.AGRE_ID =  FILTER_CDM.CDM_AGRE_ID 
								LEFT JOIN FILTER_CDMEND ON  FILTER_C.AGRE_ID =  FILTER_CDMEND.END_CDM_AGRE_ID 
								LEFT JOIN FILTER_CSST ON  FILTER_C.AGRE_ID =  FILTER_CSST.CSST_AGRE_ID  
								LEFT JOIN FILTER_CTH ON  FILTER_C.AGRE_ID =  FILTER_CTH.CTH_AGRE_ID 
								LEFT JOIN FILTER_CPP ON  FILTER_C.AGRE_ID =  FILTER_CPP.CPP_AGRE_ID 
						        LEFT JOIN FILTER_CP ON  FILTER_C.CLM_NO = FILTER_CP.CP_CLM_NO 
								LEFT JOIN FILTER_RI  on FILTER_CP.CP_CUST_ID=FILTER_RI.RI_CUST_ID
							  --  LEFT JOIN CS ON  FILTER_C.CLM_NO = CS.CS_CLM_NO 
							    LEFT JOIN FILTER_CFT ON  FILTER_C.CLM_NO =  FILTER_CFT.CFT_CLM_NO  
							    LEFT JOIN FILTER_JOIN_BILL_CPP ON FILTER_C.CLM_NO =  FILTER_JOIN_BILL_CPP.CLAIM_NUMBER
								LEFT JOIN FILTER_COD ON  FILTER_C.AGRE_ID =  FILTER_COD.COD_AGRE_ID  )  


----FINAL ETL LAYER FOR CREATING HKEY and UNIQUE_ID_KEY
,

ETL1 as (
SELECT distinct
md5(cast(
    
    coalesce(cast(CLM_NO as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY,		
CLM_NO                               AS CLM_NO ,		
AGRE_ID                              AS  AGRE_ID ,		
CLM_TYP_CD                           AS CLM_TYP_CD ,		
CLM_TYP_NM                           AS CLM_TYP_NM ,		
case when CTH_AGRE_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS CHNG_OVR_IND,		
CLM_STT_TYP_CD                       AS CLM_STT_TYP_CD ,		
CLM_STT_TYP_NM                       AS CLM_STT_TYP_NM ,		
CLM_STS_TYP_CD                       AS CLM_STS_TYP_CD ,		
CLM_STS_TYP_NM                       AS CLM_STS_TYP_NM ,		
CLM_TRANS_RSN_TYP_CD                 AS CLM_TRANS_RSN_TYP_CD ,		
CLM_TRANS_RSN_TYP_NM                 AS CLM_TRANS_RSN_TYP_NM ,		
CASE WHEN CSH_CLM_STT_TYP_CD = 'CLS' THEN CSH_CLM_CLM_STS_STT_DT ELSE  NULL END AS CLAIM_CLOSED_DATE,	
CLM_LOSS_DESC                        AS CLM_LOSS_DESC ,		
OCCR_SRC_TYP_NM                      AS OCCR_SRC_TYP_NM ,		
OCCR_MEDA_TYP_NM                     AS OCCR_MEDA_TYP_NM ,		
CASE FIREFIGHTER_CANCER_IND WHEN 'YES' THEN 'Y' WHEN 'NO' THEN 'N' WHEN 'UNK' THEN 'U' ELSE NULL END AS FIREFIGHTER_CANCER_IND ,		
CASE COVID_EXPOSURE_IND  WHEN 'YES' THEN 'Y' WHEN 'NO' THEN 'N' WHEN 'UNK' THEN 'U' ELSE NULL END AS COVID_EXPOSURE_IND ,		
CASE COVID_EMERGENCY_WORKER_IND WHEN 'YES' THEN 'Y' WHEN 'NO' THEN 'N' WHEN 'UNK' THEN 'U' ELSE NULL END AS COVID_EMERGENCY_WORKER_IND ,		
CASE COVID_HEALTH_CARE_WORKER_IND  WHEN 'YES' THEN 'Y' WHEN 'NO' THEN 'N' WHEN 'UNK' THEN 'U' ELSE NULL END AS COVID_HEALTH_CARE_WORKER_IND ,		
CASE WHEN COMBINED_CLAIM_IND IS NOT NULL THEN 'Y' ELSE 'N' END AS COMBINED_CLAIM_IND ,		
CASE SB223_IND  WHEN 'YES' THEN 'Y' WHEN 'NO' THEN 'N' WHEN 'UNK' THEN 'U' ELSE NULL END AS SB223_IND ,		
OCCR_PRMS_TYP_NM                    AS OCCR_PRMS_TYP_NM ,		
CASE EMPLOYER_PREMISES_IND  WHEN 'YES' THEN 'Y' WHEN 'NO' THEN 'N' WHEN 'UNK' THEN 'U' ELSE NULL END AS EMPLOYER_PREMISES_IND ,		
ACCIDENT_PREMISES_TEXT AS ACCIDENT_PREMISES_TEXT ,		
CASE K_PROGRAM_ENROLLMENT_DESC   WHEN 'YES' THEN 'YES' WHEN 'NO' THEN 'NO' WHEN 'UNK' THEN 'UNKNOWN' ELSE NULL END  AS K_PROGRAM_ENROLLMENT_DESC ,		
CASE WHEN K_PROGRAM_ENROLLMENT_DESC = 'YES' THEN K_PROGRAM_TYPE_DESC END AS K_PROGRAM_TYPE_DESC,		
CASE WHEN (K_PROGRAM_ENROLLMENT_DESC = 'YES' AND LENGTH(K_PROGRAM_START_DATE)=8) THEN TO_DATE(K_PROGRAM_START_DATE, 'MMDDYYYY') END AS K_PROGRAM_START_DATE ,	
CASE WHEN (K_PROGRAM_ENROLLMENT_DESC = 'YES' AND LENGTH(K_PROGRAM_END_DATE)=8) THEN TO_DATE(K_PROGRAM_END_DATE, 'MMDDYYYY') ELSE NULL END AS K_PROGRAM_END_DATE ,
CASE WHEN K_PROGRAM_ENROLLMENT_DESC = 'YES' THEN CAST(KSTT_AUDIT_USER_CREA_DTM AS DATE) END AS K_PROGRAM_ENTRY_DATE ,
CASE WHEN K_PROGRAM_ENROLLMENT_DESC = 'YES' THEN K_PROGRAM_REASON_CODE END AS K_PROGRAM_REASON_CODE ,		
CASE WHEN K_PROGRAM_ENROLLMENT_DESC = 'YES' THEN K_PROGRAM_REASON_DESC END AS K_PROGRAM_REASON_DESC ,		
CLM_CTRPH_INJR_IND                  AS CLM_CTRPH_INJR_IND ,		
CLM_OCCR_LOC_STR_1                  AS CLM_OCCR_LOC_STR_1 ,
CLM_OCCR_LOC_STR_2                  AS CLM_OCCR_LOC_STR_2 ,
CLM_OCCR_LOC_NM                     AS CLM_OCCR_LOC_NM ,		
CLM_OCCR_LOC_CITY_NM                AS CLM_OCCR_LOC_CITY_NM ,		
CLM_OCCR_LOC_POST_CD                AS CLM_OCCR_LOC_POST_CD ,		
CLM_OCCR_LOC_CNTY_NM                AS CLM_OCCR_LOC_CNTY_NM ,		
CLM_OCCR_LOC_STT_CD                 AS CLM_OCCR_LOC_STT_CD ,		
CLM_OCCR_LOC_STT_NM                 AS CLM_OCCR_LOC_STT_NM ,		
CLM_OCCR_LOC_CNTRY_NM               AS CLM_OCCR_LOC_CNTRY_NM ,		
CLM_OCCR_LOC_CMT                    AS CLM_OCCR_LOC_CMT ,		
NOI_CTG_TYP_NM						AS	NOI_CTG_TYP_NM ,				
NOI_TYP_NM							AS	NOI_TYP_NM ,	
CLAIM_FILE_DATE,
CLAIM_INITIAL_FILE_DATE,								
CLMT_CLM_CLMT_NTF_DT AS CLM_CLMT_NTF_DT,		
EMPLR_CLM_EMPLR_NTF_DT AS CLM_EMPLR_NTF_DT,		
CS_CLS_CD AS CS_CLS_CD,		
CASE WHEN CS_CLS_CD is not null and  POLICY_TYPE_CODE = 'PES' THEN  '970' ELSE  '000'   END AS DRVD_MANUAL_CLASS_SUFFIX_CODE,		
INDUSTRY_GROUP_CODE,	
CLM_OCCR_DATE	                    AS 	CLM_OCCR_DATE ,
AUDIT_USER_ID_CREA	                AS	AUDIT_USER_ID_CREA ,
AUDIT_USER_CREA_DTM	                AS 	AUDIT_USER_CREA_DTM ,
CLM_FST_DCSN_DATE	                AS 	CLM_FST_DCSN_DATE ,	
FIRST_DETERMINATION_DATE,
DETERMINATION_USER_LGN_NM AS DETERMINATION_USER_LGN_NM,		
FIRST_ASGN_EFF_DT AS FIRST_ASGN_EFF_DATE, 		
LAST_ASGN_EFF_DT AS LAST_ASGN_EFF_DATE,		
UPPER(ORG_UNT_NM) AS ORG_UNT_NM,		
UPPER(ORG_UNT_ABRV_NM) AS ORG_UNT_ABRV_NM,		
CLM_CLMT_LST_WK_DATE AS CLM_CLMT_LST_WK_DATE,		
CASE WHEN CLM_DISAB_MANG_DISAB_TYP_CD = 'WORKING' THEN CLM_DISAB_MANG_EFF_DT ELSE NULL END AS   ARTW_DATE,		
CASE WHEN END_CLM_DISAB_MANG_DISAB_TYP_CD  = 'OFFWKESTRTW' THEN END_CLM_DISAB_MANG_END_DT ELSE NULL END AS ERTW_DATE,		
INTL_STLD_INDM_DATE AS INTL_STLD_INDM_DATE,		
INTL_STLD_MDCL_DATE AS INTL_STLD_MDCL_DATE,		
DATEDIFF(DAYS, CLM_OCCR_DATE, CLAIM_FILE_DATE) AS CLAIM_FILE_LAG_DAYS_COUNT ,		
ENTRY_USER_LGN_NM                   AS 	ENTRY_USER_LGN_NM ,	
PRIMARY_ICD_CD                      AS  PRIMARY_ICD_CD ,		
CAST(ICD_CODE_VERSION_NUMBER AS INT) AS ICD_CODE_VERSION_NUMBER ,		
CLAIM_RELEASE_DATE	                AS 	CLAIM_RELEASE_DATE ,
CUST_NO	                            AS 	CUST_NO	,								
CUST_ID	                            AS	CUST_ID ,
EMP_CUST_NO	                        AS 	EMP_CUST_NO ,
PRSN_BIRTH_DATE	                    AS 	PRSN_BIRTH_DATE ,
CASE WHEN PRSN_BIRTH_DATE IS NULL THEN  NULL ELSE FLOOR(MONTHS_BETWEEN(CLM_OCCR_DATE, PRSN_BIRTH_DATE)/12) END AS AGE_AT_OCCURRENCE ,		
CPH_PLCY_NO	                        AS  PLCY_NO ,
PLCY_ORIG_EFF_DT,	
BUSN_SEQ_NO	                        AS 	BUSN_SEQ_NO ,
INS_PARTICIPANT	                    AS	INS_PARTICIPANT ,
POLICY_TYPE_CODE	                AS  POLICY_TYPE_CODE ,	
PLCY_STS_TYP_CD	                    AS	PLCY_STS_TYP_CD ,
PLCY_STS_RSN_TYP_CD	                AS	PLCY_STS_RSN_TYP_CD ,
CASE WHEN PLCY_STS_TYP_CD IN ('EXP', 'ACT') THEN 'Y' WHEN PLCY_NO IS NULL THEN NULL ELSE 'N' end as POLICY_ACTIVE_IND ,		
CLM_CLMT_JOB_TTL,		
FIRST_POR_VISIT_DATE,
INVOICE_DISTINCT_COUNT,
INVOICE_LINE_DISTINCT_COUNT,
MCO_NO,
TOTAL_NWWL_PAID_AMT,
TOTAL_SCH_LOSS_PAID_AMT,
CTRPH_INJR_TYP_CD,
CTRPH_INJR_TYP_NM,
CATASTROPHIC_EFFECTIVE_DATE,
CATASTROPHIC_EXPIRATION_DATE,
CLM_REL_SNPSHT_IND
from C
)

SELECT * FROM ETL1