

      create or replace  table DEV_EDW.STAGING.STG_CLAIM_HISTORY  as
      (---- SRC LAYER ----
WITH
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_HISTORY ),
SRC_COLCT as ( SELECT *     from     DEV_VIEWS.PCMP.CAUSE_OF_LOSS_CATEGORY_TYPE ),
SRC_COLT as ( SELECT *     from     DEV_VIEWS.PCMP.CAUSE_OF_LOSS_TYPE ),
SRC_CCT as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_CLAIM_TYPE ),
SRC_CT as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_TYPE ),
SRC_CEST as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_EMPLOYMENT_STATUS_TYPE ),
SRC_ST as ( SELECT *     from     DEV_VIEWS.PCMP.STATE ),
SRC_CO as ( SELECT *     from     DEV_VIEWS.PCMP.COUNTRY ),
SRC_CRT as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_RECOVERY_TYPE ),
SRC_CET as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_SETTLEMENT_TYPE ),
SRC_CTRPH as ( SELECT *     from     DEV_VIEWS.PCMP.CATASTROPHE ),
SRC_CIT as ( SELECT *     from     DEV_VIEWS.PCMP.CATASTROPHIC_INJURY_TYPE ),
SRC_ELT as ( SELECT *     from     DEV_VIEWS.PCMP.EDUCATION_LEVEL_TYPE ),
SRC_JT as ( SELECT *     from     DEV_VIEWS.PCMP.JURISDICTION_TYPE ),
SRC_MTT as ( SELECT *     from     DEV_VIEWS.PCMP.MEDICAL_TREATMENT_TYPE ),
SRC_NOICT as ( SELECT *     from     DEV_VIEWS.PCMP.NATURE_OF_INJURY_CATEGORY_TYPE ),
SRC_NOIT as ( SELECT *     from     DEV_VIEWS.PCMP.NATURE_OF_INJURY_TYPE ),
SRC_OMT as ( SELECT *     from     DEV_VIEWS.PCMP.OCCURRENCE_MEDIA_TYPE ),
SRC_OPT as ( SELECT *     from     DEV_VIEWS.PCMP.OCCURRENCE_PREMISES_TYPE ),
SRC_OST as ( SELECT *     from     DEV_VIEWS.PCMP.OCCURRENCE_SOURCE_TYPE ),
SRC_PILLT as ( SELECT *     from     DEV_VIEWS.PCMP.PRE_INJURY_LABOR_LEVEL_TYPE ),
SRC_UCT as ( SELECT *     from     DEV_VIEWS.PCMP.UNUSUAL_CLAIM_TYPE ),
//SRC_C as ( SELECT *     from     CLAIM_HISTORY) ,
//SRC_COLCT as ( SELECT *     from     CAUSE_OF_LOSS_CATEGORY_TYPE) ,
//SRC_COLT as ( SELECT *     from     CAUSE_OF_LOSS_TYPE) ,
//SRC_CCT as ( SELECT *     from     CLAIM_CLAIM_TYPE) ,
//SRC_CT as ( SELECT *     from     CLAIM_TYPE) ,
//SRC_CEST as ( SELECT *     from     CLAIM_EMPLOYMENT_STATUS_TYPE) ,
//SRC_ST as ( SELECT *     from     STATE) ,
//SRC_CO as ( SELECT *     from     COUNTRY) ,
//SRC_CRT as ( SELECT *     from     CLAIM_RECOVERY_TYPE) ,
//SRC_CET as ( SELECT *     from     CLAIM_SETTLEMENT_TYPE) ,
//SRC_CTRPH as ( SELECT *     from     CATASTROPHE) ,
//SRC_CIT as ( SELECT *     from     CATASTROPHIC_INJURY_TYPE) ,
//SRC_ELT as ( SELECT *     from     EDUCATION_LEVEL_TYPE) ,
//SRC_JT as ( SELECT *     from     JURISDICTION_TYPE) ,
//SRC_MTT as ( SELECT *     from     MEDICAL_TREATMENT_TYPE) ,
//SRC_NOICT as ( SELECT *     from     NATURE_OF_INJURY_CATEGORY_TYPE) ,
//SRC_NOIT as ( SELECT *     from     NATURE_OF_INJURY_TYPE) ,
//SRC_OMT as ( SELECT *     from     OCCURRENCE_MEDIA_TYPE) ,
//SRC_OPT as ( SELECT *     from     OCCURRENCE_PREMISES_TYPE) ,
//SRC_OST as ( SELECT *     from     OCCURRENCE_SOURCE_TYPE) ,
//SRC_PILLT as ( SELECT *     from     PRE_INJURY_LABOR_LEVEL_TYPE) ,
//SRC_UCT as ( SELECT *     from     UNUSUAL_CLAIM_TYPE) ,

---- LOGIC LAYER ----


LOGIC_C as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		, upper( TRIM( CLM_NO ) )                            as                                             CLM_NO 
		, upper( TRIM( CAUS_OF_LOSS_CTG_TYP_CD ) )           as                            CAUS_OF_LOSS_CTG_TYP_CD 
		, upper( TRIM( CAUS_OF_LOSS_TYP_CD ) )               as                                CAUS_OF_LOSS_TYP_CD 
		, CLM_ADMN_NTF_DT                                    as                                    CLM_ADMN_NTF_DT 
		, cast( CLM_ADMN_NTF_TIME_LOSS_DT as DATE )          as                          CLM_ADMN_NTF_TIME_LOSS_DT 
		, upper( TRIM( CLM_ALCH_INVD_IND ) )                 as                                  CLM_ALCH_INVD_IND 
		, cast( CLM_ANTCPT_SRGY_DT as DATE )                 as                                 CLM_ANTCPT_SRGY_DT 
		, upper( TRIM( CLM_ANTCPT_SRGY_IND ) )               as                                CLM_ANTCPT_SRGY_IND 
		, cast( CLM_BILL_ERLST_DT as DATE )                  as                                  CLM_BILL_ERLST_DT 
		, cast( CLM_BILL_LTST_DT as DATE )                   as                                   CLM_BILL_LTST_DT 
		, upper( TRIM( CLM_CLMT_FATL_IND ) )                 as                                  CLM_CLMT_FATL_IND 
		, cast( CLM_CLMT_HIRE_DT as DATE )                   as                                   CLM_CLMT_HIRE_DT 
		, CLM_CLMT_HIRE_STT_ID                               as                               CLM_CLMT_HIRE_STT_ID 
		, upper( TRIM( CLM_CLMT_JOB_TTL ) )                  as                                   CLM_CLMT_JOB_TTL 
		, cast( CLM_CLMT_LST_WK_DT as DATE )                 as                                 CLM_CLMT_LST_WK_DT 
		, CLM_CLMT_NTF_DT                                    as                                    CLM_CLMT_NTF_DT 
		, CLM_CLMT_SHFT_BGN_TIME                             as                             CLM_CLMT_SHFT_BGN_TIME 
		, upper( TRIM( CLM_CNTC_EMAIL ) )                    as                                     CLM_CNTC_EMAIL 
		, upper( TRIM( CLM_CNTC_NM ) )                       as                                        CLM_CNTC_NM 
		, upper( TRIM( CLM_CNTC_PHN ) )                      as                                       CLM_CNTC_PHN 
		, upper( TRIM( CLM_COV_TYP_CD ) )                    as                                     CLM_COV_TYP_CD 
		, upper( TRIM( CLM_CTRPH_INJR_CMT ) )                as                                 CLM_CTRPH_INJR_CMT 
		, upper( TRIM( CLM_CTRPH_INJR_IND ) )                as                                 CLM_CTRPH_INJR_IND 
		, cast( CLM_DISAB_BGN_DT as DATE )                   as                                   CLM_DISAB_BGN_DT 
		, upper( TRIM( CLM_DO_NOT_CLOS_IND ) )               as                                CLM_DO_NOT_CLOS_IND 
		, upper( TRIM( CLM_DO_NOT_REFR_TO_RHBL_IND ) )       as                        CLM_DO_NOT_REFR_TO_RHBL_IND 
		, upper( TRIM( CLM_DRUG_INVD_IND ) )                 as                                  CLM_DRUG_INVD_IND 
		, CLM_EMPLR_NTF_DT                                   as                                   CLM_EMPLR_NTF_DT 
		, cast( CLM_EMPLR_NTF_TIME_LOSS_DT as DATE )         as                         CLM_EMPLR_NTF_TIME_LOSS_DT 
		, upper( TRIM( CLM_EMPL_STS_TYP_CD ) )               as                                CLM_EMPL_STS_TYP_CD 
		, cast( CLM_FST_DCSN_DT as DATE )                    as                                    CLM_FST_DCSN_DT 
		, cast( CLM_FST_RPT_DISAB_DT as DATE )               as                               CLM_FST_RPT_DISAB_DT 
		, upper( TRIM( CLM_FULL_WGS_PD_IND ) )               as                                CLM_FULL_WGS_PD_IND 
		, upper( TRIM( CLM_GRP_TYP_CD ) )                    as                                     CLM_GRP_TYP_CD 
		, CLM_INDM_PAY_DD_NOT_PD                             as                             CLM_INDM_PAY_DD_NOT_PD 
		, upper( TRIM( CLM_INVSTG_TYP_CD ) )                 as                                  CLM_INVSTG_TYP_CD 
		, upper( trim( TRIM( CLM_LOSS_DESC ) ))              as                                      CLM_LOSS_DESC 
		, upper( TRIM( CLM_LOSS_TYP_CD ) )                   as                                    CLM_LOSS_TYP_CD 
		, upper( TRIM( CLM_MULTI_OCCR_IND ) )                as                                 CLM_MULTI_OCCR_IND 
		, upper( TRIM( CLM_NOTE_IND ) )                      as                                       CLM_NOTE_IND 
		, upper( TRIM( CLM_NO_DRV_UPCS ) )                   as                                    CLM_NO_DRV_UPCS 
		, CLM_OCCR_DTM                                       as                                       CLM_OCCR_DTM 
		, upper( TRIM( CLM_OCCR_DTM_VRFY_IND ) )             as                              CLM_OCCR_DTM_VRFY_IND 
		, upper( TRIM( CLM_OCCR_LOC_COMT ) )                 as                                  CLM_OCCR_LOC_COMT 
		, CLM_OCCR_LOC_LAT                                   as                                   CLM_OCCR_LOC_LAT 
		, CLM_OCCR_LOC_LONG                                  as                                  CLM_OCCR_LOC_LONG 
		, upper( TRIM( CLM_OCCR_LOC_NM ) )                   as                                    CLM_OCCR_LOC_NM 
		, upper( trim( TRIM( CLM_OCCR_LOC_STR_1 ) ))         as                                 CLM_OCCR_LOC_STR_1 
		, upper( trim( TRIM( CLM_OCCR_LOC_STR_2 ) ))         as                                 CLM_OCCR_LOC_STR_2 
		, upper( TRIM( CLM_OCCR_LOC_CITY_NM ) )              as                               CLM_OCCR_LOC_CITY_NM 
		, upper( TRIM( CLM_OCCR_LOC_CITY_UPCS_SRCH_NM ) )    as                     CLM_OCCR_LOC_CITY_UPCS_SRCH_NM 
		, upper( TRIM( CLM_OCCR_LOC_POST_CD ) )              as                               CLM_OCCR_LOC_POST_CD 
		, upper( TRIM( CLM_OCCR_LOC_CNTY_NM ) )              as                               CLM_OCCR_LOC_CNTY_NM 
		, upper( TRIM( CLM_OCCR_LOC_CNTY_UPCS_SRCH_NM ) )    as                     CLM_OCCR_LOC_CNTY_UPCS_SRCH_NM 
		, upper( TRIM( CLM_OCCR_NOI_DESC ) )                 as                                  CLM_OCCR_NOI_DESC 
		, cast( CLM_OCCR_PRPR_DT as DATE )                   as                                   CLM_OCCR_PRPR_DT 
		, upper( TRIM( CLM_OCCR_PRPR_NM ) )                  as                                   CLM_OCCR_PRPR_NM 
		, upper( TRIM( CLM_OCCR_PRPR_PHN ) )                 as                                  CLM_OCCR_PRPR_PHN 
		, upper( TRIM( CLM_OCCR_PRPR_TTL ) )                 as                                  CLM_OCCR_PRPR_TTL 
		, cast( CLM_OCCR_RPT_DT as DATE )                    as                                    CLM_OCCR_RPT_DT 
		, upper( TRIM( CLM_OCCR_TM_SET_IND ) )               as                                CLM_OCCR_TM_SET_IND 
		, upper( TRIM( CLM_PRSMPTN_IND ) )                   as                                    CLM_PRSMPTN_IND 
		, upper( TRIM( CLM_PRVS_IMPR_AWARD_IND ) )           as                            CLM_PRVS_IMPR_AWARD_IND 
		, upper( TRIM( CLM_RCVR_TYP_CD ) )                   as                                    CLM_RCVR_TYP_CD 
		, upper( TRIM( CLM_REL_SNPSHT_IND ) )                as                                 CLM_REL_SNPSHT_IND 
		, upper( TRIM( CLM_RTN_WK_DIFF_EMPLR_IND ) )         as                          CLM_RTN_WK_DIFF_EMPLR_IND 
		, cast( CLM_SCND_INJR_DT as DATE )                   as                                   CLM_SCND_INJR_DT 
		, upper( TRIM( CLM_SETL_TYP_CD ) )                   as                                    CLM_SETL_TYP_CD 
		, cast( CLM_UNDRGN_SRGY_DT as DATE )                 as                                 CLM_UNDRGN_SRGY_DT 
		, upper( TRIM( CLM_UNDRGN_SRGY_IND ) )               as                                CLM_UNDRGN_SRGY_IND 
		, CNTRY_ID                                           as                                           CNTRY_ID 
		, upper( TRIM( CTRPH_CD ) )                          as                                           CTRPH_CD 
		, upper( TRIM( CTRPH_INJR_TYP_CD ) )                 as                                  CTRPH_INJR_TYP_CD 
		, upper( TRIM( EDUC_LVL_TYP_CD ) )                   as                                    EDUC_LVL_TYP_CD 
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       as                                       HIST_END_DTM 
		, upper( TRIM( JUR_TYP_CD ) )                        as                                         JUR_TYP_CD 
		, upper( TRIM( LOB_TYP_CD ) )                        as                                         LOB_TYP_CD 
		, LOSS_COV_TYP_ID                                    as                                    LOSS_COV_TYP_ID 
		, upper( TRIM( MCO_NO ) )                            as                                             MCO_NO 
		, upper( TRIM( MED_TRT_TYP_CD ) )                    as                                     MED_TRT_TYP_CD 
		, upper( TRIM( NOC_UNT_TYP_CD ) )                    as                                     NOC_UNT_TYP_CD 
		, upper( TRIM( NOI_CTG_TYP_CD ) )                    as                                     NOI_CTG_TYP_CD 
		, upper( TRIM( NOI_TYP_CD ) )                        as                                         NOI_TYP_CD 
		, upper( TRIM( NWISP_DTL_TYP_CD_EVNT_EXPSR ) )       as                        NWISP_DTL_TYP_CD_EVNT_EXPSR 
		, upper( TRIM( NWISP_DTL_TYP_CD_NOI ) )              as                               NWISP_DTL_TYP_CD_NOI 
		, upper( TRIM( NWISP_DTL_TYP_CD_SECD_SRC_INJR ) )    as                     NWISP_DTL_TYP_CD_SECD_SRC_INJR 
		, upper( TRIM( NWISP_DTL_TYP_CD_SRC_INJR ) )         as                          NWISP_DTL_TYP_CD_SRC_INJR 
		, upper( TRIM( OCCR_MEDA_TYP_CD ) )                  as                                   OCCR_MEDA_TYP_CD 
		, upper( TRIM( OCCR_PRMS_TYP_CD ) )                  as                                   OCCR_PRMS_TYP_CD 
		, upper( TRIM( OCCR_SRC_TYP_CD ) )                   as                                    OCCR_SRC_TYP_CD 
		, upper( TRIM( POST_INJR_LBR_LVL_TYP_CD ) )          as                           POST_INJR_LBR_LVL_TYP_CD 
		, upper( TRIM( PRE_INJR_LBR_LVL_TYP_CD ) )           as                            PRE_INJR_LBR_LVL_TYP_CD 
		, STT_ID                                             as                                             STT_ID 
		, upper( TRIM( UNSL_CLM_TYP_CD ) )                   as                                    UNSL_CLM_TYP_CD 
		, upper( TRIM( ACT_IN_EFF_TYP_CD ) )                 as                                  ACT_IN_EFF_TYP_CD 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_C
            ),

LOGIC_COLCT as ( SELECT 
		  upper( TRIM( CAUS_OF_LOSS_CTG_TYP_NM ) )           as                            CAUS_OF_LOSS_CTG_TYP_NM 
		, upper( TRIM( CAUS_OF_LOSS_CTG_TYP_CD ) )           as                            CAUS_OF_LOSS_CTG_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_COLCT
            ),

LOGIC_COLT as ( SELECT 
		  upper( TRIM( CAUS_OF_LOSS_TYP_NM ) )               as                                CAUS_OF_LOSS_TYP_NM 
		, upper( TRIM( CAUS_OF_LOSS_TYP_CD ) )               as                                CAUS_OF_LOSS_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_COLT
            ),

LOGIC_CCT as ( SELECT 
		  upper( TRIM( CLM_TYP_CD ) )                        as                                         CLM_TYP_CD 
		, AGRE_ID                                            as                                            AGRE_ID 
		from SRC_CCT
            ),

LOGIC_CT as ( SELECT 
		  upper( TRIM( CLM_TYP_NM ) )                        as                                         CLM_TYP_NM 
		, upper( TRIM( CLM_TYP_CD ) )                        as                                         CLM_TYP_CD 
		from SRC_CT
            ),

LOGIC_CEST as ( SELECT 
		  upper( TRIM( CLM_EMPL_STS_TYP_NM ) )               as                                CLM_EMPL_STS_TYP_NM 
		, upper( TRIM( CLM_EMPL_STS_TYP_CD ) )               as                                CLM_EMPL_STS_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_CEST
            ),

LOGIC_ST as ( SELECT 
		  upper( TRIM( STT_ABRV ) )                          as                                           STT_ABRV 
		, upper( TRIM( STT_NM ) )                            as                                             STT_NM 
		, STT_ID                                             as                                             STT_ID 
		, upper( TRIM( STT_VOID_IND ) )                      as                                       STT_VOID_IND 
		from SRC_ST
            ),

LOGIC_CO as ( SELECT 
		  upper( TRIM( CNTRY_NM ) )                          as                                           CNTRY_NM 
		, CNTRY_ID                                           as                                           CNTRY_ID 
		, upper( TRIM( CNTRY_VOID_IND ) )                    as                                     CNTRY_VOID_IND 
		from SRC_CO
            ),

LOGIC_CRT as ( SELECT 
		  upper( TRIM( CLM_RCVR_TYP_NM ) )                   as                                    CLM_RCVR_TYP_NM 
		, upper( TRIM( CLM_RCVR_TYP_CD ) )                   as                                    CLM_RCVR_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_CRT
            ),

LOGIC_CET as ( SELECT 
		  upper( TRIM( CLM_SETL_TYP_NM ) )                   as                                    CLM_SETL_TYP_NM 
		, upper( TRIM( CLM_SETL_TYP_CD ) )                   as                                    CLM_SETL_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_CET
            ),

LOGIC_CTRPH as ( SELECT 
		  cast( CTRPH_EFF_DT as DATE )                       as                                       CTRPH_EFF_DT 
		, cast( CTRPH_END_DT as DATE )                       as                                       CTRPH_END_DT 
		, upper( TRIM( CTRPH_NM ) )                          as                                           CTRPH_NM 
		, upper( TRIM( CTRPH_CD ) )                          as                                           CTRPH_CD 
		from SRC_CTRPH
            ),

LOGIC_CIT as ( SELECT 
		  upper( TRIM( CTRPH_INJR_TYP_NM ) )                 as                                  CTRPH_INJR_TYP_NM 
		, upper( TRIM( CTRPH_INJR_TYP_CD ) )                 as                                  CTRPH_INJR_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_CIT
            ),

LOGIC_ELT as ( SELECT 
		  upper( TRIM( EDUC_LVL_TYP_NM ) )                   as                                    EDUC_LVL_TYP_NM 
		, upper( TRIM( EDUC_LVL_TYP_CD ) )                   as                                    EDUC_LVL_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_ELT
            ),

LOGIC_JT as ( SELECT 
		  upper( TRIM( JUR_TYP_NM ) )                        as                                         JUR_TYP_NM 
		, upper( TRIM( JUR_TYP_CD ) )                        as                                         JUR_TYP_CD 
		from SRC_JT
            ),

LOGIC_MTT as ( SELECT 
		  upper( TRIM( MED_TRT_TYP_NM ) )                    as                                     MED_TRT_TYP_NM 
		, upper( TRIM( MED_TRT_TYP_CD ) )                    as                                     MED_TRT_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_MTT
            ),

LOGIC_NOICT as ( SELECT 
		  upper( TRIM( NOI_CTG_TYP_NM ) )                    as                                     NOI_CTG_TYP_NM 
		, upper( TRIM( NOI_CTG_TYP_CD ) )                    as                                     NOI_CTG_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_NOICT
            ),

LOGIC_NOIT as ( SELECT 
		  upper( TRIM( NOI_TYP_NM ) )                        as                                         NOI_TYP_NM 
		, upper( TRIM( NOI_TYP_CD ) )                        as                                         NOI_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_NOIT
            ),

LOGIC_OMT as ( SELECT 
		  upper( TRIM( OCCR_MEDA_TYP_NM ) )                  as                                   OCCR_MEDA_TYP_NM 
		, upper( TRIM( OCCR_MEDA_TYP_CD ) )                  as                                   OCCR_MEDA_TYP_CD 
		from SRC_OMT
            ),

LOGIC_OPT as ( SELECT 
		  upper( TRIM( OCCR_PRMS_TYP_NM ) )                  as                                   OCCR_PRMS_TYP_NM 
		, upper( TRIM( OCCR_PRMS_TYP_CD ) )                  as                                   OCCR_PRMS_TYP_CD 
		from SRC_OPT
            ),

LOGIC_OST as ( SELECT 
		  upper( TRIM( OCCR_SRC_TYP_NM ) )                   as                                    OCCR_SRC_TYP_NM 
		, upper( TRIM( OCCR_SRC_TYP_CD ) )                   as                                    OCCR_SRC_TYP_CD 
		from SRC_OST
            ),

LOGIC_PILLT as ( SELECT 
		  upper( TRIM( PRE_INJR_LBR_LVL_TYP_NM ) )           as                            PRE_INJR_LBR_LVL_TYP_NM 
		, upper( TRIM( PRE_INJR_LBR_LVL_TYP_CD ) )           as                            PRE_INJR_LBR_LVL_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_PILLT
            ),

LOGIC_UCT as ( SELECT 
		  upper( TRIM( UNSL_CLM_TYP_NM ) )                   as                                    UNSL_CLM_TYP_NM 
		, upper( TRIM( UNSL_CLM_TYP_CD ) )                   as                                    UNSL_CLM_TYP_CD 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		from SRC_UCT
            )

---- RENAME LAYER ----
,

RENAME_C as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_NO                                             as                                             CLM_NO
		, CAUS_OF_LOSS_CTG_TYP_CD                            as                            CAUS_OF_LOSS_CTG_TYP_CD
		, CAUS_OF_LOSS_TYP_CD                                as                                CAUS_OF_LOSS_TYP_CD
		, CLM_ADMN_NTF_DT                                    as                                    CLM_ADMN_NTF_DT
		, CLM_ADMN_NTF_TIME_LOSS_DT                          as                        CLM_ADMN_NTF_TIME_LOSS_DATE
		, CLM_ALCH_INVD_IND                                  as                                  CLM_ALCH_INVD_IND
		, CLM_ANTCPT_SRGY_DT                                 as                               CLM_ANTCPT_SRGY_DATE
		, CLM_ANTCPT_SRGY_IND                                as                                CLM_ANTCPT_SRGY_IND
		, CLM_BILL_ERLST_DT                                  as                                CLM_BILL_ERLST_DATE
		, CLM_BILL_LTST_DT                                   as                                 CLM_BILL_LTST_DATE
		, CLM_CLMT_FATL_IND                                  as                                  CLM_CLMT_FATL_IND
		, CLM_CLMT_HIRE_DT                                   as                                 CLM_CLMT_HIRE_DATE
		, CLM_CLMT_HIRE_STT_ID                               as                               CLM_CLMT_HIRE_STT_ID
		, CLM_CLMT_JOB_TTL                                   as                                   CLM_CLMT_JOB_TTL
		, CLM_CLMT_LST_WK_DT                                 as                               CLM_CLMT_LST_WK_DATE
		, CLM_CLMT_NTF_DT                                    as                                    CLM_CLMT_NTF_DT
		, CLM_CLMT_SHFT_BGN_TIME                             as                             CLM_CLMT_SHFT_BGN_TIME
		, CLM_CNTC_EMAIL                                     as                                     CLM_CNTC_EMAIL
		, CLM_CNTC_NM                                        as                                        CLM_CNTC_NM
		, CLM_CNTC_PHN                                       as                                       CLM_CNTC_PHN
		, CLM_COV_TYP_CD                                     as                                     CLM_COV_TYP_CD
		, CLM_CTRPH_INJR_CMT                                 as                                 CLM_CTRPH_INJR_CMT
		, CLM_CTRPH_INJR_IND                                 as                                 CLM_CTRPH_INJR_IND
		, CLM_DISAB_BGN_DT                                   as                                 CLM_DISAB_BGN_DATE
		, CLM_DO_NOT_CLOS_IND                                as                                CLM_DO_NOT_CLOS_IND
		, CLM_DO_NOT_REFR_TO_RHBL_IND                        as                        CLM_DO_NOT_REFR_TO_RHBL_IND
		, CLM_DRUG_INVD_IND                                  as                                  CLM_DRUG_INVD_IND
		, CLM_EMPLR_NTF_DT                                   as                                   CLM_EMPLR_NTF_DT
		, CLM_EMPLR_NTF_TIME_LOSS_DT                         as                       CLM_EMPLR_NTF_TIME_LOSS_DATE
		, CLM_EMPL_STS_TYP_CD                                as                                CLM_EMPL_STS_TYP_CD
		, CLM_FST_DCSN_DT                                    as                                  CLM_FST_DCSN_DATE
		, CLM_FST_RPT_DISAB_DT                               as                             CLM_FST_RPT_DISAB_DATE
		, CLM_FULL_WGS_PD_IND                                as                                CLM_FULL_WGS_PD_IND
		, CLM_GRP_TYP_CD                                     as                                     CLM_GRP_TYP_CD
		, CLM_INDM_PAY_DD_NOT_PD                             as                             CLM_INDM_PAY_DD_NOT_PD
		, CLM_INVSTG_TYP_CD                                  as                                  CLM_INVSTG_TYP_CD
		, CLM_LOSS_DESC                                      as                                      CLM_LOSS_DESC
		, CLM_LOSS_TYP_CD                                    as                                    CLM_LOSS_TYP_CD
		, CLM_MULTI_OCCR_IND                                 as                                 CLM_MULTI_OCCR_IND
		, CLM_NOTE_IND                                       as                                       CLM_NOTE_IND
		, CLM_NO_DRV_UPCS                                    as                                    CLM_NO_DRV_UPCS
		, CLM_OCCR_DTM                                       as                                       CLM_OCCR_DTM
		, CLM_OCCR_DTM_VRFY_IND                              as                              CLM_OCCR_DTM_VRFY_IND
		, CLM_OCCR_LOC_COMT                                  as                                  CLM_OCCR_LOC_COMT
		, CLM_OCCR_LOC_LAT                                   as                                   CLM_OCCR_LOC_LAT
		, CLM_OCCR_LOC_LONG                                  as                                  CLM_OCCR_LOC_LONG
		, CLM_OCCR_LOC_NM                                    as                                    CLM_OCCR_LOC_NM
		, CLM_OCCR_LOC_STR_1                                 as                                 CLM_OCCR_LOC_STR_1
		, CLM_OCCR_LOC_STR_2                                 as                                 CLM_OCCR_LOC_STR_2
		, CLM_OCCR_LOC_CITY_NM                               as                               CLM_OCCR_LOC_CITY_NM
		, CLM_OCCR_LOC_CITY_UPCS_SRCH_NM                     as                     CLM_OCCR_LOC_CITY_UPCS_SRCH_NM
		, CLM_OCCR_LOC_POST_CD                               as                               CLM_OCCR_LOC_POST_CD
		, CLM_OCCR_LOC_CNTY_NM                               as                               CLM_OCCR_LOC_CNTY_NM
		, CLM_OCCR_LOC_CNTY_UPCS_SRCH_NM                     as                     CLM_OCCR_LOC_CNTY_UPCS_SRCH_NM
		, CLM_OCCR_NOI_DESC                                  as                                  CLM_OCCR_NOI_DESC
		, CLM_OCCR_PRPR_DT                                   as                                 CLM_OCCR_PRPR_DATE
		, CLM_OCCR_PRPR_NM                                   as                                   CLM_OCCR_PRPR_NM
		, CLM_OCCR_PRPR_PHN                                  as                                  CLM_OCCR_PRPR_PHN
		, CLM_OCCR_PRPR_TTL                                  as                                  CLM_OCCR_PRPR_TTL
		, CLM_OCCR_RPT_DT                                    as                                  CLM_OCCR_RPT_DATE
		, CLM_OCCR_TM_SET_IND                                as                                CLM_OCCR_TM_SET_IND
		, CLM_PRSMPTN_IND                                    as                                    CLM_PRSMPTN_IND
		, CLM_PRVS_IMPR_AWARD_IND                            as                            CLM_PRVS_IMPR_AWARD_IND
		, CLM_RCVR_TYP_CD                                    as                                    CLM_RCVR_TYP_CD
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND
		, CLM_RTN_WK_DIFF_EMPLR_IND                          as                          CLM_RTN_WK_DIFF_EMPLR_IND
		, CLM_SCND_INJR_DT                                   as                                 CLM_SCND_INJR_DATE
		, CLM_SETL_TYP_CD                                    as                                    CLM_SETL_TYP_CD
		, CLM_UNDRGN_SRGY_DT                                 as                               CLM_UNDRGN_SRGY_DATE
		, CLM_UNDRGN_SRGY_IND                                as                                CLM_UNDRGN_SRGY_IND
		, CNTRY_ID                                           as                                           CNTRY_ID
		, CTRPH_CD                                           as                                           CTRPH_CD
		, CTRPH_INJR_TYP_CD                                  as                                  CTRPH_INJR_TYP_CD
		, EDUC_LVL_TYP_CD                                    as                                    EDUC_LVL_TYP_CD
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM
		, HIST_END_DTM                                       as                                       HIST_END_DTM
		, JUR_TYP_CD                                         as                                         JUR_TYP_CD
		, LOB_TYP_CD                                         as                                         LOB_TYP_CD
		, LOSS_COV_TYP_ID                                    as                                    LOSS_COV_TYP_ID
		, MCO_NO                                             as                                             MCO_NO
		, MED_TRT_TYP_CD                                     as                                     MED_TRT_TYP_CD
		, NOC_UNT_TYP_CD                                     as                                     NOC_UNT_TYP_CD
		, NOI_CTG_TYP_CD                                     as                                     NOI_CTG_TYP_CD
		, NOI_TYP_CD                                         as                                         NOI_TYP_CD
		, NWISP_DTL_TYP_CD_EVNT_EXPSR                        as                        NWISP_DTL_TYP_CD_EVNT_EXPSR
		, NWISP_DTL_TYP_CD_NOI                               as                               NWISP_DTL_TYP_CD_NOI
		, NWISP_DTL_TYP_CD_SECD_SRC_INJR                     as                     NWISP_DTL_TYP_CD_SECD_SRC_INJR
		, NWISP_DTL_TYP_CD_SRC_INJR                          as                          NWISP_DTL_TYP_CD_SRC_INJR
		, OCCR_MEDA_TYP_CD                                   as                                   OCCR_MEDA_TYP_CD
		, OCCR_PRMS_TYP_CD                                   as                                   OCCR_PRMS_TYP_CD
		, OCCR_SRC_TYP_CD                                    as                                    OCCR_SRC_TYP_CD
		, POST_INJR_LBR_LVL_TYP_CD                           as                           POST_INJR_LBR_LVL_TYP_CD
		, PRE_INJR_LBR_LVL_TYP_CD                            as                            PRE_INJR_LBR_LVL_TYP_CD
		, STT_ID                                             as                                             STT_ID
		, UNSL_CLM_TYP_CD                                    as                                    UNSL_CLM_TYP_CD
		, ACT_IN_EFF_TYP_CD                                  as                                  ACT_IN_EFF_TYP_CD
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_C   ), 
RENAME_COLCT as ( SELECT 
		  CAUS_OF_LOSS_CTG_TYP_NM                            as                            CAUS_OF_LOSS_CTG_TYP_NM
		, CAUS_OF_LOSS_CTG_TYP_CD                            as                      COLCT_CAUS_OF_LOSS_CTG_TYP_CD
		, VOID_IND                                           as                                     COLCT_VOID_IND 
				FROM     LOGIC_COLCT   ), 
RENAME_COLT as ( SELECT 
		  CAUS_OF_LOSS_TYP_NM                                as                                CAUS_OF_LOSS_TYP_NM
		, CAUS_OF_LOSS_TYP_CD                                as                           COLT_CAUS_OF_LOSS_TYP_CD
		, VOID_IND                                           as                                      COLT_VOID_IND 
				FROM     LOGIC_COLT   ), 
RENAME_CCT as ( SELECT 
		  CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, AGRE_ID                                            as                                        CCT_AGRE_ID 
				FROM     LOGIC_CCT   ), 
RENAME_CT as ( SELECT 
		  CLM_TYP_NM                                         as                                         CLM_TYP_NM
		, CLM_TYP_CD                                         as                                      CT_CLM_TYP_CD 
				FROM     LOGIC_CT   ), 
RENAME_CEST as ( SELECT 
		  CLM_EMPL_STS_TYP_NM                                as                                CLM_EMPL_STS_TYP_NM
		, CLM_EMPL_STS_TYP_CD                                as                           CEST_CLM_EMPL_STS_TYP_CD
		, VOID_IND                                           as                                      CEST_VOID_IND 
				FROM     LOGIC_CEST   ), 
RENAME_ST as ( SELECT 
		  STT_ABRV                                           as                                CLM_OCCR_LOC_STT_CD
		, STT_NM                                             as                                CLM_OCCR_LOC_STT_NM
		, STT_ID                                             as                                          ST_STT_ID
		, STT_VOID_IND                                       as                                    ST_STT_VOID_IND 
				FROM     LOGIC_ST   ), 
RENAME_CO as ( SELECT 
		  CNTRY_NM                                           as                              CLM_OCCR_LOC_CNTRY_NM
		, CNTRY_ID                                           as                                        CO_CNTRY_ID
		, CNTRY_VOID_IND                                     as                                  CO_CNTRY_VOID_IND 
				FROM     LOGIC_CO   ), 
RENAME_CRT as ( SELECT 
		  CLM_RCVR_TYP_NM                                    as                                    CLM_RCVR_TYP_NM
		, CLM_RCVR_TYP_CD                                    as                                CRT_CLM_RCVR_TYP_CD
		, VOID_IND                                           as                                       CRT_VOID_IND 
				FROM     LOGIC_CRT   ), 
RENAME_CET as ( SELECT 
		  CLM_SETL_TYP_NM                                    as                                    CLM_SETL_TYP_NM
		, CLM_SETL_TYP_CD                                    as                                CET_CLM_SETL_TYP_CD
		, VOID_IND                                           as                                       CET_VOID_IND 
				FROM     LOGIC_CET   ), 
RENAME_CTRPH as ( SELECT 
		  CTRPH_EFF_DT                                       as                                     CTRPH_EFF_DATE
		, CTRPH_END_DT                                       as                                     CTRPH_END_DATE
		, CTRPH_NM                                           as                                           CTRPH_NM
		, CTRPH_CD                                           as                                     CTRPH_CTRPH_CD 
				FROM     LOGIC_CTRPH   ), 
RENAME_CIT as ( SELECT 
		  CTRPH_INJR_TYP_NM                                  as                                  CTRPH_INJR_TYP_NM
		, CTRPH_INJR_TYP_CD                                  as                              CIT_CTRPH_INJR_TYP_CD
		, VOID_IND                                           as                                       CIT_VOID_IND 
				FROM     LOGIC_CIT   ), 
RENAME_ELT as ( SELECT 
		  EDUC_LVL_TYP_NM                                    as                                    EDUC_LVL_TYP_NM
		, EDUC_LVL_TYP_CD                                    as                                ELT_EDUC_LVL_TYP_CD
		, VOID_IND                                           as                                       ELT_VOID_IND 
				FROM     LOGIC_ELT   ), 
RENAME_JT as ( SELECT 
		  JUR_TYP_NM                                         as                                         JUR_TYP_NM
		, JUR_TYP_CD                                         as                                      JT_JUR_TYP_CD 
				FROM     LOGIC_JT   ), 
RENAME_MTT as ( SELECT 
		  MED_TRT_TYP_NM                                     as                                     MED_TRT_TYP_NM
		, MED_TRT_TYP_CD                                     as                                 MTT_MED_TRT_TYP_CD
		, VOID_IND                                           as                                       MTT_VOID_IND 
				FROM     LOGIC_MTT   ), 
RENAME_NOICT as ( SELECT 
		  NOI_CTG_TYP_NM                                     as                                     NOI_CTG_TYP_NM
		, NOI_CTG_TYP_CD                                     as                               NOICT_NOI_CTG_TYP_CD
		, VOID_IND                                           as                                     NOICT_VOID_IND 
				FROM     LOGIC_NOICT   ), 
RENAME_NOIT as ( SELECT 
		  NOI_TYP_NM                                         as                                         NOI_TYP_NM
		, NOI_TYP_CD                                         as                                    NOIT_NOI_TYP_CD
		, VOID_IND                                           as                                      NOIT_VOID_IND 
				FROM     LOGIC_NOIT   ), 
RENAME_OMT as ( SELECT 
		  OCCR_MEDA_TYP_NM                                   as                                   OCCR_MEDA_TYP_NM
		, OCCR_MEDA_TYP_CD                                   as                               OMT_OCCR_MEDA_TYP_CD 
				FROM     LOGIC_OMT   ), 
RENAME_OPT as ( SELECT 
		  OCCR_PRMS_TYP_NM                                   as                                   OCCR_PRMS_TYP_NM
		, OCCR_PRMS_TYP_CD                                   as                               OPT_OCCR_PRMS_TYP_CD 
				FROM     LOGIC_OPT   ), 
RENAME_OST as ( SELECT 
		  OCCR_SRC_TYP_NM                                    as                                    OCCR_SRC_TYP_NM
		, OCCR_SRC_TYP_CD                                    as                                OST_OCCR_SRC_TYP_CD 
				FROM     LOGIC_OST   ), 
RENAME_PILLT as ( SELECT 
		  PRE_INJR_LBR_LVL_TYP_NM                            as                            PRE_INJR_LBR_LVL_TYP_NM
		, PRE_INJR_LBR_LVL_TYP_CD                            as                      PILLT_PRE_INJR_LBR_LVL_TYP_CD
		, VOID_IND                                           as                                     PILLT_VOID_IND 
				FROM     LOGIC_PILLT   ), 
RENAME_UCT as ( SELECT 
		  UNSL_CLM_TYP_NM                                    as                                    UNSL_CLM_TYP_NM
		, UNSL_CLM_TYP_CD                                    as                                UCT_UNSL_CLM_TYP_CD
		, VOID_IND                                           as                                       UCT_VOID_IND 
				FROM     LOGIC_UCT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_C                              as ( SELECT * from    RENAME_C   ),
FILTER_ST                             as ( SELECT * from    RENAME_ST 
                                            WHERE ST_STT_VOID_IND = 'N'  ),
FILTER_CO                             as ( SELECT * from    RENAME_CO 
                                            WHERE CO_CNTRY_VOID_IND = 'N'  ),
FILTER_JT                             as ( SELECT * from    RENAME_JT   ),
FILTER_UCT                            as ( SELECT * from    RENAME_UCT 
                                            WHERE UCT_VOID_IND = 'N'  ),
FILTER_OMT                            as ( SELECT * from    RENAME_OMT   ),
FILTER_OPT                            as ( SELECT * from    RENAME_OPT   ),
FILTER_OST                            as ( SELECT * from    RENAME_OST   ),
FILTER_CCT                            as ( SELECT * from    RENAME_CCT   ),
FILTER_CT                             as ( SELECT * from    RENAME_CT   ),
FILTER_ELT                            as ( SELECT * from    RENAME_ELT 
                                            WHERE ELT_VOID_IND = 'N'  ),
FILTER_CEST                           as ( SELECT * from    RENAME_CEST 
                                            WHERE CEST_VOID_IND = 'N'  ),
FILTER_NOIT                           as ( SELECT * from    RENAME_NOIT 
                                            WHERE NOIT_VOID_IND = 'N'  ),
FILTER_NOICT                          as ( SELECT * from    RENAME_NOICT 
                                            WHERE NOICT_VOID_IND = 'N'  ),
FILTER_COLT                           as ( SELECT * from    RENAME_COLT 
                                            WHERE COLT_VOID_IND = 'N'  ),
FILTER_COLCT                          as ( SELECT * from    RENAME_COLCT 
                                            WHERE COLCT_VOID_IND = 'N'  ),
FILTER_CRT                            as ( SELECT * from    RENAME_CRT 
                                            WHERE CRT_VOID_IND = 'N'  ),
FILTER_CET                            as ( SELECT * from    RENAME_CET 
                                            WHERE CET_VOID_IND = 'N'  ),
FILTER_CTRPH                          as ( SELECT * from    RENAME_CTRPH   ),
FILTER_CIT                            as ( SELECT * from    RENAME_CIT 
                                            WHERE CIT_VOID_IND = 'N'  ),
FILTER_MTT                            as ( SELECT * from    RENAME_MTT 
                                            WHERE MTT_VOID_IND = 'N'  ),
FILTER_PILLT                          as ( SELECT * from    RENAME_PILLT 
                                            WHERE PILLT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CCT as ( SELECT * 
				FROM  FILTER_CCT
				LEFT JOIN FILTER_CT ON  FILTER_CCT.CLM_TYP_CD =  FILTER_CT.CT_CLM_TYP_CD  ),
C as ( SELECT * 
				FROM  FILTER_C
				LEFT JOIN FILTER_ST ON  FILTER_C.STT_ID =  FILTER_ST.ST_STT_ID 
								LEFT JOIN FILTER_CO ON  FILTER_C.CNTRY_ID =  FILTER_CO.CO_CNTRY_ID 
								LEFT JOIN FILTER_JT ON  FILTER_C.JUR_TYP_CD =  FILTER_JT.JT_JUR_TYP_CD 
								LEFT JOIN FILTER_UCT ON  FILTER_C.UNSL_CLM_TYP_CD =  FILTER_UCT.UCT_UNSL_CLM_TYP_CD 
								LEFT JOIN FILTER_OMT ON  FILTER_C.OCCR_MEDA_TYP_CD =  FILTER_OMT.OMT_OCCR_MEDA_TYP_CD 
								LEFT JOIN FILTER_OPT ON  FILTER_C.OCCR_PRMS_TYP_CD =  FILTER_OPT.OPT_OCCR_PRMS_TYP_CD 
								LEFT JOIN FILTER_OST ON  FILTER_C.OCCR_SRC_TYP_CD =  FILTER_OST.OST_OCCR_SRC_TYP_CD 
						LEFT JOIN CCT ON  FILTER_C.AGRE_ID = CCT.CCT_AGRE_ID 
								LEFT JOIN FILTER_ELT ON  FILTER_C.EDUC_LVL_TYP_CD =  FILTER_ELT.ELT_EDUC_LVL_TYP_CD 
								LEFT JOIN FILTER_CEST ON  FILTER_C.CLM_EMPL_STS_TYP_CD =  FILTER_CEST.CEST_CLM_EMPL_STS_TYP_CD 
								LEFT JOIN FILTER_NOIT ON  FILTER_C.NOI_TYP_CD =  FILTER_NOIT.NOIT_NOI_TYP_CD 
								LEFT JOIN FILTER_NOICT ON  FILTER_C.NOI_CTG_TYP_CD =  FILTER_NOICT.NOICT_NOI_CTG_TYP_CD 
								LEFT JOIN FILTER_COLT ON  FILTER_C.CAUS_OF_LOSS_TYP_CD =  FILTER_COLT.COLT_CAUS_OF_LOSS_TYP_CD 
								LEFT JOIN FILTER_COLCT ON  FILTER_C.CAUS_OF_LOSS_CTG_TYP_CD =  FILTER_COLCT.COLCT_CAUS_OF_LOSS_CTG_TYP_CD 
								LEFT JOIN FILTER_CRT ON  FILTER_C.CLM_RCVR_TYP_CD =  FILTER_CRT.CRT_CLM_RCVR_TYP_CD 
								LEFT JOIN FILTER_CET ON  FILTER_C.CLM_SETL_TYP_CD =  FILTER_CET.CET_CLM_SETL_TYP_CD 
								LEFT JOIN FILTER_CTRPH ON  FILTER_C.CTRPH_CD =  FILTER_CTRPH.CTRPH_CTRPH_CD 
								LEFT JOIN FILTER_CIT ON  FILTER_C.CTRPH_INJR_TYP_CD =  FILTER_CIT.CIT_CTRPH_INJR_TYP_CD 
								LEFT JOIN FILTER_MTT ON  FILTER_C.MED_TRT_TYP_CD =  FILTER_MTT.MTT_MED_TRT_TYP_CD 
								LEFT JOIN FILTER_PILLT ON  FILTER_C.PRE_INJR_LBR_LVL_TYP_CD =  FILTER_PILLT.PILLT_PRE_INJR_LBR_LVL_TYP_CD  )
SELECT 
HIST_ID
, AGRE_ID
, CLM_NO
, CAUS_OF_LOSS_CTG_TYP_CD
, CAUS_OF_LOSS_CTG_TYP_NM
, CAUS_OF_LOSS_TYP_CD
, CAUS_OF_LOSS_TYP_NM
, CLM_TYP_CD
, CLM_TYP_NM
, CLM_ADMN_NTF_DT
, CLM_ADMN_NTF_TIME_LOSS_DATE
, CLM_ALCH_INVD_IND
, CLM_ANTCPT_SRGY_DATE
, CLM_ANTCPT_SRGY_IND
, CLM_BILL_ERLST_DATE
, CLM_BILL_LTST_DATE
, CLM_CLMT_FATL_IND
, CLM_CLMT_HIRE_DATE
, CLM_CLMT_HIRE_STT_ID
, CLM_CLMT_JOB_TTL
, CLM_CLMT_LST_WK_DATE
, CLM_CLMT_NTF_DT
, CLM_CLMT_SHFT_BGN_TIME
, CLM_CNTC_EMAIL
, CLM_CNTC_NM
, CLM_CNTC_PHN
, CLM_COV_TYP_CD
, CLM_CTRPH_INJR_CMT
, CLM_CTRPH_INJR_IND
, CLM_DISAB_BGN_DATE
, CLM_DO_NOT_CLOS_IND
, CLM_DO_NOT_REFR_TO_RHBL_IND
, CLM_DRUG_INVD_IND
, CLM_EMPLR_NTF_DT
, CLM_EMPLR_NTF_TIME_LOSS_DATE
, CLM_EMPL_STS_TYP_CD
, CLM_EMPL_STS_TYP_NM
, CLM_FST_DCSN_DATE
, CLM_FST_RPT_DISAB_DATE
, CLM_FULL_WGS_PD_IND
, CLM_GRP_TYP_CD
, CLM_INDM_PAY_DD_NOT_PD
, CLM_INVSTG_TYP_CD
, CLM_LOSS_DESC
, CLM_LOSS_TYP_CD
, CLM_MULTI_OCCR_IND
, CLM_NOTE_IND
, CLM_NO_DRV_UPCS
, CLM_OCCR_DTM
, CLM_OCCR_DTM_VRFY_IND
, CLM_OCCR_LOC_COMT
, CLM_OCCR_LOC_LAT
, CLM_OCCR_LOC_LONG
, CLM_OCCR_LOC_NM
, CLM_OCCR_LOC_STR_1
, CLM_OCCR_LOC_STR_2
, CLM_OCCR_LOC_CITY_NM
, CLM_OCCR_LOC_CITY_UPCS_SRCH_NM
, CLM_OCCR_LOC_POST_CD
, CLM_OCCR_LOC_CNTY_NM
, CLM_OCCR_LOC_CNTY_UPCS_SRCH_NM
, CLM_OCCR_LOC_STT_CD
, CLM_OCCR_LOC_STT_NM
, CLM_OCCR_LOC_CNTRY_NM
, CLM_OCCR_NOI_DESC
, CLM_OCCR_PRPR_DATE
, CLM_OCCR_PRPR_NM
, CLM_OCCR_PRPR_PHN
, CLM_OCCR_PRPR_TTL
, CLM_OCCR_RPT_DATE
, CLM_OCCR_TM_SET_IND
, CLM_PRSMPTN_IND
, CLM_PRVS_IMPR_AWARD_IND
, CLM_RCVR_TYP_CD
, CLM_RCVR_TYP_NM
, CLM_REL_SNPSHT_IND
, CLM_RTN_WK_DIFF_EMPLR_IND
, CLM_SCND_INJR_DATE
, CLM_SETL_TYP_CD
, CLM_SETL_TYP_NM
, CLM_UNDRGN_SRGY_DATE
, CLM_UNDRGN_SRGY_IND
, CNTRY_ID
, CTRPH_CD
, CTRPH_EFF_DATE
, CTRPH_END_DATE
, CTRPH_NM
, CTRPH_INJR_TYP_CD
, CTRPH_INJR_TYP_NM
, EDUC_LVL_TYP_CD
, EDUC_LVL_TYP_NM
, HIST_EFF_DTM
, HIST_END_DTM
, JUR_TYP_CD
, JUR_TYP_NM
, LOB_TYP_CD
, LOSS_COV_TYP_ID
, MCO_NO
, MED_TRT_TYP_CD
, MED_TRT_TYP_NM
, NOC_UNT_TYP_CD
, NOI_CTG_TYP_CD
, NOI_CTG_TYP_NM
, NOI_TYP_CD
, NOI_TYP_NM
, NWISP_DTL_TYP_CD_EVNT_EXPSR
, NWISP_DTL_TYP_CD_NOI
, NWISP_DTL_TYP_CD_SECD_SRC_INJR
, NWISP_DTL_TYP_CD_SRC_INJR
, OCCR_MEDA_TYP_CD
, OCCR_MEDA_TYP_NM
, OCCR_PRMS_TYP_CD
, OCCR_PRMS_TYP_NM
, OCCR_SRC_TYP_CD
, OCCR_SRC_TYP_NM
, POST_INJR_LBR_LVL_TYP_CD
, PRE_INJR_LBR_LVL_TYP_CD
, PRE_INJR_LBR_LVL_TYP_NM
, STT_ID
, UNSL_CLM_TYP_CD
, UNSL_CLM_TYP_NM
, ACT_IN_EFF_TYP_CD
, AUDIT_USER_CREA_DTM
, AUDIT_USER_ID_CREA
, AUDIT_USER_ID_UPDT
, AUDIT_USER_UPDT_DTM
from C
      );
    