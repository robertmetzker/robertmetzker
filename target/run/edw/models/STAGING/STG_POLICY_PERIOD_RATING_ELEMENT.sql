

      create or replace  table DEV_EDW.STAGING.STG_POLICY_PERIOD_RATING_ELEMENT  as
      (---- SRC LAYER ----
WITH
SRC_PPRE as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD_RATING_ELEMENT ),
SRC_PP as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD ),
SRC_RELT as ( SELECT *     from     DEV_VIEWS.PCMP.RATING_ELEMENT_LMCJ_TYPE ),
SRC_RENT as ( SELECT *     from     DEV_VIEWS.PCMP.RATING_ELEMENT_NAME_TYPE ),
SRC_RET as ( SELECT *     from     DEV_VIEWS.PCMP.RATING_ELEMENT_TYPE ),
SRC_REUT as ( SELECT *     from     DEV_VIEWS.PCMP.RATING_ELEMENT_USAGE_TYPE ),
SRC_DISP as ( SELECT *     from     DEV_VIEWS.PCMP.RATING_ELEMENT_DISPLAY_TYPE ),
SRC_WCRTT as ( SELECT *     from     DEV_VIEWS.PCMP.WC_CLASS_RATE_TIER_TYPE ),
SRC_EM as ( SELECT *     from     DEV_VIEWS.PCMP.EXPERIENCE_MODIFIER ),
SRC_EMT as ( SELECT *     from     DEV_VIEWS.PCMP.EXPERIENCE_MODIFIER_TYPE ),
SRC_DED as ( SELECT *     from     DEV_VIEWS.PCMP.RATING_ELEMENT_DEDUCTIBLE_TYPE ),
SRC_DT as ( SELECT *     from     DEV_VIEWS.PCMP.DEDUCTIBLE_TYPE ),
SRC_DCT as ( SELECT *     from     DEV_VIEWS.PCMP.DEDUCTIBLE_CATEGORY_TYPE ),
SRC_HGT1 as ( SELECT *     from     DEV_VIEWS.PCMP.HAZARD_GROUP_TYPE ),
SRC_LDD as ( SELECT *     from     DEV_VIEWS.PCMP.LARGE_DEDUCTIBLE_DETAIL ),
SRC_BLDD as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_LARGE_DEDUCTIBLE_DETAIL ),
SRC_HGT2 as ( SELECT *     from     DEV_VIEWS.PCMP.HAZARD_GROUP_TYPE ),
//SRC_PPRE as ( SELECT *     from     POLICY_PERIOD_RATING_ELEMENT) ,
//SRC_PP as ( SELECT *     from     POLICY_PERIOD) ,
//SRC_RELT as ( SELECT *     from     RATING_ELEMENT_LMCJ_TYPE) ,
//SRC_RENT as ( SELECT *     from     RATING_ELEMENT_NAME_TYPE) ,
//SRC_RET as ( SELECT *     from     RATING_ELEMENT_TYPE) ,
//SRC_REUT as ( SELECT *     from     RATING_ELEMENT_USAGE_TYPE) ,
//SRC_DISP as ( SELECT *     from     RATING_ELEMENT_DISPLAY_TYPE) ,
//SRC_WCRTT as ( SELECT *     from     WC_CLASS_RATE_TIER_TYPE) ,
//SRC_EM as ( SELECT *     from     EXPERIENCE_MODIFIER) ,
//SRC_EMT as ( SELECT *     from     EXPERIENCE_MODIFIER_TYPE) ,
//SRC_DED as ( SELECT *     from     RATING_ELEMENT_DEDUCTIBLE_TYPE) ,
//SRC_DT as ( SELECT *     from     DEDUCTIBLE_TYPE) ,
//SRC_DCT as ( SELECT *     from     DEDUCTIBLE_CATEGORY_TYPE) ,
//SRC_HGT1 as ( SELECT *     from     HAZARD_GROUP_TYPE) ,
//SRC_LDD as ( SELECT *     from     LARGE_DEDUCTIBLE_DETAIL) ,
//SRC_BLDD as ( SELECT *     from     BWC_LARGE_DEDUCTIBLE_DETAIL) ,
//SRC_HGT2 as ( SELECT *     from     HAZARD_GROUP_TYPE) ,

---- LOGIC LAYER ----

LOGIC_PPRE as ( SELECT 
		  PPRE_ID                                            AS                                            PPRE_ID 
		, PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		, cast( PPRE_EFF_DT as DATE )                        AS                                        PPRE_EFF_DT 
		, cast( PPRE_END_DT as DATE )                        AS                                        PPRE_END_DT 
		, RT_ELEM_LMCJ_TYP_ID                                AS                                RT_ELEM_LMCJ_TYP_ID 
		, PPRE_RT                                            AS                                            PPRE_RT 
		, upper( TRIM( WC_CLS_RT_TIER_TYP_CD ) )             AS                              WC_CLS_RT_TIER_TYP_CD 
		, EXPRN_MOD_ID                                       AS                                       EXPRN_MOD_ID 
		, cast( PPRE_ARD_DT as DATE )                        AS                                        PPRE_ARD_DT 
		, upper( PPRE_ARD_OVRRD_IND )                        AS                                 PPRE_ARD_OVRRD_IND 
		, upper( PPRE_RN_IND )                               AS                                        PPRE_RN_IND 
		, PPRE_DVND_OVRD_AMT                                 AS                                 PPRE_DVND_OVRD_AMT 
		, PPRE_DVND_OVRD_PCT                                 AS                                 PPRE_DVND_OVRD_PCT 
		, PPRE_RTSP_TAX_MLTPLR                               AS                               PPRE_RTSP_TAX_MLTPLR 
		, PPRE_RTSP_EXCS_LOSS_PREM_FCTR                      AS                      PPRE_RTSP_EXCS_LOSS_PREM_FCTR 
		, RT_ELEM_DED_TYP_ID                                 AS                                 RT_ELEM_DED_TYP_ID 
		, LDD_ID                                             AS                                             LDD_ID 
		, upper( PPRE_NOTE_IND )                             AS                                      PPRE_NOTE_IND 
		, upper( PPRE_MNL_ADD_IND )                          AS                                   PPRE_MNL_ADD_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PPRE
            ),
LOGIC_PP as ( SELECT 
		  TRIM( PLCY_NO )                                    AS                                            PLCY_NO 
		, PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
        , UPPER(VOID_IND)                                    AS                                        PP_VOID_IND
		from SRC_PP
            ),
LOGIC_RELT as ( SELECT 
		  upper( TRIM( RT_ELEM_TYP_CD ) )                    AS                                     RT_ELEM_TYP_CD 
		, upper( TRIM( RT_ELEM_DSPLY_TYP_CD ) )              AS                               RT_ELEM_DSPLY_TYP_CD 
		, upper( RT_ELEM_LMCJ_TYP_DSPLY_IND )                AS                         RT_ELEM_LMCJ_TYP_DSPLY_IND 
		, upper( RT_ELEM_LMCJ_TYP_WEB_DSPLY_IND )            AS                     RT_ELEM_LMCJ_TYP_WEB_DSPLY_IND 
		, upper( RT_ELEM_LMCJ_TYP_USER_ENT_IND )             AS                      RT_ELEM_LMCJ_TYP_USER_ENT_IND 
		, upper( RT_ELEM_LMCJ_TYP_PREM_CALC_IND )            AS                     RT_ELEM_LMCJ_TYP_PREM_CALC_IND 
		, upper( RT_ELEM_LMCJ_TYP_PED_OVRRD_IND )            AS                     RT_ELEM_LMCJ_TYP_PED_OVRRD_IND 
		, upper( RT_ELEM_LMCJ_TYP_ZRO_DSPLY_IND )            AS                     RT_ELEM_LMCJ_TYP_ZRO_DSPLY_IND 
		, upper( RT_ELEM_LMCJ_TYP_USER_SEL_IND )             AS                      RT_ELEM_LMCJ_TYP_USER_SEL_IND 
		, RT_ELEM_LMCJ_TYP_ID                                AS                                RT_ELEM_LMCJ_TYP_ID 
		, upper( RT_ELEM_LMCJ_TYP_VOID_IND )                 AS                          RT_ELEM_LMCJ_TYP_VOID_IND 
		from SRC_RELT
            ),
LOGIC_RENT as ( SELECT 
		  upper( TRIM( RT_ELEM_TYP_NM ) )                    AS                                     RT_ELEM_TYP_NM 
		, upper( TRIM( RT_ELEM_TYP_CD ) )                    AS                                     RT_ELEM_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_RENT
            ),
LOGIC_RET as ( SELECT 
		  upper( TRIM( RT_ELEM_USAGE_TYP_CD ) )              AS                               RT_ELEM_USAGE_TYP_CD 
		, upper( TRIM( RT_ELEM_TYP_CD ) )                    AS                                     RT_ELEM_TYP_CD 
		, upper( RT_ELEM_TYP_VOID_IND )                      AS                               RT_ELEM_TYP_VOID_IND 
		from SRC_RET
            ),
LOGIC_REUT as ( SELECT 
		  upper( TRIM( RT_ELEM_USAGE_TYP_DESC ) )            AS                             RT_ELEM_USAGE_TYP_DESC 
		, upper( TRIM( RT_ELEM_USAGE_TYP_CD ) )              AS                               RT_ELEM_USAGE_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_REUT
            ),
LOGIC_DISP as ( SELECT 
		  upper( TRIM( RT_ELEM_DSPLY_TYP_NM ) )              AS                               RT_ELEM_DSPLY_TYP_NM 
		, upper( TRIM( RT_ELEM_DSPLY_TYP_CD ) )              AS                               RT_ELEM_DSPLY_TYP_CD 
		, upper( RT_ELEM_DSPLY_TYP_VOID_IND )                AS                         RT_ELEM_DSPLY_TYP_VOID_IND 
		from SRC_DISP
            ),
LOGIC_WCRTT as ( SELECT 
		  upper( TRIM( WC_CLS_RT_TIER_TYP_NM ) )             AS                              WC_CLS_RT_TIER_TYP_NM 
		, upper( TRIM( WC_CLS_RT_TIER_TYP_CD ) )             AS                              WC_CLS_RT_TIER_TYP_CD 
		, upper( WC_CLS_RT_TIER_TYP_VOID_IND )               AS                        WC_CLS_RT_TIER_TYP_VOID_IND 
		from SRC_WCRTT
            ),
LOGIC_EM as ( SELECT 
		  upper( TRIM( EXPRN_MOD_TYP_CD ) )                  AS                                   EXPRN_MOD_TYP_CD 
		, EXPRN_MOD_FCTR                                     AS                                     EXPRN_MOD_FCTR 
		, cast( EXPRN_MOD_EFF_DT as DATE )                   AS                                   EXPRN_MOD_EFF_DT 
		, cast( EXPRN_MOD_END_DT as DATE )                   AS                                   EXPRN_MOD_END_DT 
		, cast( EXPRN_MOD_ANV_RT_DT as DATE )                AS                                EXPRN_MOD_ANV_RT_DT 
		, EXPRN_MOD_ID                                       AS                                       EXPRN_MOD_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_EM
            ),
LOGIC_EMT as ( SELECT 
		  upper( TRIM( EXPRN_MOD_TYP_NM ) )                  AS                                   EXPRN_MOD_TYP_NM 
		, upper( TRIM( EXPRN_MOD_TYP_CD ) )                  AS                                   EXPRN_MOD_TYP_CD 
		, upper( EXPRN_MOD_TYP_VOID_IND )                    AS                             EXPRN_MOD_TYP_VOID_IND 
		from SRC_EMT
            ),
LOGIC_DED as ( SELECT 
		  upper( TRIM( DED_TYP_CD ) )                        AS                                         DED_TYP_CD 
		, upper( TRIM( DED_CTG_TYP_CD ) )                    AS                                     DED_CTG_TYP_CD 
		, upper( TRIM( HZRD_GRP_TYP_CD ) )                   AS                                    HZRD_GRP_TYP_CD 
		, RT_ELEM_DED_TYP_AGG_AMT                            AS                            RT_ELEM_DED_TYP_AGG_AMT 
		, RT_ELEM_DED_TYP_RT                                 AS                                 RT_ELEM_DED_TYP_RT 
		, RT_ELEM_DED_TYP_ID                                 AS                                 RT_ELEM_DED_TYP_ID 
		, upper( RT_ELEM_DED_TYP_VOID_IND )                  AS                           RT_ELEM_DED_TYP_VOID_IND 
		from SRC_DED
            ),
LOGIC_DT as ( SELECT 
		  upper( DED_TYP_AMT )                               AS                                        DED_TYP_AMT 
		, upper( TRIM( DED_TYP_CD ) )                        AS                                         DED_TYP_CD 
		, upper( DED_TYP_VOID_IND )                          AS                                   DED_TYP_VOID_IND 
		from SRC_DT
            ),
LOGIC_DCT as ( SELECT 
		  upper( TRIM( DED_CTG_TYP_NM ) )                    AS                                     DED_CTG_TYP_NM 
		, upper( TRIM( DED_CTG_TYP_CD ) )                    AS                                     DED_CTG_TYP_CD 
		, upper( DED_CTG_TYP_VOID_IND )                      AS                               DED_CTG_TYP_VOID_IND 
		from SRC_DCT
            ),
LOGIC_HGT1 as ( SELECT 
		  upper( TRIM( HZRD_GRP_TYP_NM ) )                   AS                                    HZRD_GRP_TYP_NM 
		, upper( TRIM( HZRD_GRP_TYP_CD ) )                   AS                                    HZRD_GRP_TYP_CD 
		, upper( HZRD_GRP_TYP_VOID_IND )                     AS                              HZRD_GRP_TYP_VOID_IND 
		from SRC_HGT1
            ),
LOGIC_LDD as ( SELECT 
		  LDD_DED_AMT                                        AS                                        LDD_DED_AMT 
		, LDD_AGGR_AMT                                       AS                                       LDD_AGGR_AMT 
		, LDD_ID                                             AS                                             LDD_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_LDD
            ),
LOGIC_BLDD as ( SELECT 
		  upper( TRIM( HZRD_GRP_TYP_CD ) )                   AS                                    HZRD_GRP_TYP_CD 
		, LDD_ID                                             AS                                             LDD_ID 
		from SRC_BLDD
            ),
LOGIC_HGT2 as ( SELECT 
		  upper( TRIM( HZRD_GRP_TYP_NM ) )                   AS                                    HZRD_GRP_TYP_NM 
		, upper( TRIM( HZRD_GRP_TYP_CD ) )                   AS                                    HZRD_GRP_TYP_CD 
		, upper( HZRD_GRP_TYP_VOID_IND )                     AS                              HZRD_GRP_TYP_VOID_IND 
		from SRC_HGT2
            )

---- RENAME LAYER ----
,

RENAME_PPRE as ( SELECT 
		  PPRE_ID                                            as                                            PPRE_ID
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, PPRE_EFF_DT                                        as                                        PPRE_EFF_DT
		, PPRE_END_DT                                        as                                        PPRE_END_DT
		, RT_ELEM_LMCJ_TYP_ID                                as                                RT_ELEM_LMCJ_TYP_ID
		, PPRE_RT                                            as                                            PPRE_RT
		, WC_CLS_RT_TIER_TYP_CD                              as                              WC_CLS_RT_TIER_TYP_CD
		, EXPRN_MOD_ID                                       as                                       EXPRN_MOD_ID
		, PPRE_ARD_DT                                        as                                        PPRE_ARD_DT
		, PPRE_ARD_OVRRD_IND                                 as                                 PPRE_ARD_OVRRD_IND
		, PPRE_RN_IND                                        as                                        PPRE_RN_IND
		, PPRE_DVND_OVRD_AMT                                 as                                 PPRE_DVND_OVRD_AMT
		, PPRE_DVND_OVRD_PCT                                 as                                 PPRE_DVND_OVRD_PCT
		, PPRE_RTSP_TAX_MLTPLR                               as                               PPRE_RTSP_TAX_MLTPLR
		, PPRE_RTSP_EXCS_LOSS_PREM_FCTR                      as                      PPRE_RTSP_EXCS_LOSS_PREM_FCTR
		, RT_ELEM_DED_TYP_ID                                 as                                 RT_ELEM_DED_TYP_ID
		, LDD_ID                                             as                                             LDD_ID
		, PPRE_NOTE_IND                                      as                                      PPRE_NOTE_IND
		, PPRE_MNL_ADD_IND                                   as                                   PPRE_MNL_ADD_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_PPRE   ), 
RENAME_PP as ( SELECT 
		  PLCY_NO                                            as                                            PLCY_NO
		, PLCY_PRD_ID                                        as                                     PP_PLCY_PRD_ID 
	    , PP_VOID_IND                                        as                                        PP_VOID_IND

				FROM     LOGIC_PP   ), 
RENAME_RELT as ( SELECT 
		  RT_ELEM_TYP_CD                                     as                                     RT_ELEM_TYP_CD
		, RT_ELEM_DSPLY_TYP_CD                               as                               RT_ELEM_DSPLY_TYP_CD
		, RT_ELEM_LMCJ_TYP_DSPLY_IND                         as                         RT_ELEM_LMCJ_TYP_DSPLY_IND
		, RT_ELEM_LMCJ_TYP_WEB_DSPLY_IND                     as                     RT_ELEM_LMCJ_TYP_WEB_DSPLY_IND
		, RT_ELEM_LMCJ_TYP_USER_ENT_IND                      as                      RT_ELEM_LMCJ_TYP_USER_ENT_IND
		, RT_ELEM_LMCJ_TYP_PREM_CALC_IND                     as                     RT_ELEM_LMCJ_TYP_PREM_CALC_IND
		, RT_ELEM_LMCJ_TYP_PED_OVRRD_IND                     as                     RT_ELEM_LMCJ_TYP_PED_OVRRD_IND
		, RT_ELEM_LMCJ_TYP_ZRO_DSPLY_IND                     as                     RT_ELEM_LMCJ_TYP_ZRO_DSPLY_IND
		, RT_ELEM_LMCJ_TYP_USER_SEL_IND                      as                      RT_ELEM_LMCJ_TYP_USER_SEL_IND
		, RT_ELEM_LMCJ_TYP_ID                                as                           RELT_RT_ELEM_LMCJ_TYP_ID
		, RT_ELEM_LMCJ_TYP_VOID_IND                          as                          RT_ELEM_LMCJ_TYP_VOID_IND 
				FROM     LOGIC_RELT   ), 
RENAME_RENT as ( SELECT 
		  RT_ELEM_TYP_NM                                     as                                     RT_ELEM_TYP_NM
		, RT_ELEM_TYP_CD                                     as                                RENT_RT_ELEM_TYP_CD
		, VOID_IND                                           as                                      RENT_VOID_IND 
				FROM     LOGIC_RENT   ), 
RENAME_RET as ( SELECT 
		  RT_ELEM_USAGE_TYP_CD                               as                               RT_ELEM_USAGE_TYP_CD
		, RT_ELEM_TYP_CD                                     as                                 RET_RT_ELEM_TYP_CD
		, RT_ELEM_TYP_VOID_IND                               as                               RT_ELEM_TYP_VOID_IND 
				FROM     LOGIC_RET   ), 
RENAME_REUT as ( SELECT 
		  RT_ELEM_USAGE_TYP_DESC                             as                             RT_ELEM_USAGE_TYP_DESC
		, RT_ELEM_USAGE_TYP_CD                               as                          REUT_RT_ELEM_USAGE_TYP_CD
		, VOID_IND                                           as                                      REUT_VOID_IND 
				FROM     LOGIC_REUT   ), 
RENAME_DISP as ( SELECT 
		  RT_ELEM_DSPLY_TYP_NM                               as                               RT_ELEM_DSPLY_TYP_NM
		, RT_ELEM_DSPLY_TYP_CD                               as                          DISP_RT_ELEM_DSPLY_TYP_CD
		, RT_ELEM_DSPLY_TYP_VOID_IND                         as                         RT_ELEM_DSPLY_TYP_VOID_IND 
				FROM     LOGIC_DISP   ), 
RENAME_WCRTT as ( SELECT 
		  WC_CLS_RT_TIER_TYP_NM                              as                              WC_CLS_RT_TIER_TYP_NM
		, WC_CLS_RT_TIER_TYP_CD                              as                        WCRTT_WC_CLS_RT_TIER_TYP_CD
		, WC_CLS_RT_TIER_TYP_VOID_IND                        as                        WC_CLS_RT_TIER_TYP_VOID_IND 
				FROM     LOGIC_WCRTT   ), 
RENAME_EM as ( SELECT 
		  EXPRN_MOD_TYP_CD                                   as                                   EXPRN_MOD_TYP_CD
		, EXPRN_MOD_FCTR                                     as                                     EXPRN_MOD_FCTR
		, EXPRN_MOD_EFF_DT                                   as                                   EXPRN_MOD_EFF_DT
		, EXPRN_MOD_END_DT                                   as                                   EXPRN_MOD_END_DT
		, EXPRN_MOD_ANV_RT_DT                                as                                EXPRN_MOD_ANV_RT_DT
		, EXPRN_MOD_ID                                       as                                    EM_EXPRN_MOD_ID
		, VOID_IND                                           as                                        EM_VOID_IND 
				FROM     LOGIC_EM   ), 
RENAME_EMT as ( SELECT 
		  EXPRN_MOD_TYP_NM                                   as                                   EXPRN_MOD_TYP_NM
		, EXPRN_MOD_TYP_CD                                   as                               EMT_EXPRN_MOD_TYP_CD
		, EXPRN_MOD_TYP_VOID_IND                             as                             EXPRN_MOD_TYP_VOID_IND 
				FROM     LOGIC_EMT   ), 
RENAME_DED as ( SELECT 
		  DED_TYP_CD                                         as                                         DED_TYP_CD
		, DED_CTG_TYP_CD                                     as                                     DED_CTG_TYP_CD
		, HZRD_GRP_TYP_CD                                    as                                DED_HZRD_GRP_TYP_CD
		, RT_ELEM_DED_TYP_AGG_AMT                            as                            RT_ELEM_DED_TYP_AGG_AMT
		, RT_ELEM_DED_TYP_RT                                 as                                 RT_ELEM_DED_TYP_RT
		, RT_ELEM_DED_TYP_ID                                 as                             DED_RT_ELEM_DED_TYP_ID
		, RT_ELEM_DED_TYP_VOID_IND                           as                           RT_ELEM_DED_TYP_VOID_IND 
				FROM     LOGIC_DED   ), 
RENAME_DT as ( SELECT 
		  DED_TYP_AMT                                        as                                        DED_TYP_AMT
		, DED_TYP_CD                                         as                                      DT_DED_TYP_CD
		, DED_TYP_VOID_IND                                   as                                   DED_TYP_VOID_IND 
				FROM     LOGIC_DT   ), 
RENAME_DCT as ( SELECT 
		  DED_CTG_TYP_NM                                     as                                     DED_CTG_TYP_NM
		, DED_CTG_TYP_CD                                     as                                 DCT_DED_CTG_TYP_CD
		, DED_CTG_TYP_VOID_IND                               as                               DED_CTG_TYP_VOID_IND 
				FROM     LOGIC_DCT   ), 
RENAME_HGT1 as ( SELECT 
		  HZRD_GRP_TYP_NM                                    as                                DED_HZRD_GRP_TYP_NM
		, HZRD_GRP_TYP_CD                                    as                               HGT1_HZRD_GRP_TYP_CD
		, HZRD_GRP_TYP_VOID_IND                              as                         HGT1_HZRD_GRP_TYP_VOID_IND 
				FROM     LOGIC_HGT1   ), 
RENAME_LDD as ( SELECT 
		  LDD_DED_AMT                                        as                                        LDD_DED_AMT
		, LDD_AGGR_AMT                                       as                                       LDD_AGGR_AMT
		, LDD_ID                                             as                                         LDD_LDD_ID
		, VOID_IND                                           as                                       LDD_VOID_IND 
				FROM     LOGIC_LDD   ), 
RENAME_BLDD as ( SELECT 
		  HZRD_GRP_TYP_CD                                    as                                LDD_HZRD_GRP_TYP_CD
		, LDD_ID                                             as                                        BLDD_LDD_ID 
				FROM     LOGIC_BLDD   ), 
RENAME_HGT2 as ( SELECT 
		  HZRD_GRP_TYP_NM                                    as                                LDD_HZRD_GRP_TYP_NM
		, HZRD_GRP_TYP_CD                                    as                               HGT2_HZRD_GRP_TYP_CD
		, HZRD_GRP_TYP_VOID_IND                              as                         HGT2_HZRD_GRP_TYP_VOID_IND 
				FROM     LOGIC_HGT2   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PPRE                           as ( SELECT * from    RENAME_PPRE   ),
FILTER_PP                             as ( SELECT * from    RENAME_PP 
                 WHERE PP_VOID_IND = 'N'  ),
FILTER_RELT                           as ( SELECT * from    RENAME_RELT 
				WHERE RT_ELEM_LMCJ_TYP_VOID_IND = 'N'  ),
FILTER_RENT                           as ( SELECT * from    RENAME_RENT 
				WHERE RENT_VOID_IND = 'N'  ),
FILTER_RET                            as ( SELECT * from    RENAME_RET 
				WHERE RT_ELEM_TYP_VOID_IND = 'N'  ),
FILTER_DISP                           as ( SELECT * from    RENAME_DISP 
				WHERE RT_ELEM_DSPLY_TYP_VOID_IND = 'N'  ),
FILTER_WCRTT                          as ( SELECT * from    RENAME_WCRTT 
				WHERE WC_CLS_RT_TIER_TYP_VOID_IND = 'N'  ),
FILTER_DED                            as ( SELECT * from    RENAME_DED 
				WHERE RT_ELEM_DED_TYP_VOID_IND = 'N'  ),
FILTER_LDD                            as ( SELECT * from    RENAME_LDD 
				WHERE LDD_VOID_IND = 'N'  ),
FILTER_EM                             as ( SELECT * from    RENAME_EM 
				WHERE EM_VOID_IND = 'N'  ),
FILTER_EMT                            as ( SELECT * from    RENAME_EMT 
				WHERE EXPRN_MOD_TYP_VOID_IND = 'N'  ),
FILTER_REUT                           as ( SELECT * from    RENAME_REUT 
				WHERE REUT_VOID_IND = 'N'  ),
FILTER_DT                             as ( SELECT * from    RENAME_DT 
				WHERE DED_TYP_VOID_IND = 'N'  ),
FILTER_DCT                            as ( SELECT * from    RENAME_DCT 
				WHERE DED_CTG_TYP_VOID_IND = 'N'  ),
FILTER_BLDD                           as ( SELECT * from    RENAME_BLDD   ),
FILTER_HGT1                           as ( SELECT * from    RENAME_HGT1 
				WHERE HGT1_HZRD_GRP_TYP_VOID_IND = 'N'  ),
FILTER_HGT2                           as ( SELECT * from    RENAME_HGT2 
				WHERE HGT2_HZRD_GRP_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

BLDD as ( SELECT * 
				FROM  FILTER_BLDD
				LEFT JOIN FILTER_HGT2 ON  FILTER_BLDD.LDD_HZRD_GRP_TYP_CD =  FILTER_HGT2.HGT2_HZRD_GRP_TYP_CD  ),
DED as ( SELECT * 
				FROM  FILTER_DED
				LEFT JOIN FILTER_DT ON  FILTER_DED.DED_TYP_CD =  FILTER_DT.DT_DED_TYP_CD 
								LEFT JOIN FILTER_DCT ON  FILTER_DED.DED_CTG_TYP_CD =  FILTER_DCT.DCT_DED_CTG_TYP_CD 
								LEFT JOIN FILTER_HGT1 ON  FILTER_DED.DED_HZRD_GRP_TYP_CD =  FILTER_HGT1.HGT1_HZRD_GRP_TYP_CD  ),
RET as ( SELECT * 
				FROM  FILTER_RET
				LEFT JOIN FILTER_REUT ON  FILTER_RET.RT_ELEM_USAGE_TYP_CD =  FILTER_REUT.REUT_RT_ELEM_USAGE_TYP_CD  ),
EM as ( SELECT * 
				FROM  FILTER_EM
				LEFT JOIN FILTER_EMT ON  FILTER_EM.EXPRN_MOD_TYP_CD =  FILTER_EMT.EMT_EXPRN_MOD_TYP_CD  ),
RELT as ( SELECT * 
				FROM  FILTER_RELT
				LEFT JOIN FILTER_RENT ON  FILTER_RELT.RT_ELEM_TYP_CD =  FILTER_RENT.RENT_RT_ELEM_TYP_CD 
						LEFT JOIN RET ON  FILTER_RELT.RT_ELEM_TYP_CD = RET.RET_RT_ELEM_TYP_CD 
								LEFT JOIN FILTER_DISP ON  FILTER_RELT.RT_ELEM_DSPLY_TYP_CD =  FILTER_DISP.DISP_RT_ELEM_DSPLY_TYP_CD  ),
PPRE as ( SELECT * 
				FROM  FILTER_PPRE
				INNER JOIN FILTER_PP ON  FILTER_PPRE.PLCY_PRD_ID =  FILTER_PP.PP_PLCY_PRD_ID 
						LEFT JOIN RELT ON  FILTER_PPRE.RT_ELEM_LMCJ_TYP_ID = RELT.RELT_RT_ELEM_LMCJ_TYP_ID 
								LEFT JOIN FILTER_WCRTT ON  FILTER_PPRE.WC_CLS_RT_TIER_TYP_CD =  FILTER_WCRTT.WCRTT_WC_CLS_RT_TIER_TYP_CD 
						LEFT JOIN DED ON  FILTER_PPRE.RT_ELEM_DED_TYP_ID = DED.DED_RT_ELEM_DED_TYP_ID 
								LEFT JOIN FILTER_LDD ON  FILTER_PPRE.LDD_ID =  FILTER_LDD.LDD_LDD_ID 
						LEFT JOIN EM ON  FILTER_PPRE.EXPRN_MOD_ID = EM.EM_EXPRN_MOD_ID 
						LEFT JOIN BLDD ON  FILTER_PPRE.LDD_ID = BLDD.BLDD_LDD_ID  )
SELECT * 
from PPRE
      );
    