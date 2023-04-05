

---- SRC LAYER ----
WITH

SRC_CES            as ( SELECT *     FROM     {{ ref( 'STG_DRVD_CLAIM_EXAM_SCHEDULE_HISTORY' ) }} ),
SRC_C              as ( SELECT *     FROM     {{ ref( 'STG_CASES' ) }} ),
SRC_CE             as ( SELECT *     FROM     {{ ref( 'STG_CASE_EVENT' ) }} ),
SRC_OU             as ( SELECT *     FROM     {{ ref( 'STG_ORGANIZATIONAL_UNIT' ) }} ),
SRC_PRFL           as ( SELECT *     FROM     {{ ref( 'STG_CASE_PROFILE' ) }} ),
SRC_REL            as ( SELECT *     FROM     {{ ref( 'STG_CASE_RELATED' ) }} ),
SRC_REL_CS         as ( SELECT *     FROM     {{ ref( 'STG_CASES' ) }} ),
SRC_CDE            as ( SELECT *     FROM     {{ ref( 'STG_CASE_DETAIL_EXAM_SCHEDULE' ) }} ),
SRC_CP             as ( SELECT *     FROM     {{ ref( 'STG_CASE_PARTICIPATION' ) }} ),
SRC_CRI            as ( SELECT *     FROM     {{ ref( 'STG_CUSTOMER_ROLE_IDENTIFIER' ) }} ),
SRC_PRV            as ( SELECT *     FROM     {{ ref( 'DST_PROVIDER' ) }} ),
SRC_I              as ( SELECT *     FROM     {{ ref( 'STG_CASE_ISSUE' ) }} ),
SRC_Z              as ( SELECT *     FROM     {{ ref( 'STG_DEP_US_ZIPCODE_LIST' ) }} ),

/*
SRC_CES            as ( SELECT *     FROM     STAGING.STG_DRVD_CLAIM_EXAM_SCHEDULE_HISTORY ),
SRC_C              as ( SELECT *     FROM     STAGING.STG_CASES ),
SRC_CE             as ( SELECT *     FROM     STAGING.STG_CASE_EVENT ),
SRC_OU             as ( SELECT *     FROM     STAGING.STG_ORGANIZATIONAL_UNIT ),
SRC_PRFL           as ( SELECT *     FROM     STAGING.STG_CASE_PROFILE ),
SRC_REL            as ( SELECT *     FROM     STAGING.STG_CASE_RELATED ),
SRC_REL_CS         as ( SELECT *     FROM     STAGING.STG_CASES ),
SRC_CDE            as ( SELECT *     FROM     STAGING.STG_CASE_DETAIL_EXAM_SCHEDULE ),
SRC_CP             as ( SELECT *     FROM     STAGING.STG_CASE_PARTICIPATION ),
SRC_CRI            as ( SELECT *     FROM     STAGING.STG_CUSTOMER_ROLE_IDENTIFIER ),
SRC_PRV            as ( SELECT *     FROM     STAGING.DST_PROVIDER ),
SRC_I              as ( SELECT *     FROM     STAGING.STG_CASE_ISSUE ),
SRC_Z              as ( SELECT *     FROM     STAGING.STG_DEP_US_ZIPCODE_LIST ),

*/

---- LOGIC LAYER ----


, LOGIC_CES as ( 
	SELECT 	  
		 CASE_NO                                            as                                            CASE_NO	, 
		 GROUP_NO                                           as                                           GROUP_NO	, 
		 CASE_ID                                            as                                            CASE_ID	, 
		 CLM_NO                                             as                                             CLM_NO	, 
		 CASE_OWNER                                         as                                         CASE_OWNER	, 
		 CASE_OWNER_LGN_NM                                  as                                  CASE_OWNER_LGN_NM	, 
		 FNCT_ROLE_CD                                       as                                       FNCT_ROLE_CD	, 
		 CASE_OWNER_FUNCTIONAL_ROLE                         as                         CASE_OWNER_FUNCTIONAL_ROLE	, 
		 CASE_NM                                            as                                            CASE_NM	, 
		 CASE_EFFECTIVE_DATE                                as                                CASE_EFFECTIVE_DATE	, 
		 CASE_RSOL_TYP_CD                                   as                                   CASE_RSOL_TYP_CD	, 
		 CASE_RSOL_TYP_NM                                   as                                   CASE_RSOL_TYP_NM	, 
		 CASE_STT_TYP_CD                                    as                                    CASE_STT_TYP_CD	, 
		 CASE_STT_TYP_NM                                    as                                    CASE_STT_TYP_NM	, 
		 CASE_STS_TYP_CD                                    as                                    CASE_STS_TYP_CD	, 
		 CASE_STS_TYP_NM                                    as                                    CASE_STS_TYP_NM	, 
		 CASE_STS_RSN_TYP_CD                                as                                CASE_STS_RSN_TYP_CD	, 
		 CASE_STS_RSN_TYP_NM                                as                                CASE_STS_RSN_TYP_NM	, 
		 CLM_AGRE_ID                                        as                                        CLM_AGRE_ID	, 
		 EXAM_SCHDL_USER_LGN_NM                             as                             EXAM_SCHDL_USER_LGN_NM	, 
		 EXAM_SCHDL_DTM                                     as                                     EXAM_SCHDL_DTM	, 
		 EXAM_SCHDL_DATE                                    as                                    EXAM_SCHDL_DATE	, 
		 EXAM_RSCHDL_STS_RSN_CODE                           as                           EXAM_RSCHDL_STS_RSN_CODE	, 
		 EXAM_RSCHDL_STS_RSN_DESC                           as                           EXAM_RSCHDL_STS_RSN_DESC	, 
		 FNCT_ROLE_CD                                       as                                       FNCT_ROLE_CD	, 
		 CASE_STS_TYP_NM                                    as                                    CASE_STS_TYP_NM	, 
		 CASE_STS_RSN_TYP_NM                                as                                CASE_STS_RSN_TYP_NM	, 
		 NO_SHOW_COUNT                                      as                                      NO_SHOW_COUNT	, 
		 IF NO_SHOW_COUNT =1 THEN 'Y' ELSE 'N'              as                                        NO_SHOW_IND	, 
		 EXAM_RESCHDL_COUNT                                 as                                 EXAM_RESCHDL_COUNT	, 
		 EXAM_RESCHDL_IND                                   as                                   EXAM_RESCHDL_IND	, 
		 CASE_COMP_DT                                       as                                       CASE_COMP_DT	, 
		 CURRENT_IND                                        as                                        CURRENT_IND
	 from SRC_CES )

, LOGIC_C as ( 
	SELECT 	  
		 CASE_INT_DT                                        as                                        CASE_INT_DT	, 
		 CASE_DUE_DT                                        as                                        CASE_DUE_DT	, 
		 CASE_TYP_CD                                        as                                        CASE_TYP_CD	, 
		 CASE_TYP_NM                                        as                                        CASE_TYP_NM	, 
		 CASE_CTG_TYP_CD                                    as                                    CASE_CTG_TYP_CD	, 
		 CASE_CTG_TYP_NM                                    as                                    CASE_CTG_TYP_NM	, 
		 APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD	, 
		 APP_CNTX_TYP_NM                                    as                                    APP_CNTX_TYP_NM	, 
		 CASE_PRTY_TYP_CD                                   as                                   CASE_PRTY_TYP_CD	, 
		 CASE_PRTY_TYP_NM                                   as                                   CASE_PRTY_TYP_NM	, 
		 CASE_EXTRNL_NO                                     as                                     CASE_EXTRNL_NO
	 from SRC_C )

, LOGIC_CE as ( 
	SELECT 	  
		 AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM	, 
		 CASE_EVNT_COMP_DT                                  as                                  CASE_EVNT_COMP_DT	, 
		 CASE_EVNT_TYP_CD                                   as                                   CASE_EVNT_TYP_CD	, 
		 CASE_ID                                            as                                            CASE_ID	, 
		 ORG_UNT_ID                                         as                                         ORG_UNT_ID	, 
		 VOID_IND                                           as                                           VOID_IND
	 from SRC_CE )

, LOGIC_OU as ( 
	SELECT 	  
		 ORG_UNT_ID                                         as                                         ORG_UNT_ID	, 
		 ORG_UNT_NM                                         as                                         ORG_UNT_NM
	 from SRC_OU )

, LOGIC_PRFL as ( 
	SELECT 	  
		 CASE_PRFL_CTG_TYP_CD                               as                               CASE_PRFL_CTG_TYP_CD	, 
		 CASE_PRFL_CTG_TYP_NM                               as                               CASE_PRFL_CTG_TYP_NM	, 
		 CASE_ID                                            as                                            CASE_ID	, 
		 CASE_PRFL_SEQ_NO                                   as                                   CASE_PRFL_SEQ_NO	, 
		 VOID_IND                                           as                                           VOID_IND
	 from SRC_PRFL )

, LOGIC_REL as ( 
	SELECT 	  
		 AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM	, 
		 CASE_ID                                            as                                            CASE_ID	, 
		 CASE_RLT_CASE_ID                                   as                                   CASE_RLT_CASE_ID	, 
		 VOID_IND                                           as                                           VOID_IND
	 from SRC_REL )

, LOGIC_REL_CS as ( 
	SELECT 	  
		 CASE_NO                                            as                                            CASE_NO	, 
		 CASE_ID                                            as                                            CASE_ID	, 
		 VOID_IND                                           as                                           VOID_IND
	 from SRC_REL_CS )

, LOGIC_CDE as ( 
	SELECT 	  
		 CASE_ID                                            as                                            CASE_ID	, 
		 CD_ADNDM_REQS_TYP_CD                               as                               CD_ADNDM_REQS_TYP_CD	, 
		 CD_ADNDM_REQS_TYP_NM                               as                               CD_ADNDM_REQS_TYP_NM	, 
		 CD_EXM_REQS_TYP_CD                                 as                                 CD_EXM_REQS_TYP_CD	, 
		 CD_EXM_REQS_TYP_NM                                 as                                 CD_EXM_REQS_TYP_NM	, 
		 CDES_ADDTNL_TST_IND                                as                                CDES_ADDTNL_TST_IND	, 
		 CDES_ADNDM_REQS_IND                                as                                CDES_ADNDM_REQS_IND	, 
		 CDES_ADR_NO                                        as                                        CDES_ADR_NO	, 
		 CDES_CLMT_AVL_FRI_IND                              as                              CDES_CLMT_AVL_FRI_IND	, 
		 CDES_CLMT_AVL_MON_IND                              as                              CDES_CLMT_AVL_MON_IND	, 
		 CDES_CLMT_AVL_SAT_IND                              as                              CDES_CLMT_AVL_SAT_IND	, 
		 CDES_CLMT_AVL_SUN_IND                              as                              CDES_CLMT_AVL_SUN_IND	, 
		 CDES_CLMT_AVL_THU_IND                              as                              CDES_CLMT_AVL_THU_IND	, 
		 CDES_CLMT_AVL_TUE_IND                              as                              CDES_CLMT_AVL_TUE_IND	, 
		 CDES_CLMT_AVL_WED_IND                              as                              CDES_CLMT_AVL_WED_IND	, 
		 CDES_EXM_ADDR_CITY_NM                              as                              CDES_EXM_ADDR_CITY_NM	, 
		 CDES_EXM_ADDR_CNTRY_NM                             as                             CDES_EXM_ADDR_CNTRY_NM	, 
		 CDES_EXM_ADDR_CNTY_NM                              as                              CDES_EXM_ADDR_CNTY_NM	, 
		 CDES_EXM_ADDR_POST_CD                              as                              CDES_EXM_ADDR_POST_CD	, 
		 CDES_EXM_ADDR_STR_1                                as                                CDES_EXM_ADDR_STR_1	, 
		 CDES_EXM_ADDR_STR_2                                as                                CDES_EXM_ADDR_STR_2	, 
		 CDES_EXM_ADDR_STT_NM                               as                               CDES_EXM_ADDR_STT_NM	, 
		 CDES_EXM_DT                                        as                                        CDES_EXM_DT	, 
		 CDES_EXM_RPT_RECV_DT                               as                               CDES_EXM_RPT_RECV_DT	, 
		 CDES_GRTT_45_IND                                   as                                   CDES_GRTT_45_IND	, 
		 CDES_ITPRT_NEED_IND                                as                                CDES_ITPRT_NEED_IND	, 
		 CDES_RSLT_SUSPD_IND                                as                                CDES_RSLT_SUSPD_IND	, 
		 CDES_SPL_REQD                                      as                                      CDES_SPL_REQD	, 
		 CDES_TRVL_REMB_IND                                 as                                 CDES_TRVL_REMB_IND	, 
		 CPS_TYP_CD                                         as                                         CPS_TYP_CD	, 
		 CPS_TYP_CD_SCND                                    as                                    CPS_TYP_CD_SCND	, 
		 CPS_TYP_NM                                         as                                         CPS_TYP_NM	, 
		 CPS_TYP_NM_SCND                                    as                                    CPS_TYP_NM_SCND	, 
		 LANG_TYP_CD                                        as                                        LANG_TYP_CD	, 
		 LANG_TYP_NM                                        as                                        LANG_TYP_NM	, 
		 VOID_IND                                           as                                           VOID_IND
	 from SRC_CDE )

, LOGIC_CP as ( 
	SELECT 	  
		 CASE_ID                                            as                                            CASE_ID	, 
		 CASE_PTCP_CITY                                     as                                     CASE_PTCP_CITY	, 
		 CASE_PTCP_CNTRY                                    as                                    CASE_PTCP_CNTRY	, 
		 CASE_PTCP_CNTY                                     as                                     CASE_PTCP_CNTY	, 
		 CASE_PTCP_END_DT                                   as                                   CASE_PTCP_END_DT	, 
		 CASE_PTCP_PRI_IND                                  as                                  CASE_PTCP_PRI_IND	, 
		 CASE_PTCP_STR_1                                    as                                    CASE_PTCP_STR_1	, 
		 CASE_PTCP_STR_2                                    as                                    CASE_PTCP_STR_2	, 
		 CASE_PTCP_STT                                      as                                      CASE_PTCP_STT	, 
		 CASE_PTCP_TYP_CD                                   as                                   CASE_PTCP_TYP_CD	, 
		 CASE_PTCP_ZIP                                      as                                      CASE_PTCP_ZIP	, 
		 CUST_ID                                            as                                            CUST_ID	, 
		 VOID_IND                                           as                                           VOID_IND
	 from SRC_CP )

, LOGIC_CRI as ( 
	SELECT 	  
		 CRI_VOID_IND                                       as                                       CRI_VOID_IND	, 
		 CUST_ID                                            as                                            CUST_ID	, 
		 CUST_ROLE_ID_END_DT                                as                                CUST_ROLE_ID_END_DT	, 
		 CUST_ROLE_ID_VAL_STR                               as                               CUST_ROLE_ID_VAL_STR	, 
		 ID_TYP_CD                                          as                                          ID_TYP_CD
	 from SRC_CRI )

, LOGIC_PRV as ( 
	SELECT 	  
		 NPI_NMBR                                           as                                           NPI_NMBR	, 
		 PEACH_NUMBER                                       as                                       PEACH_NUMBER
	 from SRC_PRV )

, LOGIC_I as ( 
	SELECT 	  
		 CASE_ISS_TYP_CD                                    as                                    CASE_ISS_TYP_CD	, 
		 CASE_ISS_TYP_NM                                    as                                    CASE_ISS_TYP_NM	, 
		 CASE_ID                                            as                                            CASE_ID	, 
		 CASE_ISS_ID                                        as                                        CASE_ISS_ID	, 
		 CASE_ISS_PRI_IND                                   as                                   CASE_ISS_PRI_IND	, 
		 VOID_IND                                           as                                           VOID_IND
	 from SRC_I )

, LOGIC_Z as ( 
	SELECT 	  
		 LONGITUDE                                          as                                          LONGITUDE	, 
		 LATITUDE                                           as                                           LATITUDE	, 
		 ZIPCODE                                            as                                            ZIPCODE
	 from SRC_Z )


---- RENAME LAYER ----


, RENAME_CES as ( SELECT  
		 CASE_NO                                            as                                            CASE_NO , 
		 GROUP_NO                                           as                                    SCHEDULE_NUMBER , 
		 CASE_ID                                            as                                            CASE_ID , 
		 CLM_NO                                             as                                             CLM_NO , 
		 CASE_OWNER                                         as                                         CASE_OWNER , 
		 CASE_OWNER_LGN_NM                                  as                                  CASE_OWNER_LGN_NM , 
		 FNCT_ROLE_CD                                       as                      CASE_OWNER_FUNCTIONAL_ROLE_CD , 
		 CASE_OWNER_FUNCTIONAL_ROLE                         as                         CASE_OWNER_FUNCTIONAL_ROLE , 
		 CASE_NM                                            as                                            CASE_NM , 
		 CASE_EFFECTIVE_DATE                                as                                CASE_EFFECTIVE_DATE , 
		 CASE_RSOL_TYP_CD                                   as                                   CASE_RSOL_TYP_CD , 
		 CASE_RSOL_TYP_NM                                   as                                   CASE_RSOL_TYP_NM , 
		 CASE_STT_TYP_CD                                    as                                    CASE_STT_TYP_CD , 
		 CASE_STT_TYP_NM                                    as                                    CASE_STT_TYP_NM , 
		 CASE_STS_TYP_CD                                    as                                    CASE_STS_TYP_CD , 
		 CASE_STS_TYP_NM                                    as                                    CASE_STS_TYP_NM , 
		 CASE_STS_RSN_TYP_CD                                as                                CASE_STS_RSN_TYP_CD , 
		 CASE_STS_RSN_TYP_NM                                as                                CASE_STS_RSN_TYP_NM , 
		 CLM_AGRE_ID                                        as                                        CLM_AGRE_ID , 
		 EXAM_SCHDL_USER_LGN_NM                             as                             EXAM_SCHDL_USER_LGN_NM , 
		 EXAM_SCHDL_DTM                                     as                                     EXAM_SCHDL_DTM , 
		 EXAM_SCHDL_DATE                                    as                                    EXAM_SCHDL_DATE , 
		 EXAM_RSCHDL_STS_RSN_CODE                           as                           EXAM_RSCHDL_STS_RSN_CODE , 
		 EXAM_RSCHDL_STS_RSN_DESC                           as                           EXAM_RSCHDL_STS_RSN_DESC , 
		 FNCT_ROLE_CD                                       as                                       FNCT_ROLE_CD , 
		 CASE_STS_TYP_NM                                    as                             CRNT_CASE_STS_TYP_DESC , 
		 CASE_STS_RSN_TYP_NM                                as                         CRNT_CASE_STS_RSN_TYP_DESC , 
		 NO_SHOW_COUNT                                      as                                      NO_SHOW_COUNT , 
		 NO_SHOW_IND                                        as                                        NO_SHOW_IND , 
		 EXAM_RESCHDL_COUNT                                 as                                 EXAM_RESCHDL_COUNT , 
		 EXAM_RESCHDL_IND                                   as                                   EXAM_RESCHDL_IND , 
		 CASE_COMP_DT                                       as                                       CASE_COMP_DT , 
		 CURRENT_IND                                        as                                        CURRENT_IND 
		FROM LOGIC_CES
            )

, RENAME_C as ( SELECT  
		 CASE_INT_DT                                        as                                CASE_INITIATED_DATE , 
		 CASE_DUE_DT                                        as                                      CASE_DUE_DATE , 
		 CASE_TYP_CD                                        as                                        CASE_TYP_CD , 
		 CASE_TYP_NM                                        as                                        CASE_TYP_NM , 
		 CASE_CTG_TYP_CD                                    as                                    CASE_CTG_TYP_CD , 
		 CASE_CTG_TYP_NM                                    as                                    CASE_CTG_TYP_NM , 
		 APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD , 
		 APP_CNTX_TYP_NM                                    as                                    APP_CNTX_TYP_NM , 
		 CASE_PRTY_TYP_CD                                   as                                   CASE_PRTY_TYP_CD , 
		 CASE_PRTY_TYP_NM                                   as                                   CASE_PRTY_TYP_NM , 
		 CASE_EXTRNL_NO                                     as                                     CASE_EXTRNL_NO 
		FROM LOGIC_C
            )

, RENAME_I as ( SELECT  
		 CASE_ISS_TYP_CD                                    as                                       EXAM_TYPE_CD , 
		 CASE_ISS_TYP_NM                                    as                                     EXAM_TYPE_DESC , 
		 CASE_ID                                            as                                          I_CASE_ID , 
		 CASE_ISS_ID                                        as                                        CASE_ISS_ID , 
		 CASE_ISS_PRI_IND                                   as                                   CASE_ISS_PRI_IND , 
		 VOID_IND                                           as                                         I_VOID_IND 
		FROM LOGIC_I
            )

, RENAME_PRFL as ( SELECT  
		 CASE_PRFL_CTG_TYP_CD                               as                               CASE_PRFL_CTG_TYP_CD , 
		 CASE_PRFL_CTG_TYP_NM                               as                               CASE_PRFL_CTG_TYP_NM , 
		 CASE_ID                                            as                                       PRFL_CASE_ID , 
		 CASE_PRFL_SEQ_NO                                   as                                   CASE_PRFL_SEQ_NO , 
		 VOID_IND                                           as                                      PRFL_VOID_IND 
		FROM LOGIC_PRFL
            )

, RENAME_REL_CS as ( SELECT  
		 CASE_NO                                            as                                    RELATED_CASE_NO , 
		 CASE_ID                                            as                                     REL_CS_CASE_ID , 
		 VOID_IND                                           as                                    REL_CS_VOID_IND 
		FROM LOGIC_REL_CS
            )

, RENAME_Z as ( SELECT  
		 LONGITUDE                                          as                                 ZIP_CODE_LONGITUDE , 
		 LATITUDE                                           as                                  ZIP_CODE_LATITUDE , 
		 ZIPCODE                                            as                                            ZIPCODE 
		FROM LOGIC_Z
            )

, RENAME_CDE as ( SELECT  
		 CASE_ID                                            as                                        CDE_CASE_ID , 
		 CD_ADNDM_REQS_TYP_CD                               as                               CD_ADNDM_REQS_TYP_CD , 
		 CD_ADNDM_REQS_TYP_NM                               as                               CD_ADNDM_REQS_TYP_NM , 
		 CD_EXM_REQS_TYP_CD                                 as                                 CD_EXM_REQS_TYP_CD , 
		 CD_EXM_REQS_TYP_NM                                 as                                 CD_EXM_REQS_TYP_NM , 
		 CDES_ADDTNL_TST_IND                                as                            CDE_CDES_ADDTNL_TST_IND , 
		 CDES_ADNDM_REQS_IND                                as                            CDE_CDES_ADNDM_REQS_IND , 
		 CDES_ADR_NO                                        as                                    CDE_CDES_ADR_NO , 
		 CDES_CLMT_AVL_FRI_IND                              as                          CDE_CDES_CLMT_AVL_FRI_IND , 
		 CDES_CLMT_AVL_MON_IND                              as                          CDE_CDES_CLMT_AVL_MON_IND , 
		 CDES_CLMT_AVL_SAT_IND                              as                          CDE_CDES_CLMT_AVL_SAT_IND , 
		 CDES_CLMT_AVL_SUN_IND                              as                          CDE_CDES_CLMT_AVL_SUN_IND , 
		 CDES_CLMT_AVL_THU_IND                              as                          CDE_CDES_CLMT_AVL_THU_IND , 
		 CDES_CLMT_AVL_TUE_IND                              as                          CDE_CDES_CLMT_AVL_TUE_IND , 
		 CDES_CLMT_AVL_WED_IND                              as                          CDE_CDES_CLMT_AVL_WED_IND , 
		 CDES_EXM_ADDR_CITY_NM                              as                              CDES_EXM_ADDR_CITY_NM , 
		 CDES_EXM_ADDR_CNTRY_NM                             as                             CDES_EXM_ADDR_CNTRY_NM , 
		 CDES_EXM_ADDR_CNTY_NM                              as                              CDES_EXM_ADDR_CNTY_NM , 
		 CDES_EXM_ADDR_POST_CD                              as                              CDES_EXM_ADDR_POST_CD , 
		 CDES_EXM_ADDR_STR_1                                as                                CDES_EXM_ADDR_STR_1 , 
		 CDES_EXM_ADDR_STR_2                                as                                CDES_EXM_ADDR_STR_2 , 
		 CDES_EXM_ADDR_STT_NM                               as                               CDES_EXM_ADDR_STT_NM , 
		 CDES_EXM_DT                                        as                                        CDES_EXM_DT , 
		 CDES_EXM_RPT_RECV_DT                               as                               CDES_EXM_RPT_RECV_DT , 
		 CDES_GRTT_45_IND                                   as                               CDE_CDES_GRTT_45_IND , 
		 CDES_ITPRT_NEED_IND                                as                            CDE_CDES_ITPRT_NEED_IND , 
		 CDES_RSLT_SUSPD_IND                                as                            CDE_CDES_RSLT_SUSPD_IND , 
		 CDES_SPL_REQD                                      as                                  CDE_CDES_SPL_REQD , 
		 CDES_TRVL_REMB_IND                                 as                             CDE_CDES_TRVL_REMB_IND , 
		 CPS_TYP_CD                                         as                                         CPS_TYP_CD , 
		 CPS_TYP_CD_SCND                                    as                                    CPS_TYP_CD_SCND , 
		 CPS_TYP_NM                                         as                                         CPS_TYP_NM , 
		 CPS_TYP_NM_SCND                                    as                                    CPS_TYP_NM_SCND , 
		 LANG_TYP_CD                                        as                                    CDE_LANG_TYP_CD , 
		 LANG_TYP_NM                                        as                                    CDE_LANG_TYP_NM , 
		 VOID_IND                                           as                                       CDE_VOID_IND 
		FROM LOGIC_CDE
            )

, RENAME_CE as ( SELECT  
		 AUDIT_USER_CREA_DTM                                as                             CE_AUDIT_USER_CREA_DTM , 
		 CASE_EVNT_COMP_DT                                  as                                  CASE_EVNT_COMP_DT , 
		 CASE_EVNT_TYP_CD                                   as                                   CASE_EVNT_TYP_CD , 
		 CASE_ID                                            as                                         CE_CASE_ID , 
		 ORG_UNT_ID                                         as                                         ORG_UNT_ID , 
		 VOID_IND                                           as                                        CE_VOID_IND 
		FROM LOGIC_CE
            )

, RENAME_CP as ( SELECT  
		 CASE_ID                                            as                                         CP_CASE_ID , 
		 CASE_PTCP_CITY                                     as                                     CASE_PTCP_CITY , 
		 CASE_PTCP_CNTRY                                    as                                    CASE_PTCP_CNTRY , 
		 CASE_PTCP_CNTY                                     as                                     CASE_PTCP_CNTY , 
		 CASE_PTCP_END_DT                                   as                                CP_CASE_PTCP_END_DT , 
		 CASE_PTCP_PRI_IND                                  as                               CP_CASE_PTCP_PRI_IND , 
		 CASE_PTCP_STR_1                                    as                                    CASE_PTCP_STR_1 , 
		 CASE_PTCP_STR_2                                    as                                    CASE_PTCP_STR_2 , 
		 CASE_PTCP_STT                                      as                                      CASE_PTCP_STT , 
		 CASE_PTCP_TYP_CD                                   as                                CP_CASE_PTCP_TYP_CD , 
		 CASE_PTCP_ZIP                                      as                                      CASE_PTCP_ZIP , 
		 CUST_ID                                            as                                         CP_CUST_ID , 
		 VOID_IND                                           as                                        CP_VOID_IND 
		FROM LOGIC_CP
            )

, RENAME_CRI as ( SELECT  
		 CRI_VOID_IND                                       as                                       CRI_VOID_IND , 
		 CUST_ID                                            as                                        CRI_CUST_ID , 
		 CUST_ROLE_ID_END_DT                                as                                CUST_ROLE_ID_END_DT , 
		 CUST_ROLE_ID_VAL_STR                               as                               CUST_ROLE_ID_VAL_STR , 
		 ID_TYP_CD                                          as                                          ID_TYP_CD 
		FROM LOGIC_CRI
            )

, RENAME_OU as ( SELECT  
		 ORG_UNT_ID                                         as                                      OU_ORG_UNT_ID , 
		 ORG_UNT_NM                                         as                                      OU_ORG_UNT_NM 
		FROM LOGIC_OU
            )

, RENAME_PRV as ( SELECT  
		 NPI_NMBR                                           as                                       PRV_NPI_NMBR , 
		 PEACH_NUMBER                                       as                                   PRV_PEACH_NUMBER 
		FROM LOGIC_PRV
            )

, RENAME_REL as ( SELECT  
		 AUDIT_USER_CREA_DTM                                as                            REL_AUDIT_USER_CREA_DTM , 
		 CASE_ID                                            as                                        REL_CASE_ID , 
		 CASE_RLT_CASE_ID                                   as                                   CASE_RLT_CASE_ID , 
		 VOID_IND                                           as                                       REL_VOID_IND 
		FROM LOGIC_REL
            )

---- FILTER LAYER ----

FILTER_CES                            as ( SELECT * FROM    RENAME_CES  None  ),
FILTER_C                              as ( SELECT * FROM    RENAME_C  None  ),
FILTER_CE                             as ( SELECT * FROM    RENAME_CE 
                                            WHERE CE_VOID_IND = 'N' AND CASE_EVNT_TYP_CD IN ('RTM','FT_EXAM_SCED', 'FT_EXAM_SCED_STAT_OD') 
                                          /* Table Alias: CE Extract one latest row of (CASE_EVNT_COMP_DT, CE_AUDIT_USER_CREA_DTM) for a CE_CASE_ID. */
  ),
FILTER_OU                             as ( SELECT * FROM    RENAME_OU  None  ),
FILTER_PRFL                           as ( SELECT * FROM    RENAME_PRFL 
                                            WHERE PRFL_VOID_IND = 'N' None  ),
FILTER_REL                            as ( SELECT * FROM    RENAME_REL 
                                            WHERE REL_VOID_IND = 'N' None  ),
FILTER_REL_CS                         as ( SELECT * FROM    RENAME_REL_CS 
                                            WHERE REL_CS_VOID_IND = 'N' None  ),
FILTER_CDE                            as ( SELECT * FROM    RENAME_CDE 
                                            WHERE CDE_VOID_IND = 'N' None  ),
FILTER_CP                             as ( SELECT * FROM    RENAME_CP 
                                            WHERE CP_VOID_IND = 'N' AND CP_CASE_PTCP_TYP_CD = 'EXAM_PHYS' AND CP_CASE_PTCP_END_DT IS NULL  AND CP_CASE_PTCP_PRI_IND = 'Y' None  ),
FILTER_CRI                            as ( SELECT * FROM    RENAME_CRI 
                                            WHERE CRI_VOID_IND = 'N' AND ID_TYP_CD = 'NPID' AND CUST_ROLE_ID_END_DT IS NULL None  ),
FILTER_PRV                            as ( SELECT * FROM    RENAME_PRV  None  ),
FILTER_I                              as ( SELECT * FROM    RENAME_I 
                                            WHERE I_VOID_IND = 'N' AND CASE_ISS_PRI_IND = 'Y' None  ),
FILTER_Z                              as ( SELECT * FROM    RENAME_Z  
                                          /* update the Join predicate to left(EXAM_LCTN_ZIP, 5)   */
  )

---- JOIN LAYER ----

left(EXAM_LCTN_ZIP, 5) as ( SELECT * 
				FROM  FILTER_left(EXAM_LCTN_ZIP, 5)
				LEFT JOIN FILTER_Z ON  FILTER_left(EXAM_LCTN_ZIP, 5) =  FILTER_Z.ZIPCODE  ),
CES as ( SELECT * 
				FROM  FILTER_CES
				LEFT JOIN FILTER_I ON  FILTER_CES.CASE_ID =  FILTER_I_CASE_ID  ),
CRI as ( SELECT * 
				FROM  FILTER_CRI
				LEFT JOIN FILTER_PRV ON  FILTER_CRI.CUST_ROLE_ID_VAL_STR =  FILTER_PRV_NPI_NMBR  ),
CP as ( SELECT * 
				FROM  FILTER_CP
				LEFT JOIN CRI ON  FILTER_CP.CP_CUST_ID = CRI_CUST_ID  ),
CDE_CASE_ID as ( SELECT * 
				FROM  FILTER_CDE_CASE_ID
				LEFT JOIN CP ON  FILTER_CDE_CASE_ID.CASE_ID = CP_CASE_ID  ),
REL as ( SELECT * 
				FROM  FILTER_REL
				LEFT JOIN FILTER_REL_CS ON  FILTER_REL.CASE_RLT_CASE_ID =  FILTER_REL_CS_CASE_ID  ),
CE as ( SELECT * 
				FROM  FILTER_CE
				LEFT JOIN FILTER_OU ON  FILTER_CE.ORG_UNT_ID =  FILTER_OU_ORG_UNT_ID  ),
CES as ( SELECT * 
				FROM  FILTER_CES
				INNER JOIN FILTER_C ON  FILTER_CES.CASE_ID =  FILTER_C_CASE_ID 
						LEFT JOIN CE ON  FILTER_CES.CASE_ID = CE_CASE_ID AND CES.CURRENT_IND = 'Y' 
								LEFT JOIN FILTER_PRFL ON  FILTER_CES.CASE_ID =  FILTER_PRFL_CASE_ID 
						LEFT JOIN REL ON  FILTER_CES.CASE_ID = REL_CASE_ID 
								LEFT JOIN FILTER_CDE ON  FILTER_CES.CASE_ID =  FILTER_CDE_CASE_ID AND CES.CURRENT_IND = 'Y'  )
SELECT 
		  CASE_NO
		, SCHEDULE_NUMBER
		, CASE_ID
		, CLM_NO
		, CASE_OWNER
		, CASE_OWNER_LGN_NM
		, CASE_OWNER_FUNCTIONAL_ROLE_CD
		, CASE_OWNER_FUNCTIONAL_ROLE
		, CASE_NM
		, CASE_INITIATED_DATE
		, CASE_EFFECTIVE_DATE
		, CASE_DUE_DATE
		, CASE_TYP_CD
		, CASE_TYP_NM
		, CASE_CTG_TYP_CD
		, CASE_CTG_TYP_NM
		, APP_CNTX_TYP_CD
		, APP_CNTX_TYP_NM
		, CASE_PRTY_TYP_CD
		, CASE_PRTY_TYP_NM
		, CASE_RSOL_TYP_CD
		, CASE_RSOL_TYP_NM
		, EXAM_TYPE_CD
		, EXAM_TYPE_DESC
		, CASE_PRFL_CTG_TYP_CD
		, CASE_PRFL_CTG_TYP_NM
		, CASE_STT_TYP_CD
		, CASE_STT_TYP_NM
		, CASE_STS_TYP_CD
		, CASE_STS_TYP_NM
		, CASE_STS_RSN_TYP_CD
		, CASE_STS_RSN_TYP_NM
		, 
		 COALESCE (CDE.CD_EXM_REQS_TYP_CD, CES.EXAM_REQUESTOR_TYP_CD, 'UNK')
                                                             as                              EXAM_REQUESTOR_TYP_CD
		, 
		 COALESCE (CDE.CD_EXM_REQS_TYP_NM, CES.EXAM_REQUESTOR_TYP_DESC, 'UNKNOWN')
                                                             as                            EXAM_REQUESTOR_TYP_DESC
		, 
		 COALESCE (CDE.CPS_TYP_CD, CES.RQSTD_PHYSC_SPCLT_TYP_CODE, 'NONE')
                                                             as                         RQSTD_PHYSC_SPCLT_TYP_CODE
		, 
		 COALESCE (CDE.CPS_TYP_NM, CES.RQSTD_PHYSC_SPCLT_TYP_DESC, 'NONE')
                                                             as                         RQSTD_PHYSC_SPCLT_TYP_DESC
		, 
		 COALESCE (CDE.CPS_TYP_CD_SCND, CES.SCNDR_RQSTD_PHYSC_SPCLT_TYP_CODE)
                                                             as                   SCNDR_RQSTD_PHYSC_SPCLT_TYP_CODE
		, 
		 COALESCE (CDE.CPS_TYP_NM_SCND, CES.SCNDR_RQSTD_PHYSC_SPCLT_TYP_DESC)
                                                             as                   SCNDR_RQSTD_PHYSC_SPCLT_TYP_DESC
		, 
		 COALESCE (CDE.CD_ADNDM_REQS_TYP_CD, CES.ADDENDUM_REQUEST_TYPE_CODE)
                                                             as                         ADDENDUM_REQUEST_TYPE_CODE
		, 
		 COALESCE (CDE.CD_ADNDM_REQS_TYP_NM, CES.ADDENDUM_REQUEST_TYPE_DESC)
                                                             as                         ADDENDUM_REQUEST_TYPE_DESC
		, 
		 COALESCE (CDE.CDE_LANG_TYP_CD, CES.LANG_TYP_CD)    as                                        LANG_TYP_CD
		, 
		 COALESCE (CDE.CDE_LANG_TYP_NM, CES.LANG_TYP_NM)    as                                        LANG_TYP_NM
		, CASE_EXTRNL_NO
		, RELATED_CASE_NO
		, 
		 COALESCE (CDE.CDE_CDES_ADR_NO, CES.CDES_ADR_NO)    as                                        CDES_ADR_NO
		, CLM_AGRE_ID
		, 
		 COALESCE (CE.CASE_EVNT_TYP_NM, CES.REFERRED_TO, 'UNKNOWN')
                                                             as                                        REFERRED_TO
		, 
		 COALESCE (CE.CASE_EVNT_FST_RMND_DT, CE.CASE_EVNT_DUE_DT, CES.REFERRAL_DATE ).

CAST AS DATE
                                                             as                                      REFERRAL_DATE
		, 
		 COALESCE (CE.AUDIT_USER_CREA_DTM::DATE, CES.REFERRAL_CREATE_DATE)
                                                             as                               REFERRAL_CREATE_DATE
		, 
		 COALESCE (CE.CASE_EVNT_DUE_DT, CES.DUE_DATE)       as                                           DUE_DATE
		, EXAM_SCHDL_USER_LGN_NM
		, EXAM_SCHDL_DTM
		, EXAM_SCHDL_DATE
		, 
		 COALESCE (CDE.CDES_EXM_DT, CES.EXAM_DATE)          as                                          EXAM_DATE
		, 
		 COALESCE (CDE.CDES_EXM_RPT_RECV_DT, CES.EXAM_REPORT_RECV_DATE)
                                                             as                              EXAM_REPORT_RECV_DATE
		, EXAM_RSCHDL_STS_RSN_CODE
		, EXAM_RSCHDL_STS_RSN_DESC
		, 
		 COALESCE (OU.OU_ORG_UNT_NM, CES.ORG_UNT_NM)        as                                         ORG_UNT_NM
		, FNCT_ROLE_CD
		, 
		 COALESCE (CDE_CDES_CLMT_AVL_SUN_IND, CES.CDES_CLMT_AVL_SUN_IND)
                                                             as                              CDES_CLMT_AVL_SUN_IND
		, 
		 COALESCE (CDE_CDES_CLMT_AVL_MON_IND, CES.CDES_CLMT_AVL_MON_IND)
                                                             as                              CDES_CLMT_AVL_MON_IND
		, 
		 COALESCE (CDE_CDES_CLMT_AVL_TUE_IND, CES.CDES_CLMT_AVL_TUE_IND)
                                                             as                              CDES_CLMT_AVL_TUE_IND
		, 
		 COALESCE (CDE_CDES_CLMT_AVL_WED_IND, CES.CDES_CLMT_AVL_WED_IND)
                                                             as                              CDES_CLMT_AVL_WED_IND
		, 
		 COALESCE (CDE_CDES_CLMT_AVL_THU_IND, CES.CDES_CLMT_AVL_THU_IND)
                                                             as                              CDES_CLMT_AVL_THU_IND
		, 
		 COALESCE (CDE_CDES_CLMT_AVL_FRI_IND, CES.CDES_CLMT_AVL_FRI_IND)
                                                             as                              CDES_CLMT_AVL_FRI_IND
		, 
		 COALESCE (CDE_CDES_CLMT_AVL_SAT_IND, CES.CDES_CLMT_AVL_SAT_IND)
                                                             as                              CDES_CLMT_AVL_SAT_IND
		, 
		 COALESCE (CDE_CDES_ITPRT_NEED_IND, CES.CDES_ITPRT_NEED_IND)
                                                             as                                CDES_ITPRT_NEED_IND
		, 
		 COALESCE (CDE_CDES_GRTT_45_IND, CES.CDES_GRTT_45_IND)
                                                             as                                   CDES_GRTT_45_IND
		, 
		 COALESCE (CDE_CDES_TRVL_REMB_IND, CES.CDES_TRVL_REMB_IND)
                                                             as                                 CDES_TRVL_REMB_IND
		, 
		 COALESCE (CDE_CDES_ADDTNL_TST_IND, CES.CDES_ADDTNL_TST_IND)
                                                             as                                CDES_ADDTNL_TST_IND
		, 
		 COALESCE (CDE_CDES_ADNDM_REQS_IND, CES.CDES_ADNDM_REQS_IND)
                                                             as                                CDES_ADNDM_REQS_IND
		, 
		 COALESCE (CDE_CDES_RSLT_SUSPD_IND, CES.CDES_RSLT_SUSPD_IND)
                                                             as                                CDES_RSLT_SUSPD_IND
		, 
		 COALESCE (CDE.CDES_SPL_REQD, CES.INJURED_WORK_EXAM_AVAILABILITY_NOTE)
                                                             as                INJURED_WORK_EXAM_AVAILABILITY_NOTE
		, CRNT_CASE_STS_TYP_DESC
		, CRNT_CASE_STS_RSN_TYP_DESC
		, 
		 NOTE:
DATA EXTRACTION RULE
TABLE ALIAS: PRV
EXTRACT THE MIN OF PRV_PEACH_NUMBER FOR A PRV_NPI_NMBR.

NVL(PRV.PRV_PEACH_NUMBER, CES.PRVDR_PEACH_NUMBER)
                                                             as                                 PRVDR_PEACH_NUMBER
		, 
		 COALESCE(CDE.CDES_EXM_ADDR_STR_1,CP.CASE_PTCP_STR_1, CES.EXAM_LOC_STR1)
                                                             as                                      EXAM_LOC_STR1
		, 
		 COALESCE(CDE.CDES_EXM_ADDR_STR_2,CP.CASE_PTCP_STR_2, CES.EXAM_LOC_STR2)
                                                             as                                      EXAM_LOC_STR2
		, 
		 COALESCE(CDE.CDES_EXM_ADDR_CITY_NM,CP.CASE_PTCP_CITY, CES.EXAM_LCTN_CITY)
                                                             as                                     EXAM_LCTN_CITY
		, 
		 COALESCE(CDE.CDES_EXM_ADDR_STT_NM,CP.CASE_PTCP_STT, CES.EXAM_LCTN_STATE_NAME)
                                                             as                               EXAM_LCTN_STATE_NAME
		, 
		 COALESCE(CDE.CDES_EXM_ADDR_POST_CD,CP.CASE_PTCP_ZIP, CES.EXAM_LCTN_ZIP)
                                                             as                                      EXAM_LCTN_ZIP
		, 
		 COALESCE(CDE.CDES_EXM_ADDR_CNTY_NM,CP.CASE_PTCP_CNTY, CES.EXAM_LCTN_COUNTY_NAME)
                                                             as                              EXAM_LCTN_COUNTY_NAME
		, 
		 COALESCE(CDE.CDES_EXM_ADDR_CNTRY_NM, CP.CASE_PTCP_CNTRY, CES.EXAM_LCTN_COUNTRY_NAME)
                                                             as                             EXAM_LCTN_COUNTRY_NAME
		, 
		 IF EXAM_LCTN_STATE_NAME STARTS WITH 'OH'  THEN 'N', IF EXAM_LCTN_STATE_NAME IS NULL THEN NULL ELSE 'Y'
                                                             as                                   OUT_OF_STATE_IND
		, ZIP_CODE_LONGITUDE
		, ZIP_CODE_LATITUDE
		, NO_SHOW_COUNT
		, NO_SHOW_IND
		, EXAM_RESCHDL_COUNT
		, EXAM_RESCHDL_IND
		, CASE_COMP_DT
		, CURRENT_IND 
FROM CES
            /* Final layer Filter: Ignore/filter out the records if CASE_PRFL_CTG_TYP_CD = 'ADR' */
