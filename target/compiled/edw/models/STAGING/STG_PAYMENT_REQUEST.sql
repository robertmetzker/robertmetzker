---- SRC LAYER ----
WITH
SRC_PR as ( SELECT *     from     DEV_VIEWS.PCMP.PAYMENT_REQUEST ),
SRC_PMPT as ( SELECT *     from     DEV_VIEWS.PCMP.PAYMENT_MEDIA_PREFERENCE_TYPE ),
SRC_PRT as ( SELECT *     from     DEV_VIEWS.PCMP.PAYMENT_REQUEST_TYPE ),
//SRC_PR as ( SELECT *     from     PAYMENT_REQUEST) ,
//SRC_PMPT as ( SELECT *     from     PAYMENT_MEDIA_PREFERENCE_TYPE) ,
//SRC_PRT as ( SELECT *     from     PAYMENT_REQUEST_TYPE) ,

---- LOGIC LAYER ----


LOGIC_PR as ( SELECT 
		  PAY_REQS_ID                                        as                                        PAY_REQS_ID 
		, upper( TRIM( PAY_REQS_NO ) )                       as                                        PAY_REQS_NO 
		, upper( PAY_REQS_AMT )                              as                                       PAY_REQS_AMT 
		, upper( TRIM( PAY_MEDA_PREF_TYP_CD ) )              as                               PAY_MEDA_PREF_TYP_CD 
		, upper( CUST_ID_PAYE )                              as                                       CUST_ID_PAYE 
		, upper( CUST_ID_INCO )                              as                                       CUST_ID_INCO 
		, upper( TRIM( PAY_REQS_TYP_CD ) )                   as                                    PAY_REQS_TYP_CD 
		, cast( PAY_REQS_DT as DATE )                        as                                        PAY_REQS_DT 
		, upper( TRIM( PAY_REQS_DRV_NM ) )                   as                                    PAY_REQS_DRV_NM 
		, upper( TRIM( PAY_REQS_INCO_NM ) )                  as                                   PAY_REQS_INCO_NM 
		, upper( TRIM( PAY_REQS_ADDR_STR_1 ) )               as                                PAY_REQS_ADDR_STR_1 
		, upper( TRIM( PAY_REQS_ADDR_STR_2 ) )               as                                PAY_REQS_ADDR_STR_2 
		, upper( TRIM( PAY_REQS_CITY_NM ) )                  as                                   PAY_REQS_CITY_NM 
		, upper( TRIM( PAY_REQS_STT_NM ) )                   as                                    PAY_REQS_STT_NM 
		, upper( TRIM( PAY_REQS_POST_CD ) )                  as                                   PAY_REQS_POST_CD 
		, upper( TRIM( PAY_REQS_CNTRY_NM ) )                 as                                  PAY_REQS_CNTRY_NM 
		, upper( TRIM( PAY_REQS_BANK_ACCT_TYP_NM ) )         as                          PAY_REQS_BANK_ACCT_TYP_NM 
		, upper( TRIM( PAY_REQS_BANK_ROUT_NO ) )             as                              PAY_REQS_BANK_ROUT_NO 
		, upper( TRIM( PAY_REQS_BANK_ACCT_NO ) )             as                              PAY_REQS_BANK_ACCT_NO 
		, upper( TRIM( PAY_REQS_CAR_BANK_ACCT_TYP_NM ) )     as                      PAY_REQS_CAR_BANK_ACCT_TYP_NM 
		, upper( TRIM( PAY_REQS_CHCK_TMPL_TYP_CD ) )         as                          PAY_REQS_CHCK_TMPL_TYP_CD 
		, upper( TRIM( PAY_REQS_CAR_BANK_NM ) )              as                               PAY_REQS_CAR_BANK_NM 
		, upper( TRIM( PAY_REQS_CAR_BANK_ACCT_NO ) )         as                          PAY_REQS_CAR_BANK_ACCT_NO 
		, upper( TRIM( PAY_REQS_CAR_BANK_ROUT_NO ) )         as                          PAY_REQS_CAR_BANK_ROUT_NO 
		, upper( TRIM( PAY_REQS_CAR_NM_1 ) )                 as                                  PAY_REQS_CAR_NM_1 
		, upper( TRIM( PAY_REQS_CAR_NM_2 ) )                 as                                  PAY_REQS_CAR_NM_2 
		, upper( TRIM( PAY_REQS_CAR_ADDR_STR_1 ) )           as                            PAY_REQS_CAR_ADDR_STR_1 
		, upper( TRIM( PAY_REQS_CAR_ADDR_STR_2 ) )           as                            PAY_REQS_CAR_ADDR_STR_2 
		, upper( TRIM( PAY_REQS_CAR_CITY_NM ) )              as                               PAY_REQS_CAR_CITY_NM 
		, upper( TRIM( PAY_REQS_CAR_STT_NM ) )               as                                PAY_REQS_CAR_STT_NM 
		, upper( TRIM( PAY_REQS_CAR_POST_CD ) )              as                               PAY_REQS_CAR_POST_CD 
		, upper( TRIM( PAY_REQS_CAR_PHN_NO ) )               as                                PAY_REQS_CAR_PHN_NO 
		, upper( TRIM( PAY_REQS_CAR_EFT_BANK_NM ) )          as                           PAY_REQS_CAR_EFT_BANK_NM 
		, upper( TRIM( PAY_REQS_EFT_IMMED_ORIG ) )           as                            PAY_REQS_EFT_IMMED_ORIG 
		, upper( TRIM( PAY_REQS_EFT_BTCH_ID ) )              as                               PAY_REQS_EFT_BTCH_ID 
		, upper( TRIM( PAY_REQS_EFT_BTCH_NM ) )              as                               PAY_REQS_EFT_BTCH_NM 
		, upper( TRIM( PAY_REQS_INS_CMPY_TYP_NM ) )          as                           PAY_REQS_INS_CMPY_TYP_NM 
		, upper( TRIM( PAY_REQS_FNCL_INST_NM ) )             as                              PAY_REQS_FNCL_INST_NM 
		, upper( TRIM( PAY_REQS_FNCL_INST_DBA_NM ) )         as                          PAY_REQS_FNCL_INST_DBA_NM 
		, upper( TRIM( PAY_REQS_FNCL_INST_ADDR_STR_1 ) )     as                      PAY_REQS_FNCL_INST_ADDR_STR_1 
		, upper( TRIM( PAY_REQS_FNCL_INST_ADDR_STR_2 ) )     as                      PAY_REQS_FNCL_INST_ADDR_STR_2 
		, upper( TRIM( PAY_REQS_FNCL_INST_CITY_NM ) )        as                         PAY_REQS_FNCL_INST_CITY_NM 
		, upper( TRIM( PAY_REQS_FNCL_INST_STT_NM ) )         as                          PAY_REQS_FNCL_INST_STT_NM 
		, upper( TRIM( PAY_REQS_FNCL_INST_POST_CD ) )        as                         PAY_REQS_FNCL_INST_POST_CD 
		, upper( TRIM( PAY_REQS_FNCL_INST_CNTRY_NM ) )       as                        PAY_REQS_FNCL_INST_CNTRY_NM 
		, upper( TRIM( PAY_REQS_FNCL_INST_PHN_NO ) )         as                          PAY_REQS_FNCL_INST_PHN_NO 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_PR
            ),

LOGIC_PMPT as ( SELECT 
		  upper( TRIM( PAY_MEDA_PREF_TYP_NM ) )              as                               PAY_MEDA_PREF_TYP_NM 
		, upper( PAY_MEDA_PREF_TYP_VOID_IND )                as                         PAY_MEDA_PREF_TYP_VOID_IND 
		, upper( TRIM( PAY_MEDA_PREF_TYP_CD ) )              as                               PAY_MEDA_PREF_TYP_CD 
		from SRC_PMPT
            ),

LOGIC_PRT as ( SELECT 
		  upper( TRIM( PAY_REQS_TYP_NM ) )                   as                                    PAY_REQS_TYP_NM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		, upper( TRIM( PAY_REQS_TYP_CD ) )                   as                                    PAY_REQS_TYP_CD 
		from SRC_PRT
            )

---- RENAME LAYER ----
,

RENAME_PR as ( SELECT 
		  PAY_REQS_ID                                        as                                        PAY_REQS_ID
		, PAY_REQS_NO                                        as                                        PAY_REQS_NO
		, PAY_REQS_AMT                                       as                                       PAY_REQS_AMT
		, PAY_MEDA_PREF_TYP_CD                               as                               PAY_MEDA_PREF_TYP_CD
		, CUST_ID_PAYE                                       as                                       CUST_ID_PAYE
		, CUST_ID_INCO                                       as                                       CUST_ID_INCO
		, PAY_REQS_TYP_CD                                    as                                    PAY_REQS_TYP_CD
		, PAY_REQS_DT                                        as                                        PAY_REQS_DT
		, PAY_REQS_DRV_NM                                    as                                    PAY_REQS_DRV_NM
		, PAY_REQS_INCO_NM                                   as                                   PAY_REQS_INCO_NM
		, PAY_REQS_ADDR_STR_1                                as                                PAY_REQS_ADDR_STR_1
		, PAY_REQS_ADDR_STR_2                                as                                PAY_REQS_ADDR_STR_2
		, PAY_REQS_CITY_NM                                   as                                   PAY_REQS_CITY_NM
		, PAY_REQS_STT_NM                                    as                                    PAY_REQS_STT_NM
		, PAY_REQS_POST_CD                                   as                                   PAY_REQS_POST_CD
		, PAY_REQS_CNTRY_NM                                  as                                  PAY_REQS_CNTRY_NM
		, PAY_REQS_BANK_ACCT_TYP_NM                          as                          PAY_REQS_BANK_ACCT_TYP_NM
		, PAY_REQS_BANK_ROUT_NO                              as                              PAY_REQS_BANK_ROUT_NO
		, PAY_REQS_BANK_ACCT_NO                              as                              PAY_REQS_BANK_ACCT_NO
		, PAY_REQS_CAR_BANK_ACCT_TYP_NM                      as                      PAY_REQS_CAR_BANK_ACCT_TYP_NM
		, PAY_REQS_CHCK_TMPL_TYP_CD                          as                          PAY_REQS_CHCK_TMPL_TYP_CD
		, PAY_REQS_CAR_BANK_NM                               as                               PAY_REQS_CAR_BANK_NM
		, PAY_REQS_CAR_BANK_ACCT_NO                          as                          PAY_REQS_CAR_BANK_ACCT_NO
		, PAY_REQS_CAR_BANK_ROUT_NO                          as                          PAY_REQS_CAR_BANK_ROUT_NO
		, PAY_REQS_CAR_NM_1                                  as                                  PAY_REQS_CAR_NM_1
		, PAY_REQS_CAR_NM_2                                  as                                  PAY_REQS_CAR_NM_2
		, PAY_REQS_CAR_ADDR_STR_1                            as                            PAY_REQS_CAR_ADDR_STR_1
		, PAY_REQS_CAR_ADDR_STR_2                            as                            PAY_REQS_CAR_ADDR_STR_2
		, PAY_REQS_CAR_CITY_NM                               as                               PAY_REQS_CAR_CITY_NM
		, PAY_REQS_CAR_STT_NM                                as                                PAY_REQS_CAR_STT_NM
		, PAY_REQS_CAR_POST_CD                               as                               PAY_REQS_CAR_POST_CD
		, PAY_REQS_CAR_PHN_NO                                as                                PAY_REQS_CAR_PHN_NO
		, PAY_REQS_CAR_EFT_BANK_NM                           as                           PAY_REQS_CAR_EFT_BANK_NM
		, PAY_REQS_EFT_IMMED_ORIG                            as                            PAY_REQS_EFT_IMMED_ORIG
		, PAY_REQS_EFT_BTCH_ID                               as                               PAY_REQS_EFT_BTCH_ID
		, PAY_REQS_EFT_BTCH_NM                               as                               PAY_REQS_EFT_BTCH_NM
		, PAY_REQS_INS_CMPY_TYP_NM                           as                           PAY_REQS_INS_CMPY_TYP_NM
		, PAY_REQS_FNCL_INST_NM                              as                              PAY_REQS_FNCL_INST_NM
		, PAY_REQS_FNCL_INST_DBA_NM                          as                          PAY_REQS_FNCL_INST_DBA_NM
		, PAY_REQS_FNCL_INST_ADDR_STR_1                      as                      PAY_REQS_FNCL_INST_ADDR_STR_1
		, PAY_REQS_FNCL_INST_ADDR_STR_2                      as                      PAY_REQS_FNCL_INST_ADDR_STR_2
		, PAY_REQS_FNCL_INST_CITY_NM                         as                         PAY_REQS_FNCL_INST_CITY_NM
		, PAY_REQS_FNCL_INST_STT_NM                          as                          PAY_REQS_FNCL_INST_STT_NM
		, PAY_REQS_FNCL_INST_POST_CD                         as                         PAY_REQS_FNCL_INST_POST_CD
		, PAY_REQS_FNCL_INST_CNTRY_NM                        as                        PAY_REQS_FNCL_INST_CNTRY_NM
		, PAY_REQS_FNCL_INST_PHN_NO                          as                          PAY_REQS_FNCL_INST_PHN_NO
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_PR   ), 
RENAME_PMPT as ( SELECT 
		  PAY_MEDA_PREF_TYP_NM                               as                               PAY_MEDA_PREF_TYP_NM
		, PAY_MEDA_PREF_TYP_VOID_IND                         as                         PAY_MEDA_PREF_TYP_VOID_IND
		, PAY_MEDA_PREF_TYP_CD                               as                          PMPT_PAY_MEDA_PREF_TYP_CD 
				FROM     LOGIC_PMPT   ), 
RENAME_PRT as ( SELECT 
		  PAY_REQS_TYP_NM                                    as                                    PAY_REQS_TYP_NM
		, VOID_IND                                           as                                       PRT_VOID_IND
		, PAY_REQS_TYP_CD                                    as                                PRT_PAY_REQS_TYP_CD 
				FROM     LOGIC_PRT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PR                             as ( SELECT * from    RENAME_PR   ),
FILTER_PMPT                           as ( SELECT * from    RENAME_PMPT 
                                            WHERE PAY_MEDA_PREF_TYP_VOID_IND = 'N'  ),
FILTER_PRT                            as ( SELECT * from    RENAME_PRT 
                                            WHERE PRT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

PR as ( SELECT * 
				FROM  FILTER_PR
				LEFT JOIN FILTER_PMPT ON  FILTER_PR.PAY_MEDA_PREF_TYP_CD =  FILTER_PMPT.PMPT_PAY_MEDA_PREF_TYP_CD 
								LEFT JOIN FILTER_PRT ON  FILTER_PR.PAY_REQS_TYP_CD =  FILTER_PRT.PRT_PAY_REQS_TYP_CD  )
SELECT * 
from PR