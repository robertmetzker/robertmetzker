

---- SRC LAYER ----
WITH
SRC_DOC            as ( SELECT *     FROM     STAGING.DSV_POLICY_DOCUMENTS ),
SRC_CUST           as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_CUSTOMER ),
//SRC_DOC            as ( SELECT *     FROM     DSV_POLICY_DOCUMENTS) ,
//SRC_CUST           as ( SELECT *     FROM     DIM_CUSTOMER) ,

---- LOGIC LAYER ----


LOGIC_DOC as ( 
    SELECT 
		  DOCM_NO                                            as                                            DOCM_NO 
		,  CASE WHEN nullif(array_to_string(array_construct_compact( DOCM_TYP_REF_NO, DOCM_TYP_VER_NO),''), '') is NULL  
                THEN MD5( '99999' ) ELSE  
                md5(cast(
                           coalesce(cast(DOCM_TYP_REF_NO as varchar), '') || '-' || 
                           coalesce(cast(DOCM_TYP_VER_NO as varchar), '') as varchar
                   )    )
                END                                          as                                 DOCUMENT_TYPE_HKEY		
		,  CASE WHEN nullif(array_to_string(array_construct_compact( DOCM_STT_TYP_CD, DOCM_STS_TYP_CD, DOCM_SYS_GEN_IND, DOCM_TYP_VER_DUP_IND, DOCM_TYP_VER_SYS_GEN_IND, DOCM_TYP_VER_SYS_GEN_ONLY_IND, DOCM_TYP_VER_CANC_PLCY_IND, DOCM_TYP_VER_PRE_PRNT_ENCL_IND, DOCM_TYP_VER_BTCH_PRNT_IND, DOCM_TYP_VER_USR_CAN_DEL_IND, DOCM_TYP_VER_MULTI_RDR_VER_IND, DOCM_TYP_VER_PRNT_DUPLX_IND),''), '') is NULL  
                THEN MD5( '99999' ) ELSE  
                md5(cast(
                    coalesce(cast(DOCM_STT_TYP_CD as varchar), '') || '-' || 
                    coalesce(cast(DOCM_STS_TYP_CD as varchar), '') || '-' || 
                    coalesce(cast(DOCM_SYS_GEN_IND as varchar), '') || '-' || 
                    coalesce(cast(DOCM_TYP_VER_DUP_IND as varchar), '') || '-' || 
                    coalesce(cast(DOCM_TYP_VER_SYS_GEN_IND as varchar), '') || '-' || 
                    coalesce(cast(DOCM_TYP_VER_SYS_GEN_ONLY_IND as varchar), '') || '-' || 
                    coalesce(cast(DOCM_TYP_VER_CANC_PLCY_IND as varchar), '') || '-' || 
                    coalesce(cast(DOCM_TYP_VER_PRE_PRNT_ENCL_IND as varchar), '') || '-' || 
                    coalesce(cast(DOCM_TYP_VER_BTCH_PRNT_IND as varchar), '') || '-' || 
                    coalesce(cast(DOCM_TYP_VER_USR_CAN_DEL_IND as varchar), '') || '-' || 
                    coalesce(cast(DOCM_TYP_VER_MULTI_RDR_VER_IND as varchar), '') || '-' || 
                    coalesce(cast(DOCM_TYP_VER_PRNT_DUPLX_IND as varchar), '')
                as varchar ))
                END                                          as                               DOCUMENT_DETAIL_HKEY		
		, md5(cast(
                    coalesce(cast(PTCP_TYP_CD as varchar), '') || '-' || 
                    coalesce(cast(PLCY_PRD_PTCP_INS_PRI_IND as varchar), '')
            as varchar ))                                    AS                             PARTICIPATION_TYPE_HKEY
		, CASE WHEN DOCM_STT_TYP_EFF_DT IS NULL THEN -1
                WHEN REPLACE(CAST(DOCM_STT_TYP_EFF_DT::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
                WHEN REPLACE(CAST(DOCM_STT_TYP_EFF_DT::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
                ELSE REPLACE(CAST(DOCM_STT_TYP_EFF_DT::DATE AS VARCHAR),'-','')::INTEGER 
                END                                          as                                 DOCUMENT_STATE_DATE_KEY
        , CASE WHEN DOCM_STS_TYP_EFF_DT IS NULL THEN -1
                WHEN REPLACE(CAST(DOCM_STS_TYP_EFF_DT::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
                WHEN REPLACE(CAST(DOCM_STS_TYP_EFF_DT::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
                ELSE REPLACE(CAST(DOCM_STS_TYP_EFF_DT::DATE AS VARCHAR),'-','')::INTEGER 
                END                                          as                                 DOCUMENT_STATUS_DATE_KEY
        , CASE WHEN AUDIT_USER_CREA_DTM IS NULL THEN -1
                WHEN REPLACE(CAST(AUDIT_USER_CREA_DTM::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
                WHEN REPLACE(CAST(AUDIT_USER_CREA_DTM::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
                ELSE REPLACE(CAST(AUDIT_USER_CREA_DTM::DATE AS VARCHAR),'-','')::INTEGER 
                END                                          as                                 DOCUMENT_PENDING_DATE_KEY
        , CASE WHEN DOCM_TARND_EXPT_DT_DRV IS NULL THEN -1
                WHEN REPLACE(CAST(DOCM_TARND_EXPT_DT_DRV::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
                WHEN REPLACE(CAST(DOCM_TARND_EXPT_DT_DRV::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
                ELSE REPLACE(CAST(DOCM_TARND_EXPT_DT_DRV::DATE AS VARCHAR),'-','')::INTEGER 
                END                                          as                                 TURN_AROUND_EXPECTED_DATE_KEY
        , CASE WHEN DOCM_TARND_RTN_DT IS NULL THEN -1
                WHEN REPLACE(CAST(DOCM_TARND_RTN_DT::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
                WHEN REPLACE(CAST(DOCM_TARND_RTN_DT::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
                ELSE REPLACE(CAST(DOCM_TARND_RTN_DT::DATE AS VARCHAR),'-','')::INTEGER 
                END                                          as                                 TURN_AROUND_RETURNED_DATE_KEY
        ,  CASE WHEN nullif(array_to_string(array_construct_compact( CR_USER_LGN_NM),''), '') is NULL  
                THEN MD5( '99999' ) ELSE  
                md5(cast(
                        coalesce(cast(CR_USER_LGN_NM as varchar), '')
                    as varchar ))
                END                                          as                                 CREATE_USER_HKEY 
        , CASE WHEN AUDIT_USER_UPDT_DTM IS NULL THEN -1
                WHEN REPLACE(CAST(AUDIT_USER_UPDT_DTM::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
                WHEN REPLACE(CAST(AUDIT_USER_UPDT_DTM::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
                ELSE REPLACE(CAST(AUDIT_USER_UPDT_DTM::DATE AS VARCHAR),'-','')::INTEGER 
                END                                          as                                 CREATE_USER_DATE_KEY
        ,  CASE WHEN nullif(array_to_string(array_construct_compact( UP_USER_LGN_NM),''), '') is NULL  
                THEN MD5( '99999' ) ELSE  
                md5(cast(
                        coalesce(cast(UP_USER_LGN_NM as varchar), '')
                     as varchar))
                END                                          as                                 UPDATE_USER_HKEY
        , CASE WHEN AUDIT_USER_UPDT_DTM IS NULL THEN -1
                WHEN REPLACE(CAST(AUDIT_USER_UPDT_DTM::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
                WHEN REPLACE(CAST(AUDIT_USER_UPDT_DTM::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
                ELSE REPLACE(CAST(AUDIT_USER_UPDT_DTM::DATE AS VARCHAR),'-','')::INTEGER 
                END                                          as                                 UPDATE_USER_DATE_KEY
		, CASE WHEN DOCM_STS_TYP_CD = 'PRDC' THEN (DOCM_TARND_RTN_DT - DOCM_STS_TYP_EFF_DT) ELSE NULL END AS TURN_AROUND_ACTUAL_DAY_COUNT 				  
        , DOCM_STT_TYP_EFF_DT                                as                                DOCM_STT_TYP_EFF_DT 
		, DOCM_STS_TYP_EFF_DT                                as                                DOCM_STS_TYP_EFF_DT 
		, DOCM_PND_DT                                        as                                        DOCM_PND_DT 
		, DOCM_TARND_EXPT_DT_DRV                             as                             DOCM_TARND_EXPT_DT_DRV 
		, DOCM_TARND_RTN_DT                                  as                                  DOCM_TARND_RTN_DT 
		, AUDIT_USER_CREA_DTM                                as                                 AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                 AUDIT_USER_UPDT_DTM 
		, PLCY_NO                                            as                                            PLCY_NO 
		, DOCM_TARND_EXPT_DD                                 as                                 DOCM_TARND_EXPT_DD 
		, DOCM_TYP_REF_NO                                    as                                    DOCM_TYP_REF_NO 
		, DOCM_TYP_VER_NO                                    as                                    DOCM_TYP_VER_NO 
		, DOCM_TYP_VER_BTCH_PRNT_TYP                         as                         DOCM_TYP_VER_BTCH_PRNT_TYP 
		, DOCM_TYP_VER_DUP_IND                               as                               DOCM_TYP_VER_DUP_IND 
		, DOCM_TYP_VER_SYS_GEN_IND                           as                           DOCM_TYP_VER_SYS_GEN_IND 
		, DOCM_TYP_VER_SYS_GEN_ONLY_IND                      as                      DOCM_TYP_VER_SYS_GEN_ONLY_IND 
		, DOCM_TYP_VER_CLM_DCSN_IND                          as                          DOCM_TYP_VER_CLM_DCSN_IND 
		, DOCM_TYP_VER_CANC_PLCY_IND                         as                         DOCM_TYP_VER_CANC_PLCY_IND 
		, DOCM_TYP_VER_PRE_PRNT_ENCL_IND                     as                     DOCM_TYP_VER_PRE_PRNT_ENCL_IND 
		, DOCM_TYP_VER_LCL_DLVR_IND                          as                          DOCM_TYP_VER_LCL_DLVR_IND 
		, DOCM_TYP_VER_BTCH_PRNT_IND                         as                         DOCM_TYP_VER_BTCH_PRNT_IND 
		, DOCM_TYP_VER_USR_CAN_DEL_IND                       as                       DOCM_TYP_VER_USR_CAN_DEL_IND 
		, DOCM_TYP_VER_MULTI_RDR_VER_IND                     as                     DOCM_TYP_VER_MULTI_RDR_VER_IND 
		, DOCM_TYP_VER_PRNT_DUPLX_IND                        as                        DOCM_TYP_VER_PRNT_DUPLX_IND 
		, CR_USER_LGN_NM                                     as                                     CR_USER_LGN_NM 
		, UP_USER_LGN_NM                                     as                                     UP_USER_LGN_NM 
		, VOID_IND                                           as                                           VOID_IND 
		, DOCM_TYP_VER_VOID_IND                              as                              DOCM_TYP_VER_VOID_IND 
		, DOCM_STT_TYP_CD                                    as                                    DOCM_STT_TYP_CD 
		, DOCM_STS_TYP_CD                                    as                                    DOCM_STS_TYP_CD 
		, CUST_NO                                            as                                            CUST_NO
		, PTCP_ID                                            as                                            PTCP_ID 
		FROM SRC_DOC
            ),
LOGIC_CUST as ( SELECT 
		  CUSTOMER_HKEY                                      as                                      CUSTOMER_HKEY 
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                               RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                     RECORD_END_DATE 
		FROM SRC_CUST
            )
---- RENAME LAYER ----
,
RENAME_DOC        as ( SELECT 
		  DOCM_NO                                            as                                    DOCUMENT_NUMBER
		, DOCUMENT_TYPE_HKEY                                 as                                 DOCUMENT_TYPE_HKEY
		, DOCUMENT_DETAIL_HKEY                               as                               DOCUMENT_DETAIL_HKEY
		, PARTICIPATION_TYPE_HKEY                            as                            PARTICIPATION_TYPE_HKEY
		, DOCUMENT_STATE_DATE_KEY                                as                            DOCUMENT_STATE_DATE_KEY
		, DOCUMENT_STATUS_DATE_KEY                                as                           DOCUMENT_STATUS_DATE_KEY
		, DOCUMENT_PENDING_DATE_KEY                            as                          DOCUMENT_PENDING_DATE_KEY
		, TURN_AROUND_EXPECTED_DATE_KEY                             as                      TURN_AROUND_EXPECTED_DATE_KEY
		, TURN_AROUND_RETURNED_DATE_KEY                                  as                      TURN_AROUND_RETURNED_DATE_KEY
		, CREATE_USER_HKEY                                   as                                   CREATE_USER_HKEY
		, CREATE_USER_DATE_KEY                                 as                               CREATE_USER_DATE_KEY
		, UPDATE_USER_HKEY                                   as                                   UPDATE_USER_HKEY
		, UPDATE_USER_DATE_KEY                                 as                               UPDATE_USER_DATE_KEY
		, PLCY_NO                                            as                                      POLICY_NUMBER
		, DOCM_TARND_EXPT_DD                                 as                     TURN_AROUND_EXPECTED_DAY_COUNT
		, TURN_AROUND_ACTUAL_DAY_COUNT                       as                       TURN_AROUND_ACTUAL_DAY_COUNT
		, DOCM_TYP_REF_NO                                    as                                    DOCM_TYP_REF_NO
		, DOCM_TYP_VER_NO                                    as                                    DOCM_TYP_VER_NO
		, DOCM_TYP_VER_BTCH_PRNT_TYP                         as                         DOCM_TYP_VER_BTCH_PRNT_TYP
		, DOCM_TYP_VER_DUP_IND                               as                               DOCM_TYP_VER_DUP_IND
		, DOCM_TYP_VER_SYS_GEN_IND                           as                           DOCM_TYP_VER_SYS_GEN_IND
		, DOCM_TYP_VER_SYS_GEN_ONLY_IND                      as                      DOCM_TYP_VER_SYS_GEN_ONLY_IND
		, DOCM_TYP_VER_CLM_DCSN_IND                          as                          DOCM_TYP_VER_CLM_DCSN_IND
		, DOCM_TYP_VER_CANC_PLCY_IND                         as                         DOCM_TYP_VER_CANC_PLCY_IND
		, DOCM_TYP_VER_PRE_PRNT_ENCL_IND                     as                     DOCM_TYP_VER_PRE_PRNT_ENCL_IND
		, DOCM_TYP_VER_LCL_DLVR_IND                          as                          DOCM_TYP_VER_LCL_DLVR_IND
		, DOCM_TYP_VER_BTCH_PRNT_IND                         as                         DOCM_TYP_VER_BTCH_PRNT_IND
		, DOCM_TYP_VER_USR_CAN_DEL_IND                       as                       DOCM_TYP_VER_USR_CAN_DEL_IND
		, DOCM_TYP_VER_MULTI_RDR_VER_IND                     as                     DOCM_TYP_VER_MULTI_RDR_VER_IND
		, DOCM_TYP_VER_PRNT_DUPLX_IND                        as                        DOCM_TYP_VER_PRNT_DUPLX_IND
		, DOCM_STT_TYP_EFF_DT                                as                                DOCM_STT_TYP_EFF_DT
		, DOCM_STS_TYP_EFF_DT                                as                                DOCM_STS_TYP_EFF_DT
		, DOCM_PND_DT                                        as                                        DOCM_PND_DT
		, DOCM_TARND_EXPT_DT_DRV                             as                             DOCM_TARND_EXPT_DT_DRV
		, DOCM_TARND_RTN_DT                                  as                                  DOCM_TARND_RTN_DT
		, CR_USER_LGN_NM                                     as                                     CR_USER_LGN_NM
		, UP_USER_LGN_NM                                     as                                     UP_USER_LGN_NM
		, VOID_IND                                           as                                           VOID_IND
		, DOCM_TYP_VER_VOID_IND                              as                              DOCM_TYP_VER_VOID_IND
		, DOCM_STT_TYP_CD                                    as                                    DOCM_STT_TYP_CD
		, DOCM_STS_TYP_CD                                    as                                    DOCM_STS_TYP_CD
		, CUST_NO                                            as                                            CUST_NO
		, PTCP_ID                                            as                                            PARTICIPATION_ID 
				FROM     LOGIC_DOC   ), 
RENAME_CUST       as ( SELECT 
		  CUSTOMER_HKEY                                      as                                      CUSTOMER_HKEY
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                               RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                     RECORD_END_DATE 
				FROM     LOGIC_CUST   )
--- FILTER LAYER (uses aliases) ----
,
FILTER_DOC                            as ( SELECT * FROM    RENAME_DOC 
                                            WHERE VOID_IND = 'N' AND DOCM_TYP_VER_VOID_IND = 'N'  ),
FILTER_CUST                           as ( SELECT * FROM    RENAME_CUST   ),
---- JOIN LAYER ----
DOC as ( SELECT * 
				FROM  FILTER_DOC
                LEFT JOIN FILTER_CUST ON  coalesce(FILTER_DOC.CUST_NO, '99999') =  FILTER_CUST.CUSTOMER_NUMBER AND DOCM_STS_TYP_EFF_DT BETWEEN RECORD_EFFECTIVE_DATE
                AND coalesce(RECORD_END_DATE, '2099-12-31')  ),
----------ETL LAYER----------				
ETL AS(SELECT 
  DOCUMENT_NUMBER
, PARTICIPATION_ID  
, CUSTOMER_HKEY
, DOCUMENT_TYPE_HKEY
, DOCUMENT_DETAIL_HKEY
, PARTICIPATION_TYPE_HKEY
, DOCUMENT_STATE_DATE_KEY
, DOCUMENT_STATUS_DATE_KEY
, DOCUMENT_PENDING_DATE_KEY
, TURN_AROUND_EXPECTED_DATE_KEY
, TURN_AROUND_RETURNED_DATE_KEY
, CREATE_USER_HKEY
, CREATE_USER_DATE_KEY
, UPDATE_USER_HKEY
, UPDATE_USER_DATE_KEY
, POLICY_NUMBER
, TURN_AROUND_EXPECTED_DAY_COUNT
, TURN_AROUND_ACTUAL_DAY_COUNT
, CURRENT_TIMESTAMP AS LOAD_DATETIME
, try_to_TIMESTAMP('Invalid')  AS UPDATE_DATETIME
, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM

FROM DOC)
select * from etl
--SELECT count(*), count(distinct customer_hkey), count(customer_hkey) FROM ETL
;
    