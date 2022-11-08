

      create or replace  table DEV_EDW.EDW_STG_CLAIMS_MART.FACT_CLAIM_CORRESPONDENCE  as
      (

---- SRC LAYER ----
WITH
SRC_DOC as ( SELECT *     from     STAGING.DSV_CLAIM_DOCUMENTS ),
//SRC_DOC as ( SELECT *     from     DSV_CLAIM_DOCUMENTS) ,

---- LOGIC LAYER ----

LOGIC_DOC as ( SELECT 
		  DOCM_NO                                            as                                            DOCM_NO 
		,  md5(cast(
    
    coalesce(cast(DOCM_TYP_REF_NO as 
    varchar
), '') || '-' || coalesce(cast(DOCM_TYP_VER_NO as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                                 DOCUMENT_TYPE_HKEY                                         
		,  md5(cast(
    
    coalesce(cast(DOCM_STT_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(DOCM_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(DOCM_SYS_GEN_IND as 
    varchar
), '') || '-' || coalesce(cast(DOCM_TYP_VER_DUP_IND as 
    varchar
), '') || '-' || coalesce(cast(DOCM_TYP_VER_SYS_GEN_IND as 
    varchar
), '') || '-' || coalesce(cast(DOCM_TYP_VER_SYS_GEN_ONLY_IND as 
    varchar
), '') || '-' || coalesce(cast(DOCM_TYP_VER_CANC_PLCY_IND as 
    varchar
), '') || '-' || coalesce(cast(DOCM_TYP_VER_PRE_PRNT_ENCL_IND as 
    varchar
), '') || '-' || coalesce(cast(DOCM_TYP_VER_BTCH_PRNT_IND as 
    varchar
), '') || '-' || coalesce(cast(DOCM_TYP_VER_USR_CAN_DEL_IND as 
    varchar
), '') || '-' || coalesce(cast(DOCM_TYP_VER_MULTI_RDR_VER_IND as 
    varchar
), '') || '-' || coalesce(cast(DOCM_TYP_VER_PRNT_DUPLX_IND as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                              DOCUMENT_DETAIL_HKEY                             
		
		, CASE WHEN  DOCM_STT_TYP_EFF_DT is null then '-1' 
                 WHEN  DOCM_STT_TYP_EFF_DT < '1901-01-01' then '-2' 
                 WHEN  DOCM_STT_TYP_EFF_DT > '2099-12-31' then '-3' 
                    ELSE regexp_replace(  DOCM_STT_TYP_EFF_DT, '[^0-9]+', '') 
			        END :: INTEGER                           as                                DOCM_STT_TYP_EFF_DT 
		, CASE WHEN  DOCM_STS_TYP_EFF_DT is null then '-1' 
                 WHEN  DOCM_STS_TYP_EFF_DT < '1901-01-01' then '-2' 
                 WHEN  DOCM_STS_TYP_EFF_DT > '2099-12-31' then '-3' 
                   ELSE regexp_replace(  DOCM_STS_TYP_EFF_DT, '[^0-9]+', '') 
			       END :: INTEGER                            as                                DOCM_STS_TYP_EFF_DT 
		, CASE WHEN  DOCM_PND_DT is null then '-1' 
                 WHEN  DOCM_PND_DT < '1901-01-01' then '-2' 
                 WHEN  DOCM_PND_DT > '2099-12-31' then '-3' 
                   ELSE regexp_replace(  DOCM_PND_DT, '[^0-9]+', '') 
                   END :: INTEGER                            as                                         DOCM_PND_DT 
		, CASE WHEN  DOCM_TARND_EXPT_DT_DRV is null then '-1' 
                 WHEN  DOCM_TARND_EXPT_DT_DRV < '1901-01-01' then '-2' 
                 WHEN  DOCM_TARND_EXPT_DT_DRV > '2099-12-31' then '-3' 
                   ELSE regexp_replace(  DOCM_TARND_EXPT_DT_DRV, '[^0-9]+', '') 
                   END :: INTEGER                            as                              DOCM_TARND_EXPT_DT_DRV 
		,  CASE WHEN  DOCM_TARND_RTN_DT is null then '-1' 
                 WHEN  DOCM_TARND_RTN_DT < '1901-01-01' then '-2' 
                 WHEN  DOCM_TARND_RTN_DT > '2099-12-31' then '-3' 
                   ELSE regexp_replace(  DOCM_TARND_RTN_DT, '[^0-9]+', '') 
                   END :: INTEGER                            as                                DOCM_TARND_RTN_DT 
		,  md5(cast(
    
    coalesce(cast(CR_USER_LGN_NM as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                                   CREATE_USER_HKEY                                           
		,  CASE WHEN  AUDIT_USER_CREA_DT is null then '-1' 
                 WHEN  AUDIT_USER_CREA_DT < '1901-01-01' then '-2' 
                 WHEN  AUDIT_USER_CREA_DT > '2099-12-31' then '-3' 
                   ELSE regexp_replace(  AUDIT_USER_CREA_DT, '[^0-9]+', '') 
                   END :: INTEGER                                 as                            AUDIT_USER_CREA_DT 
		,  md5(cast(
    
    coalesce(cast(UP_USER_LGN_NM as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                                   UPDATE_USER_HKEY                                                
		, CASE WHEN  AUDIT_USER_UPDT_DT is null then '-1' 
                 WHEN  AUDIT_USER_UPDT_DT < '1901-01-01' then '-2' 
                 WHEN  AUDIT_USER_UPDT_DT > '2099-12-31' then '-3' 
                   ELSE regexp_replace(  AUDIT_USER_UPDT_DT, '[^0-9]+', '') 
                   END :: INTEGER                            as                                 AUDIT_USER_UPDT_DT 
		, CLM_NO                                             as                                             CLM_NO 
		, DOCM_TARND_EXPT_DD                                 as                                 DOCM_TARND_EXPT_DD 
		, case when DOCM_STS_TYP_CD = 'PRDC' then (DOCM_TARND_RTN_DT - DOCM_STS_TYP_EFF_DT) else null end 
		                                                     as                       TURN_AROUND_ACTUAL_DAY_COUNT		                                                                                    
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
		, DOCM_STS_TYP_CD                                    as                                    DOCM_STS_TYP_CD
		, DOCM_STT_TYP_CD                                    as                                    DOCM_STT_TYP_CD
		from SRC_DOC
            )

---- RENAME LAYER ----
,

RENAME_DOC as ( SELECT 
		  DOCM_NO                                            as                                    DOCUMENT_NUMBER
		, DOCUMENT_TYPE_HKEY                                 as                                 DOCUMENT_TYPE_HKEY
		, DOCUMENT_DETAIL_HKEY                               as                               DOCUMENT_DETAIL_HKEY
		, DOCM_STT_TYP_EFF_DT                                as                            DOCUMENT_STATE_DATE_KEY
		, DOCM_STS_TYP_EFF_DT                                as                           DOCUMENT_STATUS_DATE_KEY
		, DOCM_PND_DT                                        as                          DOCUMENT_PENDING_DATE_KEY
		, DOCM_TARND_EXPT_DT_DRV                             as                      TURN_AROUND_EXPECTED_DATE_KEY
		, DOCM_TARND_RTN_DT                                  as                      TURN_AROUND_RETURNED_DATE_KEY
		, CREATE_USER_HKEY                                   as                                   CREATE_USER_HKEY
		, AUDIT_USER_CREA_DT                                 as                               CREATE_USER_DATE_KEY
		, UPDATE_USER_HKEY                                   as                                   UPDATE_USER_HKEY
		, AUDIT_USER_UPDT_DT                                 as                               UPDATE_USER_DATE_KEY
	    , CLM_NO                                             as                                       CLAIM_NUMBER
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
        , CR_USER_LGN_NM                                     as                                     CR_USER_LGN_NM
		, UP_USER_LGN_NM                                     as                                     UP_USER_LGN_NM
		, VOID_IND                                           as                                           VOID_IND
		, DOCM_TYP_VER_VOID_IND                              as                              DOCM_TYP_VER_VOID_IND 		
		, DOCM_STS_TYP_CD                                    as                                    DOCM_STS_TYP_CD
		, DOCM_STT_TYP_CD                                    as                                    DOCM_STT_TYP_CD
						FROM     LOGIC_DOC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DOC                            as ( SELECT * from    RENAME_DOC 
                                            WHERE VOID_IND = 'N' AND DOCM_TYP_VER_VOID_IND = 'N'  ),

---- JOIN LAYER ----

 JOIN_DOC  as  ( SELECT * 
				FROM  FILTER_DOC )
 SELECT 
  DOCUMENT_NUMBER
, coalesce( DOCUMENT_TYPE_HKEY, MD5( '99999' )) as DOCUMENT_TYPE_HKEY
, coalesce( DOCUMENT_DETAIL_HKEY, MD5( '99999' )) as DOCUMENT_DETAIL_HKEY
, DOCUMENT_STATE_DATE_KEY
, DOCUMENT_STATUS_DATE_KEY
, DOCUMENT_PENDING_DATE_KEY
, TURN_AROUND_EXPECTED_DATE_KEY
, TURN_AROUND_RETURNED_DATE_KEY
, coalesce( CREATE_USER_HKEY, MD5( '99999' )) as CREATE_USER_HKEY
, CREATE_USER_DATE_KEY
, coalesce( UPDATE_USER_HKEY, MD5( '99999' )) as UPDATE_USER_HKEY
, UPDATE_USER_DATE_KEY
, CLAIM_NUMBER
, TURN_AROUND_EXPECTED_DAY_COUNT
, TURN_AROUND_ACTUAL_DAY_COUNT
, CURRENT_TIMESTAMP AS LOAD_DATETIME
, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM  
  FROM  JOIN_DOC
      );
    