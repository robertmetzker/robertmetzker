---- SRC LAYER ----
WITH
SRC_DOC            as ( SELECT *     FROM     STAGING.STG_DOCUMENT ),
//SRC_DOC            as ( SELECT *     FROM     STG_DOCUMENT) ,

---- LOGIC LAYER ----


LOGIC_DOC as ( SELECT 
		  DOCM_STT_TYP_CD                                    as                                    DOCM_STT_TYP_CD 
		, TRIM( DOCM_STT_TYP_NM )                            as                                    DOCM_STT_TYP_NM 
		, DOCM_STS_TYP_CD                                    as                                    DOCM_STS_TYP_CD 
		, TRIM( DOCM_STS_TYP_NM )                            as                                    DOCM_STS_TYP_NM 
		, TRIM( DOCM_SYS_GEN_IND )                           as                                   DOCM_SYS_GEN_IND 
		, TRIM( DOCM_TYP_VER_DUP_IND )                       as                               DOCM_TYP_VER_DUP_IND 
		, TRIM( DOCM_TYP_VER_SYS_GEN_IND )                   as                           DOCM_TYP_VER_SYS_GEN_IND 
		, TRIM( DOCM_TYP_VER_SYS_GEN_ONLY_IND )              as                      DOCM_TYP_VER_SYS_GEN_ONLY_IND 
		, TRIM( DOCM_TYP_VER_CANC_PLCY_IND )                 as                         DOCM_TYP_VER_CANC_PLCY_IND 
		, TRIM( DOCM_TYP_VER_PRE_PRNT_ENCL_IND )             as                     DOCM_TYP_VER_PRE_PRNT_ENCL_IND 
		, TRIM( DOCM_TYP_VER_BTCH_PRNT_IND )                 as                         DOCM_TYP_VER_BTCH_PRNT_IND 
		, TRIM( DOCM_TYP_VER_USR_CAN_DEL_IND )               as                       DOCM_TYP_VER_USR_CAN_DEL_IND 
		, TRIM( DOCM_TYP_VER_MULTI_RDR_VER_IND )             as                     DOCM_TYP_VER_MULTI_RDR_VER_IND 
		, TRIM( DOCM_TYP_VER_PRNT_DUPLX_IND )                as                        DOCM_TYP_VER_PRNT_DUPLX_IND 
		, TRIM( DOCM_TYP_VER_VOID_IND )                      as                              DOCM_TYP_VER_VOID_IND 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_DOC
            )

---- RENAME LAYER ----
,

RENAME_DOC        as ( SELECT 
		  DOCM_STT_TYP_CD                                    as                                    DOCM_STT_TYP_CD
		, DOCM_STT_TYP_NM                                    as                                    DOCM_STT_TYP_NM
		, DOCM_STS_TYP_CD                                    as                                    DOCM_STS_TYP_CD
		, DOCM_STS_TYP_NM                                    as                                    DOCM_STS_TYP_NM
		, DOCM_SYS_GEN_IND                                   as                                   DOCM_SYS_GEN_IND
		, DOCM_TYP_VER_DUP_IND                               as                               DOCM_TYP_VER_DUP_IND
		, DOCM_TYP_VER_SYS_GEN_IND                           as                           DOCM_TYP_VER_SYS_GEN_IND
		, DOCM_TYP_VER_SYS_GEN_ONLY_IND                      as                      DOCM_TYP_VER_SYS_GEN_ONLY_IND
		, DOCM_TYP_VER_CANC_PLCY_IND                         as                         DOCM_TYP_VER_CANC_PLCY_IND
		, DOCM_TYP_VER_PRE_PRNT_ENCL_IND                     as                     DOCM_TYP_VER_PRE_PRNT_ENCL_IND
		, DOCM_TYP_VER_BTCH_PRNT_IND                         as                         DOCM_TYP_VER_BTCH_PRNT_IND
		, DOCM_TYP_VER_USR_CAN_DEL_IND                       as                       DOCM_TYP_VER_USR_CAN_DEL_IND
		, DOCM_TYP_VER_MULTI_RDR_VER_IND                     as                     DOCM_TYP_VER_MULTI_RDR_VER_IND
		, DOCM_TYP_VER_PRNT_DUPLX_IND                        as                        DOCM_TYP_VER_PRNT_DUPLX_IND
		, DOCM_TYP_VER_VOID_IND                              as                              DOCM_TYP_VER_VOID_IND
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_DOC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DOC                            as ( SELECT * FROM    RENAME_DOC 
                                            WHERE VOID_IND = 'N' AND DOCM_TYP_VER_VOID_IND = 'N'   ),

---- JOIN LAYER ----

 JOIN_DOC         as  ( SELECT * FROM  FILTER_DOC ),

------ETL LAYER------------
ETL AS(SELECT distinct
md5(cast(
    
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
)) AS UNIQUE_ID_KEY
,DOCM_STT_TYP_CD
,DOCM_STT_TYP_NM
,DOCM_STS_TYP_CD
,DOCM_STS_TYP_NM
,DOCM_SYS_GEN_IND
,DOCM_TYP_VER_DUP_IND
,DOCM_TYP_VER_SYS_GEN_IND
,DOCM_TYP_VER_SYS_GEN_ONLY_IND
,DOCM_TYP_VER_CANC_PLCY_IND
,DOCM_TYP_VER_PRE_PRNT_ENCL_IND
,DOCM_TYP_VER_BTCH_PRNT_IND
,DOCM_TYP_VER_USR_CAN_DEL_IND
,DOCM_TYP_VER_MULTI_RDR_VER_IND
,DOCM_TYP_VER_PRNT_DUPLX_IND
FROM JOIN_DOC)

SELECT * FROM ETL