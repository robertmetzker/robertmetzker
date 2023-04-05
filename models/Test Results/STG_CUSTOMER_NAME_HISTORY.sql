

---- SRC LAYER ----
WITH

SRC_CN             as ( SELECT *     FROM     {{ ref( 'CUSTOMER_NAME_HISTORY' ) }} ),
SRC_CNT            as ( SELECT *     FROM     {{ ref( 'CUSTOMER_NAME_TYPE' ) }} ),
SRC_CNTT           as ( SELECT *     FROM     {{ ref( 'CUSTOMER_NAME_TITLE_TYPE' ) }} ),
SRC_CNST           as ( SELECT *     FROM     {{ ref( 'CUSTOMER_NAME_SUFFIX_TYPE' ) }} ),

/*
SRC_CN             as ( SELECT *     FROM     PCMP8.CUSTOMER_NAME_HISTORY ),
SRC_CNT            as ( SELECT *     FROM     PCMP8.CUSTOMER_NAME_TYPE ),
SRC_CNTT           as ( SELECT *     FROM     PCMP8.CUSTOMER_NAME_TITLE_TYPE ),
SRC_CNST           as ( SELECT *     FROM     PCMP8.CUSTOMER_NAME_SUFFIX_TYPE ),

*/

---- LOGIC LAYER ----


, LOGIC_CN as ( 
	SELECT 	  
		 HIST_ID                                            as                                            HIST_ID	, 
		 CUST_NM_ID                                         as                                         CUST_NM_ID	, 
		 CUST_ID                                            as                                            CUST_ID	, 
		 NULLIF( TRIM( CUST_NM_TYP_CD ),'' )                as                                     CUST_NM_TYP_CD	, 
		 cast( CUST_NM_EFF_DT as DATE )                     as                                     CUST_NM_EFF_DT	, 
		 cast( CUST_NM_END_DT as DATE )                     as                                     CUST_NM_END_DT	, 
		 NULLIF( TRIM( CUST_NM_NM ),'' )                    as                                         CUST_NM_NM	, 
		 NULLIF( TRIM( CUST_NM_DRV_UPCS_NM ),'' )           as                                CUST_NM_DRV_UPCS_NM	, 
		 NULLIF( TRIM( CUST_NM_SRCH_NRMLIZED_NM ),'' )      as                           CUST_NM_SRCH_NRMLIZED_NM	, 
		 NULLIF( TRIM( CUST_NM_TTL_TYP_CD ),'' )            as                                 CUST_NM_TTL_TYP_CD	, 
		 NULLIF( TRIM( CUST_NM_FST ),'' )                   as                                        CUST_NM_FST	, 
		 NULLIF( TRIM( CUST_NM_MID ),'' )                   as                                        CUST_NM_MID	, 
		 NULLIF( TRIM( CUST_NM_LST ),'' )                   as                                        CUST_NM_LST	, 
		 NULLIF( TRIM( CUST_NM_SFX_TYP_CD ),'' )            as                                 CUST_NM_SFX_TYP_CD	, 
		 NULLIF( TRIM( CUST_NM_TAX_VRF_IND ),'' )           as                                CUST_NM_TAX_VRF_IND	, 
		 HIST_EFF_DTM                                       as                                       HIST_EFF_DTM	, 
		 HIST_END_DTM                                       as                                       HIST_END_DTM	, 
		 AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA	, 
		 AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM	, 
		 AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT	, 
		 AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM	, 
		 NULLIF( TRIM( VOID_IND ),'' )                      as                                           VOID_IND
	 from SRC_CN )

, LOGIC_CNT as ( 
	SELECT 	  
		 NULLIF( TRIM( CUST_NM_TYP_NM ),'' )                as                                     CUST_NM_TYP_NM	, 
		 NULLIF( TRIM( CUST_NM_TYP_CD ),'' )                as                                     CUST_NM_TYP_CD	, 
		 NULLIF( TRIM( CUST_NM_TYP_VOID_IND ),'' )          as                               CUST_NM_TYP_VOID_IND
	 from SRC_CNT )

, LOGIC_CNTT as ( 
	SELECT 	  
		 NULLIF( TRIM( CUST_NM_TTL_TYP_NM ),'' )            as                                 CUST_NM_TTL_TYP_NM	, 
		 NULLIF( TRIM( CUST_NM_TTL_TYP_CD ),'' )            as                                 CUST_NM_TTL_TYP_CD	, 
		 NULLIF( TRIM( CUST_NM_TTL_TYP_VOID_IND ),'' )      as                           CUST_NM_TTL_TYP_VOID_IND
	 from SRC_CNTT )

, LOGIC_CNST as ( 
	SELECT 	  
		 NULLIF( TRIM( CUST_NM_SFX_TYP_NM ),'' )            as                                 CUST_NM_SFX_TYP_NM	, 
		 NULLIF( TRIM( CUST_NM_SFX_TYP_CD ),'' )            as                                 CUST_NM_SFX_TYP_CD	, 
		 NULLIF( TRIM( CUST_NM_SFX_TYP_VOID_IND ),'' )      as                           CUST_NM_SFX_TYP_VOID_IND
	 from SRC_CNST )


---- RENAME LAYER ----


, RENAME_CN as ( SELECT  
		 HIST_ID                                            as                                            HIST_ID , 
		 CUST_NM_ID                                         as                                         CUST_NM_ID , 
		 CUST_ID                                            as                                            CUST_ID , 
		 CUST_NM_TYP_CD                                     as                                     CUST_NM_TYP_CD , 
		 CUST_NM_EFF_DT                                     as                                   CUST_NM_EFF_DATE , 
		 CUST_NM_END_DT                                     as                                   CUST_NM_END_DATE , 
		 CUST_NM_NM                                         as                                         CUST_NM_NM , 
		 CUST_NM_DRV_UPCS_NM                                as                                CUST_NM_DRV_UPCS_NM , 
		 CUST_NM_SRCH_NRMLIZED_NM                           as                           CUST_NM_SRCH_NRMLIZED_NM , 
		 CUST_NM_TTL_TYP_CD                                 as                                 CUST_NM_TTL_TYP_CD , 
		 CUST_NM_FST                                        as                                        CUST_NM_FST , 
		 CUST_NM_MID                                        as                                        CUST_NM_MID , 
		 CUST_NM_LST                                        as                                        CUST_NM_LST , 
		 CUST_NM_SFX_TYP_CD                                 as                                 CUST_NM_SFX_TYP_CD , 
		 CUST_NM_TAX_VRF_IND                                as                                CUST_NM_TAX_VRF_IND , 
		 HIST_EFF_DTM                                       as                                       HIST_EFF_DTM , 
		 HIST_END_DTM                                       as                                       HIST_END_DTM , 
		 AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA , 
		 AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM , 
		 AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT , 
		 AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM , 
		 VOID_IND                                           as                                           VOID_IND 
		FROM LOGIC_CN
            )

, RENAME_CNT as ( SELECT  
		 CUST_NM_TYP_NM                                     as                                     CUST_NM_TYP_NM , 
		 CUST_NM_TYP_CD                                     as                                 CNT_CUST_NM_TYP_CD , 
		 CUST_NM_TYP_VOID_IND                               as                               CUST_NM_TYP_VOID_IND 
		FROM LOGIC_CNT
            )

, RENAME_CNTT as ( SELECT  
		 CUST_NM_TTL_TYP_NM                                 as                                 CUST_NM_TTL_TYP_NM , 
		 CUST_NM_TTL_TYP_CD                                 as                            CNTT_CUST_NM_TTL_TYP_CD , 
		 CUST_NM_TTL_TYP_VOID_IND                           as                           CUST_NM_TTL_TYP_VOID_IND 
		FROM LOGIC_CNTT
            )

, RENAME_CNST as ( SELECT  
		 CUST_NM_SFX_TYP_NM                                 as                                 CUST_NM_SFX_TYP_NM , 
		 CUST_NM_SFX_TYP_CD                                 as                            CNST_CUST_NM_SFX_TYP_CD , 
		 CUST_NM_SFX_TYP_VOID_IND                           as                           CUST_NM_SFX_TYP_VOID_IND 
		FROM LOGIC_CNST
            )

---- FILTER LAYER ----

FILTER_CN                             as ( SELECT * FROM    RENAME_CN    ),
FILTER_CNT                            as ( SELECT * FROM    RENAME_CNT 
                                            WHERE CUST_NM_TYP_VOID_IND = 'N'   ),
FILTER_CNTT                           as ( SELECT * FROM    RENAME_CNTT 
                                            WHERE CUST_NM_TTL_TYP_VOID_IND = 'N'   ),
FILTER_CNST                           as ( SELECT * FROM    RENAME_CNST 
                                            WHERE CUST_NM_SFX_TYP_VOID_IND = 'N'   )

---- JOIN LAYER ----

CN as ( SELECT * 
				FROM  FILTER_CN
				LEFT JOIN FILTER_CNT ON  FILTER_CN.CUST_NM_TYP_CD =  FILTER_CNT.CNT_CUST_NM_TYP_CD 
								LEFT JOIN FILTER_CNTT ON  FILTER_CN.CUST_NM_TTL_TYP_CD =  FILTER_CNTT.CNTT_CUST_NM_TTL_TYP_CD 
								LEFT JOIN FILTER_CNST ON  FILTER_CN.CUST_NM_SFX_TYP_CD =  FILTER_CNST.CNST_CUST_NM_SFX_TYP_CD  )
SELECT * 
FROM CN