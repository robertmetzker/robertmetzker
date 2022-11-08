---- SRC LAYER ----
WITH
SRC_A as ( SELECT *     from     STAGING.STG_ACTIVITY ),
SRC_AD as ( SELECT *     from     STAGING.STG_ACTIVITY_DETAIL ),
SRC_DCA            as ( SELECT *     FROM     STAGING.DST_CLAIM_ACTIVITY ),
//SRC_A as ( SELECT *     from     STG_ACTIVITY) ,
//SRC_AD as ( SELECT *     from     STG_ACTIVITY_DETAIL) ,
//SRC_DCA as ( SELECT *     FROM     DST_CLAIM_ACTIVITY) ,

---- LOGIC LAYER ----

LOGIC_A as ( SELECT 
		  TRIM( CNTX_TYP_NM )                                as                                        CNTX_TYP_NM 
		, TRIM( SUBLOC_TYP_NM )                              as                                      SUBLOC_TYP_NM 
		, ACTV_ID                                            as                                            ACTV_ID 
		from SRC_A
            ),
LOGIC_AD as ( SELECT 
		  TRIM( ACTV_NM_TYP_NM )                             as                                     ACTV_NM_TYP_NM 
		, TRIM( ACTV_ACTN_TYP_NM )                           as                                   ACTV_ACTN_TYP_NM 
		, TRIM( ACTV_DTL_DESC )                              as                                      ACTV_DTL_DESC 
		, TRIM( ACTV_DTL_COL_NM )                            as                                    ACTV_DTL_COL_NM 
		, ACTV_ID                                            as                                            ACTV_ID 
		from SRC_AD
            ),
LOGIC_DCA as ( SELECT 
		  TRIM( FNCT_ROLE_NM )                               as                                       FNCT_ROLE_NM 
		, ACTV_ID                                            as                                            ACTV_ID 
		, ACTV_DTL_ID                                        as                                        ACTV_DTL_ID 
		FROM SRC_DCA
            )
---- RENAME LAYER ----
,

RENAME_A as ( SELECT 
		  CNTX_TYP_NM                                        as                                        CNTX_TYP_NM
		, SUBLOC_TYP_NM                                      as                                      SUBLOC_TYP_NM
		, ACTV_ID                                            as                                            ACTV_ID 
				FROM     LOGIC_A   ), 
RENAME_AD as ( SELECT 
		  ACTV_NM_TYP_NM                                     as                                     ACTV_NM_TYP_NM
		, ACTV_ACTN_TYP_NM                                   as                                   ACTV_ACTN_TYP_NM
		, ACTV_DTL_DESC                                      as                                      ACTV_DTL_DESC
		, ACTV_DTL_COL_NM                                    as                                    ACTV_DTL_COL_NM
		, ACTV_ID                                            as                                         AD_ACTV_ID 
				FROM     LOGIC_AD   ),
RENAME_DCA        as ( SELECT 
		  FNCT_ROLE_NM                                       as                                       FNCT_ROLE_NM
		, ACTV_ID                                            as                                        DCA_ACTV_ID
		, ACTV_DTL_ID                                        as                                    DCA_ACTV_DTL_ID 
				FROM     LOGIC_DCA   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_A                              as ( SELECT * from    RENAME_A   ),
FILTER_AD                             as ( SELECT * from    RENAME_AD   ),
FILTER_DCA                            as ( SELECT * FROM    RENAME_DCA   ),

---- JOIN LAYER ----

A as ( SELECT * 
				FROM  FILTER_A
				INNER JOIN FILTER_AD ON  FILTER_A.ACTV_ID =  FILTER_AD.AD_ACTV_ID  
				LEFT JOIN FILTER_DCA ON  FILTER_A.ACTV_ID =  FILTER_DCA.DCA_ACTV_ID  )
SELECT distinct
		 md5(cast(
    
    coalesce(cast(ACTV_ACTN_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(ACTV_NM_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(CNTX_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(ACTV_DTL_COL_NM as 
    varchar
), '') || '-' || coalesce(cast(SUBLOC_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(ACTV_DTL_DESC as 
    varchar
), '') || '-' || coalesce(cast(FNCT_ROLE_NM as 
    varchar
), '')

 as 
    varchar
)) As UNIQUE_ID_KEY
		, CNTX_TYP_NM
		, SUBLOC_TYP_NM
		, ACTV_NM_TYP_NM
		, ACTV_ACTN_TYP_NM
		, ACTV_DTL_DESC
		, ACTV_DTL_COL_NM
		, FNCT_ROLE_NM
from A