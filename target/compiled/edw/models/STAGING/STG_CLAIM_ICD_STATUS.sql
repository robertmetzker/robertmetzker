---- SRC LAYER ----
WITH
SRC_CIS as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_ICD_STATUS ),
SRC_ISTS as ( SELECT *     from     DEV_VIEWS.PCMP.ICD_STATUS_TYPE ),
SRC_BCIS as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_CLAIM_ICD_STATUS ),
SRC_ILT as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_ICD_LOCATION_TYPE ),
SRC_IST as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_ICD_SITE_TYPE ),
SRC_ICD as ( SELECT *     from     DEV_VIEWS.PCMP.ICD ),
SRC_IT as ( SELECT *     from     DEV_VIEWS.PCMP.ICD_TYPE ),
SRC_IVT as ( SELECT *     from     DEV_VIEWS.PCMP.ICD_VALIDITY_TYPE ),
//SRC_CIS as ( SELECT *     from     CLAIM_ICD_STATUS) ,
//SRC_ISTS as ( SELECT *     from     ICD_STATUS_TYPE) ,
//SRC_BCIS as ( SELECT *     from     BWC_CLAIM_ICD_STATUS) ,
//SRC_ILT as ( SELECT *     from     BWC_ICD_LOCATION_TYPE) ,
//SRC_IST as ( SELECT *     from     BWC_ICD_SITE_TYPE) ,
//SRC_ICD as ( SELECT *     from     ICD) ,
//SRC_IT as ( SELECT *     from     ICD_TYPE) ,
//SRC_IVT as ( SELECT *     from     ICD_VALIDITY_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CIS as ( SELECT 
		  CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		, upper( CLM_ICD_STS_PRI_IND )                       as                                CLM_ICD_STS_PRI_IND 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		, upper( TRIM( ICD_STS_TYP_CD ) )                    as                                     ICD_STS_TYP_CD 
		, ICD_ID                                             as                                             ICD_ID 
		, cast( CLM_ICD_STS_EFF_DT as DATE )                 as                                 CLM_ICD_STS_EFF_DT 
		, cast( CLM_ICD_STS_END_DT as DATE )                 as                                 CLM_ICD_STS_END_DT 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_CIS
            ),
LOGIC_ISTS as ( SELECT 
		  upper( TRIM( ICD_STS_TYP_NM ) )                    as                                     ICD_STS_TYP_NM 
		, upper( TRIM( ICD_STS_TYP_CD ) )                    as                                     ICD_STS_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_ISTS
            ),
LOGIC_BCIS as ( SELECT 
		  cast( ICD_STS_DT as DATE )                         as                                         ICD_STS_DT 
		, upper( TRIM( ICD_LOC_TYP_CD ) )                    as                                     ICD_LOC_TYP_CD 
		, upper( TRIM( ICD_SITE_TYP_CD ) )                   as                                    ICD_SITE_TYP_CD 
		, upper( TRIM( CLM_ICD_DESC ) )                      as                                       CLM_ICD_DESC 
		, cast( DW_CHANGE_DT as DATE )                       as                                       DW_CHANGE_DT 
		, CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID 
		from SRC_BCIS
            ),
LOGIC_ILT as ( SELECT 
		  upper( TRIM( ICD_LOC_TYP_NM ) )                    as                                     ICD_LOC_TYP_NM 
		, upper( TRIM( ICD_LOC_TYP_CD ) )                    as                                     ICD_LOC_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_ILT
            ),
LOGIC_IST as ( SELECT 
		  upper( TRIM( ICD_SITE_TYP_NM ) )                   as                                    ICD_SITE_TYP_NM 
		, upper( TRIM( ICD_SITE_TYP_NM_SRCH ) )              as                               ICD_SITE_TYP_NM_SRCH 
		, ICD_SITE_TYP_DSPLY_ORD                             as                             ICD_SITE_TYP_DSPLY_ORD 
		, upper( TRIM( ICD_SITE_TYP_CD ) )                   as                                    ICD_SITE_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_IST
            ),
LOGIC_ICD as ( SELECT 
		  ICD_ID                                             as                                             ICD_ID 
		, upper( TRIM( ICD_VER_CD ) )                        as                                         ICD_VER_CD 
		, upper( TRIM( ICD_CD ) )                            as                                             ICD_CD 
		, upper( TRIM( ICD_TYP_CD ) )                        as                                         ICD_TYP_CD 
		, upper( TRIM( ICD_VLD_TYP_CD ) )                    as                                     ICD_VLD_TYP_CD 
		, cast( ICD_EFF_DT as DATE )                         as                                         ICD_EFF_DT 
		, cast( ICD_END_DT as DATE )                         as                                         ICD_END_DT 
		, upper( TRIM( ICD_SHR_DESC ) )                      as                                       ICD_SHR_DESC 
		, upper( TRIM( ICD_LNG_DESC ) )                      as                                       ICD_LNG_DESC 
		, upper( TRIM( ICD_FULL_DESC ) )                     as                                      ICD_FULL_DESC 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_ICD
            ),
LOGIC_IT as ( SELECT 
		  upper( TRIM( ICD_TYP_NM ) )                        as                                         ICD_TYP_NM 
		, upper( TRIM( ICD_TYP_CD ) )                        as                                         ICD_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_IT
            ),
LOGIC_IVT as ( SELECT 
		  upper( TRIM( ICD_VLD_TYP_NM ) )                    as                                     ICD_VLD_TYP_NM 
		, upper( TRIM( ICD_VLD_TYP_CD ) )                    as                                     ICD_VLD_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_IVT
            )

---- RENAME LAYER ----
,

RENAME_CIS as ( SELECT 
		  CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND
		, VOID_IND                                           as                                           VOID_IND
		, ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD
		, ICD_ID                                             as                                             ICD_ID
		, CLM_ICD_STS_EFF_DT                                 as                                 CLM_ICD_STS_EFF_DT
		, CLM_ICD_STS_END_DT                                 as                                 CLM_ICD_STS_END_DT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_CIS   ), 
RENAME_ISTS as ( SELECT 
		  ICD_STS_TYP_NM                                     as                                     ICD_STS_TYP_NM
		, ICD_STS_TYP_CD                                     as                                ISTS_ICD_STS_TYP_CD
		, VOID_IND                                           as                                      ISTS_VOID_IND 
				FROM     LOGIC_ISTS   ), 
RENAME_BCIS as ( SELECT 
		  ICD_STS_DT                                         as                                         ICD_STS_DT
		, ICD_LOC_TYP_CD                                     as                                     ICD_LOC_TYP_CD
		, ICD_SITE_TYP_CD                                    as                                    ICD_SITE_TYP_CD
		, CLM_ICD_DESC                                       as                                       CLM_ICD_DESC
		, DW_CHANGE_DT                                       as                                       DW_CHANGE_DT
		, CLM_ICD_STS_ID                                     as                                BCIS_CLM_ICD_STS_ID 
				FROM     LOGIC_BCIS   ), 
RENAME_ILT as ( SELECT 
		  ICD_LOC_TYP_NM                                     as                                     ICD_LOC_TYP_NM
		, ICD_LOC_TYP_CD                                     as                                 ILT_ICD_LOC_TYP_CD
		, VOID_IND                                           as                                       ILT_VOID_IND 
				FROM     LOGIC_ILT   ), 
RENAME_IST as ( SELECT 
		  ICD_SITE_TYP_NM                                    as                                    ICD_SITE_TYP_NM
		, ICD_SITE_TYP_NM_SRCH                               as                               ICD_SITE_TYP_NM_SRCH
		, ICD_SITE_TYP_DSPLY_ORD                             as                             ICD_SITE_TYP_DSPLY_ORD
		, ICD_SITE_TYP_CD                                    as                                IST_ICD_SITE_TYP_CD
		, VOID_IND                                           as                                       IST_VOID_IND 
				FROM     LOGIC_IST   ), 
RENAME_ICD as ( SELECT 
		  ICD_ID                                             as                                         ICD_ICD_ID
		, ICD_VER_CD                                         as                                         ICD_VER_CD
		, ICD_CD                                             as                                             ICD_CD
		, ICD_TYP_CD                                         as                                         ICD_TYP_CD
		, ICD_VLD_TYP_CD                                     as                                     ICD_VLD_TYP_CD
		, ICD_EFF_DT                                         as                                         ICD_EFF_DT
		, ICD_END_DT                                         as                                         ICD_END_DT
		, ICD_SHR_DESC                                       as                                       ICD_SHR_DESC
		, ICD_LNG_DESC                                       as                                       ICD_LNG_DESC
		, ICD_FULL_DESC                                      as                                      ICD_FULL_DESC
		, VOID_IND                                           as                                       ICD_VOID_IND 
				FROM     LOGIC_ICD   ), 
RENAME_IT as ( SELECT 
		  ICD_TYP_NM                                         as                                         ICD_TYP_NM
		, ICD_TYP_CD                                         as                                      IT_ICD_TYP_CD
		, VOID_IND                                           as                                        IT_VOID_IND 
				FROM     LOGIC_IT   ), 
RENAME_IVT as ( SELECT 
		  ICD_VLD_TYP_NM                                     as                                     ICD_VLD_TYP_NM
		, ICD_VLD_TYP_CD                                     as                                 IVT_ICD_VLD_TYP_CD
		, VOID_IND                                           as                                       IVT_VOID_IND 
				FROM     LOGIC_IVT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CIS                            as ( SELECT * from    RENAME_CIS   ),
FILTER_BCIS                           as ( SELECT * from    RENAME_BCIS   ),
FILTER_ICD                            as ( SELECT * from    RENAME_ICD 
                                            WHERE ICD_VOID_IND = 'N'  ),
FILTER_ISTS                           as ( SELECT * from    RENAME_ISTS 
                                            WHERE ISTS_VOID_IND = 'N'  ),
FILTER_IT                             as ( SELECT * from    RENAME_IT 
                                            WHERE IT_VOID_IND = 'N'  ),
FILTER_IVT                            as ( SELECT * from    RENAME_IVT 
                                            WHERE IVT_VOID_IND = 'N'  ),
FILTER_ILT                            as ( SELECT * from    RENAME_ILT 
                                            WHERE ILT_VOID_IND = 'N'  ),
FILTER_IST                            as ( SELECT * from    RENAME_IST 
                                            WHERE IST_VOID_IND = 'N'  ),

---- JOIN LAYER ----

BCIS as ( SELECT * 
				FROM  FILTER_BCIS
				LEFT JOIN FILTER_ILT ON  FILTER_BCIS.ICD_LOC_TYP_CD =  FILTER_ILT.ILT_ICD_LOC_TYP_CD 
								LEFT JOIN FILTER_IST ON  FILTER_BCIS.ICD_SITE_TYP_CD =  FILTER_IST.IST_ICD_SITE_TYP_CD  ),
ICD as ( SELECT * 
				FROM  FILTER_ICD
				LEFT JOIN FILTER_IT ON  FILTER_ICD.ICD_TYP_CD =  FILTER_IT.IT_ICD_TYP_CD 
								LEFT JOIN FILTER_IVT ON  FILTER_ICD.ICD_VLD_TYP_CD =  FILTER_IVT.IVT_ICD_VLD_TYP_CD  ),
CIS as ( SELECT * 
				FROM  FILTER_CIS
				INNER JOIN BCIS ON  FILTER_CIS.CLM_ICD_STS_ID = BCIS.BCIS_CLM_ICD_STS_ID 
						LEFT JOIN ICD ON  FILTER_CIS.ICD_ID = ICD.ICD_ICD_ID 
						LEFT JOIN FILTER_ISTS ON  FILTER_CIS.ICD_STS_TYP_CD =  FILTER_ISTS.ISTS_ICD_STS_TYP_CD  )
SELECT * 
from CIS