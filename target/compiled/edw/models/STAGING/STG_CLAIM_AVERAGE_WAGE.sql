---- SRC LAYER ----
WITH
SRC_CAW as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_AVERAGE_WAGE ),
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM ),
SRC_CAWT as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_AVERAGE_WAGE_TYPE ),
SRC_STS as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_AVERAGE_WG_CALC_STS_TYP ),
//SRC_CAW as ( SELECT *     from     CLAIM_AVERAGE_WAGE) ,
//SRC_C as ( SELECT *     from     CLAIM) ,
//SRC_CAWT as ( SELECT *     from     CLAIM_AVERAGE_WAGE_TYPE) ,
//SRC_STS as ( SELECT *     from     CLAIM_AVERAGE_WG_CALC_STS_TYP) ,

---- LOGIC LAYER ----

LOGIC_CAW as ( SELECT 
		  CLM_AVG_WG_ID                                      AS                                      CLM_AVG_WG_ID 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, upper( TRIM( CLM_AVG_WG_TYP_CD ) )                 AS                                  CLM_AVG_WG_TYP_CD 
		, upper( TRIM( CAWCST_CD ) )                         AS                                          CAWCST_CD 
		, CLM_AVG_WG_CALC_AMT                                AS                                CLM_AVG_WG_CALC_AMT 
		, CLM_AVG_WG_DRV_AMT                                 AS                                 CLM_AVG_WG_DRV_AMT 
		, CLM_AVG_WG_OVRRD_AMT                               AS                               CLM_AVG_WG_OVRRD_AMT 
		, CLM_AVG_WG_CALC_BNFT_RT_AMT                        AS                        CLM_AVG_WG_CALC_BNFT_RT_AMT 
		, upper( TRIM( CLM_AVG_WG_OVRRD_COMT ) )             AS                              CLM_AVG_WG_OVRRD_COMT 
		, CLM_AVG_WG_DRV_BNFT_RT                             AS                             CLM_AVG_WG_DRV_BNFT_RT 
		, CLM_AVG_WG_OVRRD_BNFT_RT                           AS                           CLM_AVG_WG_OVRRD_BNFT_RT 
		, upper( TRIM( CLM_AVG_WG_OVRRD_BNFT_COMT ) )        AS                         CLM_AVG_WG_OVRRD_BNFT_COMT 
		, cast( CLM_AVG_WG_EFF_DT as DATE )                  AS                                  CLM_AVG_WG_EFF_DT 
		, cast( CLM_AVG_WG_END_DT as DATE )                  AS                                  CLM_AVG_WG_END_DT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CAW
            ),
LOGIC_C as ( SELECT 
		  upper( TRIM( CLM_NO ) )                            AS                                             CLM_NO 
		, AGRE_ID                                            AS                                            AGRE_ID 
		from SRC_C
            ),
LOGIC_CAWT as ( SELECT 
		  upper( TRIM( CLM_AVG_WG_TYP_NM ) )                 AS                                  CLM_AVG_WG_TYP_NM 
		, upper( TRIM( CLM_AVG_WG_TYP_CD ) )                 AS                                  CLM_AVG_WG_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CAWT
            ),
LOGIC_STS as ( SELECT 
		  upper( TRIM( CAWCST_NM ) )                         AS                                          CAWCST_NM 
		, upper( TRIM( CAWCST_CD ) )                         AS                                          CAWCST_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_STS
            )

---- RENAME LAYER ----
,

RENAME_CAW as ( SELECT 
		  CLM_AVG_WG_ID                                      as                                      CLM_AVG_WG_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_AVG_WG_TYP_CD                                  as                                  CLM_AVG_WG_TYP_CD
		, CAWCST_CD                                          as                                          CAWCST_CD
		, CLM_AVG_WG_CALC_AMT                                as                                CLM_AVG_WG_CALC_AMT
		, CLM_AVG_WG_DRV_AMT                                 as                                 CLM_AVG_WG_DRV_AMT
		, CLM_AVG_WG_OVRRD_AMT                               as                               CLM_AVG_WG_OVRRD_AMT
		, CLM_AVG_WG_CALC_BNFT_RT_AMT                        as                        CLM_AVG_WG_CALC_BNFT_RT_AMT
		, CLM_AVG_WG_OVRRD_COMT                              as                              CLM_AVG_WG_OVRRD_COMT
		, CLM_AVG_WG_DRV_BNFT_RT                             as                             CLM_AVG_WG_DRV_BNFT_RT
		, CLM_AVG_WG_OVRRD_BNFT_RT                           as                           CLM_AVG_WG_OVRRD_BNFT_RT
		, CLM_AVG_WG_OVRRD_BNFT_COMT                         as                         CLM_AVG_WG_OVRRD_BNFT_COMT
		, CLM_AVG_WG_EFF_DT                                  as                                  CLM_AVG_WG_EFF_DT
		, CLM_AVG_WG_END_DT                                  as                                  CLM_AVG_WG_END_DT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CAW   ), 
RENAME_C as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
		, AGRE_ID                                            as                                          C_AGRE_ID 
				FROM     LOGIC_C   ), 
RENAME_CAWT as ( SELECT 
		  CLM_AVG_WG_TYP_NM                                  as                                  CLM_AVG_WG_TYP_NM
		, CLM_AVG_WG_TYP_CD                                  as                             CAWT_CLM_AVG_WG_TYP_CD
		, VOID_IND                                           as                                      CAWT_VOID_IND 
				FROM     LOGIC_CAWT   ), 
RENAME_STS as ( SELECT 
		  CAWCST_NM                                          as                                          CAWCST_NM
		, CAWCST_CD                                          as                                      STS_CAWCST_CD
		, VOID_IND                                           as                                       STS_VOID_IND 
				FROM     LOGIC_STS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CAW                            as ( SELECT * from    RENAME_CAW   ),
FILTER_C                              as ( SELECT * from    RENAME_C   ),
FILTER_CAWT                           as ( SELECT * from    RENAME_CAWT 
				WHERE CAWT_VOID_IND = 'N'  ),
FILTER_STS                            as ( SELECT * from    RENAME_STS 
				WHERE STS_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CAW as ( SELECT * 
				FROM  FILTER_CAW
				INNER JOIN FILTER_C ON  FILTER_CAW.AGRE_ID =  FILTER_C.C_AGRE_ID 
								LEFT JOIN FILTER_CAWT ON  FILTER_CAW.CLM_AVG_WG_TYP_CD =  FILTER_CAWT.CAWT_CLM_AVG_WG_TYP_CD 
								LEFT JOIN FILTER_STS ON  FILTER_CAW.CAWCST_CD =  FILTER_STS.STS_CAWCST_CD  )
SELECT * 
from CAW