

      create or replace  table DEV_EDW.STAGING.STG_INDEMNITY_SCHEDULE_DETAIL_AMOUNT  as
      (---- SRC LAYER ----
WITH
SRC_ISDA as ( SELECT *     from     DEV_VIEWS.PCMP.INDEMNITY_SCHEDULE_DTL_AMT ),
SRC_BT as ( SELECT *     from     DEV_VIEWS.PCMP.BENEFIT_TYPE ),
SRC_ISDAT as ( SELECT *     from     DEV_VIEWS.PCMP.INDEMNITY_SCHEDULE_DTL_AMT_TYP ),
SRC_STS as ( SELECT *     from     DEV_VIEWS.PCMP.INDEMNITY_SCH_DTL_STS_TYP ),
//SRC_ISDA as ( SELECT *     from     INDEMNITY_SCHEDULE_DTL_AMT) ,
//SRC_BT as ( SELECT *     from     BENEFIT_TYPE) ,
//SRC_ISDAT as ( SELECT *     from     INDEMNITY_SCHEDULE_DTL_AMT_TYP) ,
//SRC_STS as ( SELECT *     from     INDEMNITY_SCH_DTL_STS_TYP) ,

---- LOGIC LAYER ----

LOGIC_ISDA as ( SELECT 
		  INDM_SCH_DTL_AMT_ID                                AS                                INDM_SCH_DTL_AMT_ID 
		, INDM_SCH_DTL_ID                                    AS                                    INDM_SCH_DTL_ID 
		, CUST_ID                                            AS                                            CUST_ID 
		, upper( TRIM( BNFT_TYP_CD ) )                       AS                                        BNFT_TYP_CD 
		, upper( TRIM( INDM_SCH_DTL_AMT_TYP_CD ) )           AS                            INDM_SCH_DTL_AMT_TYP_CD 
		, upper( TRIM( INDM_SCH_DTL_STS_TYP_CD ) )           AS                            INDM_SCH_DTL_STS_TYP_CD 
		, INDM_SCH_DTL_AMT_AMT                               AS                               INDM_SCH_DTL_AMT_AMT 
		, INDM_SCH_DTL_AMT_CHK_GRP_NO                        AS                        INDM_SCH_DTL_AMT_CHK_GRP_NO 
		, upper( INDM_SCH_DTL_AMT_PRI_IND )                  AS                           INDM_SCH_DTL_AMT_PRI_IND 
		, upper( INDM_SCH_DTL_AMT_MAILTO_IND )               AS                        INDM_SCH_DTL_AMT_MAILTO_IND 
		, upper( INDM_SCH_DTL_AMT_RMND_IND )                 AS                          INDM_SCH_DTL_AMT_RMND_IND 
		, CFT_ID                                             AS                                             CFT_ID 
		, CUST_CHLD_SUPT_ID                                  AS                                  CUST_CHLD_SUPT_ID 
		, upper( INDM_SCH_DTL_AMT_JNT_PAYE_IND )             AS                      INDM_SCH_DTL_AMT_JNT_PAYE_IND 
		, upper( ATTCH_REQD_IND )                            AS                                     ATTCH_REQD_IND 
		, upper( PAY_BY_CHCK_OVRRD_IND )                     AS                              PAY_BY_CHCK_OVRRD_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_ISDA
            ),
LOGIC_BT as ( SELECT 
		  upper( TRIM( BNFT_TYP_NM ) )                       AS                                        BNFT_TYP_NM 
		, upper( TRIM( BNFT_TYP_CD ) )                       AS                                        BNFT_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_BT
            ),
LOGIC_ISDAT as ( SELECT 
		  upper( TRIM( INDM_SCH_DTL_AMT_TYP_NM ) )           AS                            INDM_SCH_DTL_AMT_TYP_NM 
		, upper( TRIM( INDM_SCH_DTL_AMT_TYP_CD ) )           AS                            INDM_SCH_DTL_AMT_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_ISDAT
            ),
LOGIC_STS as ( SELECT 
		  upper( TRIM( INDM_SCH_DTL_STS_TYP_NM ) )           AS                            INDM_SCH_DTL_STS_TYP_NM 
		, upper( TRIM( INDM_SCH_DTL_STS_TYP_CD ) )           AS                            INDM_SCH_DTL_STS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_STS
            )

---- RENAME LAYER ----
,

RENAME_ISDA as ( SELECT 
		  INDM_SCH_DTL_AMT_ID                                as                                INDM_SCH_DTL_AMT_ID
		, INDM_SCH_DTL_ID                                    as                                    INDM_SCH_DTL_ID
		, CUST_ID                                            as                                            CUST_ID
		, BNFT_TYP_CD                                        as                                        BNFT_TYP_CD
		, INDM_SCH_DTL_AMT_TYP_CD                            as                            INDM_SCH_DTL_AMT_TYP_CD
		, INDM_SCH_DTL_STS_TYP_CD                            as                            INDM_SCH_DTL_STS_TYP_CD
		, INDM_SCH_DTL_AMT_AMT                               as                               INDM_SCH_DTL_AMT_AMT
		, INDM_SCH_DTL_AMT_CHK_GRP_NO                        as                        INDM_SCH_DTL_AMT_CHK_GRP_NO
		, INDM_SCH_DTL_AMT_PRI_IND                           as                           INDM_SCH_DTL_AMT_PRI_IND
		, INDM_SCH_DTL_AMT_MAILTO_IND                        as                        INDM_SCH_DTL_AMT_MAILTO_IND
		, INDM_SCH_DTL_AMT_RMND_IND                          as                          INDM_SCH_DTL_AMT_RMND_IND
		, CFT_ID                                             as                                             CFT_ID
		, CUST_CHLD_SUPT_ID                                  as                                  CUST_CHLD_SUPT_ID
		, INDM_SCH_DTL_AMT_JNT_PAYE_IND                      as                      INDM_SCH_DTL_AMT_JNT_PAYE_IND
		, ATTCH_REQD_IND                                     as                                     ATTCH_REQD_IND
		, PAY_BY_CHCK_OVRRD_IND                              as                              PAY_BY_CHCK_OVRRD_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_ISDA   ), 
RENAME_BT as ( SELECT 
		  BNFT_TYP_NM                                        as                                        BNFT_TYP_NM
		, BNFT_TYP_CD                                        as                                     BT_BNFT_TYP_CD
		, VOID_IND                                           as                                        BT_VOID_IND 
				FROM     LOGIC_BT   ), 
RENAME_ISDAT as ( SELECT 
		  INDM_SCH_DTL_AMT_TYP_NM                            as                            INDM_SCH_DTL_AMT_TYP_NM
		, INDM_SCH_DTL_AMT_TYP_CD                            as                      ISDAT_INDM_SCH_DTL_AMT_TYP_CD
		, VOID_IND                                           as                                     ISDAT_VOID_IND 
				FROM     LOGIC_ISDAT   ), 
RENAME_STS as ( SELECT 
		  INDM_SCH_DTL_STS_TYP_NM                            as                            INDM_SCH_DTL_STS_TYP_NM
		, INDM_SCH_DTL_STS_TYP_CD                            as                        STS_INDM_SCH_DTL_STS_TYP_CD
		, VOID_IND                                           as                                       STS_VOID_IND 
				FROM     LOGIC_STS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ISDA                           as ( SELECT * from    RENAME_ISDA   ),
FILTER_STS                            as ( SELECT * from    RENAME_STS 
				WHERE STS_VOID_IND = 'N'  ),
FILTER_ISDAT                          as ( SELECT * from    RENAME_ISDAT 
				WHERE ISDAT_VOID_IND = 'N'  ),
FILTER_BT                             as ( SELECT * from    RENAME_BT   ),

---- JOIN LAYER ----

ISDA as ( SELECT * 
				FROM  FILTER_ISDA
				LEFT JOIN FILTER_STS ON  FILTER_ISDA.INDM_SCH_DTL_STS_TYP_CD =  FILTER_STS.STS_INDM_SCH_DTL_STS_TYP_CD 
								LEFT JOIN FILTER_ISDAT ON  FILTER_ISDA.INDM_SCH_DTL_AMT_TYP_CD =  FILTER_ISDAT.ISDAT_INDM_SCH_DTL_AMT_TYP_CD 
								LEFT JOIN FILTER_BT ON  FILTER_ISDA.BNFT_TYP_CD =  FILTER_BT.BT_BNFT_TYP_CD  )
SELECT * 
from ISDA
      );
    