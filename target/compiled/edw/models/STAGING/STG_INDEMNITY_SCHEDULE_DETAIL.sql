---- SRC LAYER ----
WITH
SRC_ISD as ( SELECT *     from     DEV_VIEWS.PCMP.INDEMNITY_SCHEDULE_DETAIL ),
SRC_STS as ( SELECT *     from     DEV_VIEWS.PCMP.INDEMNITY_SCH_DTL_STS_TYP ),
//SRC_ISD as ( SELECT *     from     INDEMNITY_SCHEDULE_DETAIL) ,
//SRC_STS as ( SELECT *     from     INDEMNITY_SCH_DTL_STS_TYP) ,

---- LOGIC LAYER ----

LOGIC_ISD as ( SELECT 
		  INDM_SCH_DTL_ID                                    AS                                    INDM_SCH_DTL_ID 
		, INDM_SCH_ID                                        AS                                        INDM_SCH_ID 
		, cast( INDM_SCH_DTL_PRCS_DT as DATE )               AS                               INDM_SCH_DTL_PRCS_DT 
		, cast( INDM_SCH_DLVR_DT as DATE )                   AS                                   INDM_SCH_DLVR_DT 
		, cast( INDM_SCH_DTL_DRV_EFF_DT as DATE )            AS                            INDM_SCH_DTL_DRV_EFF_DT 
		, cast( INDM_SCH_DTL_DRV_END_DT as DATE )            AS                            INDM_SCH_DTL_DRV_END_DT 
		, INDM_SCH_DTL_DRV_DD                                AS                                INDM_SCH_DTL_DRV_DD 
		, INDM_SCH_DTL_DRV_AMT                               AS                               INDM_SCH_DTL_DRV_AMT 
		, upper( TRIM( INDM_SCH_DTL_STS_TYP_CD ) )           AS                            INDM_SCH_DTL_STS_TYP_CD 
		, INDM_SCH_DTL_OFST_AMT                              AS                              INDM_SCH_DTL_OFST_AMT 
		, INDM_SCH_DTL_LMP_RED_AMT                           AS                           INDM_SCH_DTL_LMP_RED_AMT 
		, INDM_SCH_DTL_OTHR_RED_AMT                          AS                          INDM_SCH_DTL_OTHR_RED_AMT 
		, INDM_SCH_DTL_DRV_NET_AMT                           AS                           INDM_SCH_DTL_DRV_NET_AMT 
		, upper( TRIM(INDM_SCH_DTL_FNL_PAY_IND ) )           AS                           INDM_SCH_DTL_FNL_PAY_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( TRIM( VOID_IND ) )                          AS                                           VOID_IND 
		, INDM_SCH_DTL_DSCNT_AMT                             AS                             INDM_SCH_DTL_DSCNT_AMT 
		from SRC_ISD
            ),
LOGIC_STS as ( SELECT 
		  upper( TRIM( INDM_SCH_DTL_STS_TYP_NM ) )           AS                            INDM_SCH_DTL_STS_TYP_NM 
		, upper( TRIM( INDM_SCH_DTL_STS_TYP_CD ) )           AS                            INDM_SCH_DTL_STS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_STS
            )

---- RENAME LAYER ----
,

RENAME_ISD as ( SELECT 
		  INDM_SCH_DTL_ID                                    as                                    INDM_SCH_DTL_ID
		, INDM_SCH_ID                                        as                                        INDM_SCH_ID
		, INDM_SCH_DTL_PRCS_DT                               as                               INDM_SCH_DTL_PRCS_DT
		, INDM_SCH_DLVR_DT                                   as                                   INDM_SCH_DLVR_DT
		, INDM_SCH_DTL_DRV_EFF_DT                            as                            INDM_SCH_DTL_DRV_EFF_DT
		, INDM_SCH_DTL_DRV_END_DT                            as                            INDM_SCH_DTL_DRV_END_DT
		, INDM_SCH_DTL_DRV_DD                                as                                INDM_SCH_DTL_DRV_DD
		, INDM_SCH_DTL_DRV_AMT                               as                               INDM_SCH_DTL_DRV_AMT
		, INDM_SCH_DTL_STS_TYP_CD                            as                            INDM_SCH_DTL_STS_TYP_CD
		, INDM_SCH_DTL_OFST_AMT                              as                              INDM_SCH_DTL_OFST_AMT
		, INDM_SCH_DTL_LMP_RED_AMT                           as                           INDM_SCH_DTL_LMP_RED_AMT
		, INDM_SCH_DTL_OTHR_RED_AMT                          as                          INDM_SCH_DTL_OTHR_RED_AMT
		, INDM_SCH_DTL_DRV_NET_AMT                           as                           INDM_SCH_DTL_DRV_NET_AMT
		, INDM_SCH_DTL_FNL_PAY_IND                           as                           INDM_SCH_DTL_FNL_PAY_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND
		, INDM_SCH_DTL_DSCNT_AMT                             as                             INDM_SCH_DTL_DSCNT_AMT 
				FROM     LOGIC_ISD   ), 
RENAME_STS as ( SELECT 
		  INDM_SCH_DTL_STS_TYP_NM                            as                            INDM_SCH_DTL_STS_TYP_NM
		, INDM_SCH_DTL_STS_TYP_CD                            as                        STS_INDM_SCH_DTL_STS_TYP_CD
		, VOID_IND                                           as                                       STS_VOID_IND 
				FROM     LOGIC_STS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ISD                            as ( SELECT * from    RENAME_ISD   ),
FILTER_STS                            as ( SELECT * from    RENAME_STS 
				WHERE STS_VOID_IND = 'N'  ),

---- JOIN LAYER ----

ISD as ( SELECT * 
				FROM  FILTER_ISD
				LEFT JOIN FILTER_STS ON  FILTER_ISD.INDM_SCH_DTL_STS_TYP_CD =  FILTER_STS.STS_INDM_SCH_DTL_STS_TYP_CD  )
SELECT * 
from ISD