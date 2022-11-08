---- SRC LAYER ----
WITH
SRC_ISDH as ( SELECT *     from     DEV_VIEWS.PCMP.INDEMNITY_SCHEDULE_DETAIL_HIST ),
SRC_ISDST as ( SELECT *     from     DEV_VIEWS.PCMP.INDEMNITY_SCH_DTL_STS_TYP ),
SRC_LPRT as ( SELECT *     from     DEV_VIEWS.PCMP.LATE_PAYMENT_REASON_TYPE ),
//SRC_ISDH           as ( SELECT *     FROM     INDEMNITY_SCHEDULE_DETAIL_HIST) ,
//SRC_ISDST          as ( SELECT *     FROM     INDEMNITY_SCH_DTL_STS_TYP) ,
//SRC_LPRT           as ( SELECT *     FROM     LATE_PAYMENT_REASON_TYPE) ,

---- LOGIC LAYER ----

LOGIC_ISDH as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID 
		, INDM_SCH_DTL_ID                                    as                                    INDM_SCH_DTL_ID 
		, INDM_SCH_ID                                        as                                        INDM_SCH_ID 
		, cast( INDM_SCH_DTL_PRCS_DT as DATE )               as                               INDM_SCH_DTL_PRCS_DT 
		, cast( INDM_SCH_DLVR_DT as DATE )                   as                                   INDM_SCH_DLVR_DT 
		, cast( INDM_SCH_DTL_DRV_EFF_DT as DATE )            as                            INDM_SCH_DTL_DRV_EFF_DT 
		, cast( INDM_SCH_DTL_DRV_END_DT as DATE )            as                            INDM_SCH_DTL_DRV_END_DT 
		, INDM_SCH_DTL_DRV_DD                                as                                INDM_SCH_DTL_DRV_DD 
		, INDM_SCH_DTL_DRV_AMT                               as                               INDM_SCH_DTL_DRV_AMT 
		, upper( TRIM( INDM_SCH_DTL_STS_TYP_CD ) )           as                            INDM_SCH_DTL_STS_TYP_CD 
		, INDM_SCH_DTL_OFST_AMT                              as                              INDM_SCH_DTL_OFST_AMT 
		, INDM_SCH_DTL_LMP_RED_AMT                           as                           INDM_SCH_DTL_LMP_RED_AMT 
		, INDM_SCH_DTL_OTHR_RED_AMT                          as                          INDM_SCH_DTL_OTHR_RED_AMT 
		, INDM_SCH_DTL_DRV_NET_AMT                           as                           INDM_SCH_DTL_DRV_NET_AMT 
		, upper( INDM_SCH_DTL_FNL_PAY_IND )                  as                           INDM_SCH_DTL_FNL_PAY_IND 
		, upper( TRIM( LTE_PAY_RSN_TYP_CD ) )                as                                 LTE_PAY_RSN_TYP_CD 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       as                                       HIST_END_DTM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		, INDM_SCH_DTL_DSCNT_AMT                             as                             INDM_SCH_DTL_DSCNT_AMT 
		FROM SRC_ISDH
            ),

LOGIC_ISDST as ( SELECT 
		  upper( TRIM( INDM_SCH_DTL_STS_TYP_NM ) )           as                            INDM_SCH_DTL_STS_TYP_NM 
		, upper( TRIM( INDM_SCH_DTL_STS_TYP_CD ) )           as                            INDM_SCH_DTL_STS_TYP_CD 
		FROM SRC_ISDST
            ),

LOGIC_LPRT as ( SELECT 
		  upper( TRIM( LTE_PAY_RSN_TYP_NM ) )                as                                 LTE_PAY_RSN_TYP_NM 
		, upper( TRIM( LTE_PAY_RSN_TYP_CD ) )                as                                 LTE_PAY_RSN_TYP_CD 
		FROM SRC_LPRT
            )

---- RENAME LAYER ----
,

RENAME_ISDH       as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID
		, INDM_SCH_DTL_ID                                    as                                    INDM_SCH_DTL_ID
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
		, LTE_PAY_RSN_TYP_CD                                 as                                 LTE_PAY_RSN_TYP_CD
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM
		, HIST_END_DTM                                       as                                       HIST_END_DTM
		, VOID_IND                                           as                                           VOID_IND
		, INDM_SCH_DTL_DSCNT_AMT                             as                             INDM_SCH_DTL_DSCNT_AMT 
				FROM     LOGIC_ISDH   ), 
RENAME_ISDST      as ( SELECT 
		  INDM_SCH_DTL_STS_TYP_NM                            as                            INDM_SCH_DTL_STS_TYP_NM
		, INDM_SCH_DTL_STS_TYP_CD                            as                      ISDST_INDM_SCH_DTL_STS_TYP_CD 
				FROM     LOGIC_ISDST   ), 
RENAME_LPRT       as ( SELECT 
		  LTE_PAY_RSN_TYP_NM                                 as                                 LTE_PAY_RSN_TYP_NM
		, LTE_PAY_RSN_TYP_CD                                 as                            LPRT_LTE_PAY_RSN_TYP_CD 
				FROM     LOGIC_LPRT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ISDH                           as ( SELECT * FROM    RENAME_ISDH   ),
FILTER_ISDST                          as ( SELECT * FROM    RENAME_ISDST   ),
FILTER_LPRT                           as ( SELECT * FROM    RENAME_LPRT   ),

---- JOIN LAYER ----

ISDH as ( SELECT * 
				FROM  FILTER_ISDH
				LEFT JOIN FILTER_ISDST ON  FILTER_ISDH.INDM_SCH_DTL_STS_TYP_CD =  FILTER_ISDST.ISDST_INDM_SCH_DTL_STS_TYP_CD 
				LEFT JOIN FILTER_LPRT ON  FILTER_ISDH.LTE_PAY_RSN_TYP_CD =  FILTER_LPRT.LPRT_LTE_PAY_RSN_TYP_CD  )
SELECT * 
FROM ISDH