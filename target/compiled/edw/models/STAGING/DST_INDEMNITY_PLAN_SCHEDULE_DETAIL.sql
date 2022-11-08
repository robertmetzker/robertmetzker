---- SRC LAYER ----
WITH
SRC_ISS            as ( SELECT *     FROM     STAGING.STG_INDEMNITY_SCHEDULE ),
SRC_IP             as ( SELECT *     FROM     STAGING.STG_INDEMNITY_PAYMENT ),
SRC_ISDA           as ( SELECT *     FROM     STAGING.STG_INDEMNITY_SCHEDULE_DETAIL_AMOUNT ),
SRC_ISD            as ( SELECT *     FROM     STAGING.STG_INDEMNITY_SCHEDULE_DETAIL ),
SRC_CFT            as ( SELECT *     FROM     STAGING.STG_CLAIM_FINANCIAL_TRANSACTION ),
//SRC_ISS            as ( SELECT *     FROM     STG_INDEMNITY_SCHEDULE) ,
//SRC_IP             as ( SELECT *     FROM     STG_INDEMNITY_PAYMENT) ,
//SRC_ISDA           as ( SELECT *     FROM     STG_INDEMNITY_SCHEDULE_DETAIL_AMOUNT) ,
//SRC_ISD            as ( SELECT *     FROM     STG_INDEMNITY_SCHEDULE_DETAIL) ,
//SRC_CFT            as ( SELECT *     FROM     STG_CLAIM_FINANCIAL_TRANSACTION) ,

---- LOGIC LAYER ----


LOGIC_ISS as ( SELECT 
		  TRIM( INDM_FREQ_TYP_CD )                           as                                   INDM_FREQ_TYP_CD 
		, TRIM( INDM_FREQ_TYP_NM )                           as                                   INDM_FREQ_TYP_NM 
		, INDM_SCH_AUTO_PAY_IND                              as                              INDM_SCH_AUTO_PAY_IND 
		, VOID_IND                                           as                                           VOID_IND 
		, INDM_PAY_ID                                        as                                        INDM_PAY_ID 
		, INDM_SCH_ID                                        as                                        INDM_SCH_ID 
		FROM SRC_ISS
            ),

LOGIC_IP as ( SELECT 
		  TRIM( INDM_RSN_TYP_CD )                            as                                    INDM_RSN_TYP_CD 
		, TRIM( INDM_RSN_TYP_NM )                            as                                    INDM_RSN_TYP_NM 
		, INDM_PAY_RECALC_IND                                as                                INDM_PAY_RECALC_IND 
		, VOID_IND                                           as                                           VOID_IND 
		, INDM_PAY_ID                                        as                                        INDM_PAY_ID 
		FROM SRC_IP
            ),

LOGIC_ISDA as ( SELECT 
		  TRIM( INDM_SCH_DTL_AMT_TYP_CD )                    as                            INDM_SCH_DTL_AMT_TYP_CD 
		, TRIM( INDM_SCH_DTL_AMT_TYP_NM )                    as                            INDM_SCH_DTL_AMT_TYP_NM 
		, TRIM( INDM_SCH_DTL_STS_TYP_CD )                    as                            INDM_SCH_DTL_STS_TYP_CD 
		, TRIM( INDM_SCH_DTL_STS_TYP_NM )                    as                            INDM_SCH_DTL_STS_TYP_NM 
		, INDM_SCH_DTL_AMT_PRI_IND                           as                           INDM_SCH_DTL_AMT_PRI_IND 
		, INDM_SCH_DTL_AMT_MAILTO_IND                        as                        INDM_SCH_DTL_AMT_MAILTO_IND 
		, INDM_SCH_DTL_AMT_RMND_IND                          as                          INDM_SCH_DTL_AMT_RMND_IND 
		, VOID_IND                                           as                                           VOID_IND 
		, INDM_SCH_DTL_ID                                    as                                    INDM_SCH_DTL_ID 
		, CFT_ID                                             as                                             CFT_ID 
		FROM SRC_ISDA
            ),

LOGIC_ISD as ( SELECT 
		  INDM_SCH_DTL_FNL_PAY_IND                           as                           INDM_SCH_DTL_FNL_PAY_IND 
		, VOID_IND                                           as                                           VOID_IND 
		, INDM_SCH_ID                                        as                                        INDM_SCH_ID 
		, INDM_SCH_DTL_ID                                    as                                    INDM_SCH_DTL_ID 
		FROM SRC_ISD
            ),

LOGIC_CFT as ( SELECT 
		  FNCL_TRAN_TYP_ID                                   as                                   FNCL_TRAN_TYP_ID 
		, CFT_DRV_BAL_AMT                                    as                                    CFT_DRV_BAL_AMT 
		, CFT_ID                                             as                                             CFT_ID 
		FROM SRC_CFT
            )

---- RENAME LAYER ----
,

RENAME_ISS        as ( SELECT 
		  INDM_FREQ_TYP_CD                                   as                                   INDM_FREQ_TYP_CD
		, INDM_FREQ_TYP_NM                                   as                                   INDM_FREQ_TYP_NM
		, INDM_SCH_AUTO_PAY_IND                              as                              INDM_SCH_AUTO_PAY_IND
		, VOID_IND                                           as                                       ISS_VOID_IND
		, INDM_PAY_ID                                        as                                    ISS_INDM_PAY_ID
		, INDM_SCH_ID                                        as                                        INDM_SCH_ID 
				FROM     LOGIC_ISS   ), 
RENAME_IP         as ( SELECT 
		  INDM_RSN_TYP_CD                                    as                                    INDM_RSN_TYP_CD
		, INDM_RSN_TYP_NM                                    as                                    INDM_RSN_TYP_NM
		, INDM_PAY_RECALC_IND                                as                                INDM_PAY_RECALC_IND
		, VOID_IND                                           as                                        IP_VOID_IND
		, INDM_PAY_ID                                        as                                        INDM_PAY_ID 
				FROM     LOGIC_IP   ), 
RENAME_ISDA       as ( SELECT 
		  INDM_SCH_DTL_AMT_TYP_CD                            as                            INDM_SCH_DTL_AMT_TYP_CD
		, INDM_SCH_DTL_AMT_TYP_NM                            as                            INDM_SCH_DTL_AMT_TYP_NM
		, INDM_SCH_DTL_STS_TYP_CD                            as                            INDM_SCH_DTL_STS_TYP_CD
		, INDM_SCH_DTL_STS_TYP_NM                            as                            INDM_SCH_DTL_STS_TYP_NM
		, INDM_SCH_DTL_AMT_PRI_IND                           as                           INDM_SCH_DTL_AMT_PRI_IND
		, INDM_SCH_DTL_AMT_MAILTO_IND                        as                        INDM_SCH_DTL_AMT_MAILTO_IND
		, INDM_SCH_DTL_AMT_RMND_IND                          as                          INDM_SCH_DTL_AMT_RMND_IND
		, VOID_IND                                           as                                      ISDA_VOID_IND
		, INDM_SCH_DTL_ID                                    as                               ISDA_INDM_SCH_DTL_ID
		, CFT_ID                                             as                                             CFT_ID 
				FROM     LOGIC_ISDA   ), 
RENAME_ISD        as ( SELECT 
		  INDM_SCH_DTL_FNL_PAY_IND                           as                           INDM_SCH_DTL_FNL_PAY_IND
		, VOID_IND                                           as                                       ISD_VOID_IND
		, INDM_SCH_ID                                        as                                    ISD_INDM_SCH_ID
		, INDM_SCH_DTL_ID                                    as                                    INDM_SCH_DTL_ID 
				FROM     LOGIC_ISD   ), 
RENAME_CFT        as ( SELECT 
		  FNCL_TRAN_TYP_ID                                   as                                   FNCL_TRAN_TYP_ID
		, CFT_DRV_BAL_AMT                                    as                                    CFT_DRV_BAL_AMT
		, CFT_ID                                             as                                         CFT_CFT_ID 
				FROM     LOGIC_CFT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_IP                             as ( SELECT * FROM    RENAME_IP   ),
FILTER_ISS                            as ( SELECT * FROM    RENAME_ISS   ),
FILTER_ISD                            as ( SELECT * FROM    RENAME_ISD   ),
FILTER_ISDA                           as ( SELECT * FROM    RENAME_ISDA   ),
FILTER_CFT                            as ( SELECT * FROM    RENAME_CFT 
                                            WHERE FNCL_TRAN_TYP_ID = 152 AND CFT_DRV_BAL_AMT < 0  ),

---- JOIN LAYER ----

ISDA as ( SELECT * 
				FROM  FILTER_ISDA
				LEFT JOIN FILTER_CFT ON  FILTER_ISDA.CFT_ID =  FILTER_CFT.CFT_CFT_ID  ),
ISD as ( SELECT * 
				FROM  FILTER_ISD
				LEFT JOIN ISDA ON  FILTER_ISD.INDM_SCH_DTL_ID = ISDA.ISDA_INDM_SCH_DTL_ID  ),
ISS as ( SELECT * 
				FROM  FILTER_ISS
				LEFT JOIN ISD ON  FILTER_ISS.INDM_SCH_ID = ISD.ISD_INDM_SCH_ID  ),
IP as ( SELECT * 
				FROM  FILTER_IP
				LEFT JOIN ISS ON  FILTER_IP.INDM_PAY_ID = ISS.ISS_INDM_PAY_ID  ),

---- ETL LAYER ----

ETL AS (
select DISTINCT
          NVL2(CFT_CFT_ID,'Y','N') as OVR_PYMNT_IND 
        , md5(cast(
    
    coalesce(cast(INDM_FREQ_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(INDM_RSN_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_DTL_AMT_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_DTL_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_DTL_FNL_PAY_IND as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_AUTO_PAY_IND as 
    varchar
), '') || '-' || coalesce(cast(INDM_PAY_RECALC_IND as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_DTL_AMT_PRI_IND as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_DTL_AMT_MAILTO_IND as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_DTL_AMT_RMND_IND as 
    varchar
), '') || '-' || coalesce(cast(OVR_PYMNT_IND as 
    varchar
), '') || '-' || coalesce(cast(IP_VOID_IND as 
    varchar
), '') || '-' || coalesce(cast(ISS_VOID_IND as 
    varchar
), '') || '-' || coalesce(cast(ISD_VOID_IND as 
    varchar
), '') || '-' || coalesce(cast(ISDA_VOID_IND as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
		, INDM_FREQ_TYP_CD
		, INDM_FREQ_TYP_NM
		, INDM_RSN_TYP_CD
		, INDM_RSN_TYP_NM
		, INDM_SCH_DTL_AMT_TYP_CD
		, INDM_SCH_DTL_AMT_TYP_NM
		, INDM_SCH_DTL_STS_TYP_CD
		, INDM_SCH_DTL_STS_TYP_NM
		, INDM_SCH_DTL_FNL_PAY_IND
		, INDM_SCH_AUTO_PAY_IND
		, INDM_PAY_RECALC_IND
		, INDM_SCH_DTL_AMT_PRI_IND
		, INDM_SCH_DTL_AMT_MAILTO_IND
		, INDM_SCH_DTL_AMT_RMND_IND
		, IP_VOID_IND
		, ISS_VOID_IND
		, ISD_VOID_IND
		, ISDA_VOID_IND
 from IP )

SELECT 
          UNIQUE_ID_KEY
		, INDM_FREQ_TYP_CD
		, INDM_FREQ_TYP_NM
		, INDM_RSN_TYP_CD
		, INDM_RSN_TYP_NM
		, INDM_SCH_DTL_AMT_TYP_CD
		, INDM_SCH_DTL_AMT_TYP_NM
		, INDM_SCH_DTL_STS_TYP_CD
		, INDM_SCH_DTL_STS_TYP_NM
		, INDM_SCH_DTL_FNL_PAY_IND
		, INDM_SCH_AUTO_PAY_IND
		, INDM_PAY_RECALC_IND
		, INDM_SCH_DTL_AMT_PRI_IND
		, INDM_SCH_DTL_AMT_MAILTO_IND
		, INDM_SCH_DTL_AMT_RMND_IND
		, OVR_PYMNT_IND
		, IP_VOID_IND
		, ISS_VOID_IND
		, ISD_VOID_IND
		, ISDA_VOID_IND
 FROM ETL