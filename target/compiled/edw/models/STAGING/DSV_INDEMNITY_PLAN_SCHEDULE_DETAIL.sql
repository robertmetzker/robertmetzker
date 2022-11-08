

---- SRC LAYER ----
WITH
SRC_IPSD           as ( SELECT *     FROM     STAGING.DST_INDEMNITY_PLAN_SCHEDULE_DETAIL ),
//SRC_IPSD           as ( SELECT *     FROM     DST_INDEMNITY_PLAN_SCHEDULE_DETAIL) ,

---- LOGIC LAYER ----


LOGIC_IPSD as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, INDM_FREQ_TYP_CD                                   as                                   INDM_FREQ_TYP_CD 
		, INDM_RSN_TYP_CD                                    as                                    INDM_RSN_TYP_CD 
		, INDM_SCH_DTL_AMT_TYP_CD                            as                            INDM_SCH_DTL_AMT_TYP_CD 
		, INDM_SCH_DTL_STS_TYP_CD                            as                            INDM_SCH_DTL_STS_TYP_CD 
		, INDM_SCH_DTL_FNL_PAY_IND                           as                           INDM_SCH_DTL_FNL_PAY_IND 
		, INDM_SCH_AUTO_PAY_IND                              as                              INDM_SCH_AUTO_PAY_IND 
		, INDM_PAY_RECALC_IND                                as                                INDM_PAY_RECALC_IND 
		, INDM_SCH_DTL_AMT_PRI_IND                           as                           INDM_SCH_DTL_AMT_PRI_IND 
		, INDM_SCH_DTL_AMT_MAILTO_IND                        as                        INDM_SCH_DTL_AMT_MAILTO_IND 
		, INDM_SCH_DTL_AMT_RMND_IND                          as                          INDM_SCH_DTL_AMT_RMND_IND 
		, OVR_PYMNT_IND                                      as                                      OVR_PYMNT_IND 
		, IP_VOID_IND                                        as                                        IP_VOID_IND 
		, ISS_VOID_IND                                       as                                       ISS_VOID_IND 
		, ISD_VOID_IND                                       as                                       ISD_VOID_IND 
		, ISDA_VOID_IND                                      as                                      ISDA_VOID_IND 
		, INDM_FREQ_TYP_NM                                   as                                   INDM_FREQ_TYP_NM 
		, INDM_RSN_TYP_NM                                    as                                    INDM_RSN_TYP_NM 
		, INDM_SCH_DTL_STS_TYP_NM                            as                            INDM_SCH_DTL_STS_TYP_NM 
		, INDM_SCH_DTL_AMT_TYP_NM                            as                            INDM_SCH_DTL_AMT_TYP_NM 
		FROM SRC_IPSD
            )

---- RENAME LAYER ----
,

RENAME_IPSD       as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, INDM_FREQ_TYP_CD                                   as                                FREQUENCY_TYPE_CODE
		, INDM_RSN_TYP_CD                                    as                         INDEMNITY_REASON_TYPE_CODE
		, INDM_SCH_DTL_AMT_TYP_CD                            as                   SCHEDULE_DETAIL_AMOUNT_TYPE_CODE
		, INDM_SCH_DTL_STS_TYP_CD                            as           SCHEDULE_PAYMENT_DETAIL_STATUS_TYPE_CODE
		, INDM_SCH_DTL_FNL_PAY_IND                           as                                  FINAL_PAYMENT_IND
		, INDM_SCH_AUTO_PAY_IND                              as                              SCHEDULE_AUTO_PAY_IND
		, INDM_PAY_RECALC_IND                                as                        INDEMNITY_RECALCULATION_IND
		, INDM_SCH_DTL_AMT_PRI_IND                           as                                  PRIMARY_PAYEE_IND
		, INDM_SCH_DTL_AMT_MAILTO_IND                        as                                 PAYMENT_MAILTO_IND
		, INDM_SCH_DTL_AMT_RMND_IND                          as               SCHEDULE_DETAIL_AMOUNT_REMAINING_IND
		, OVR_PYMNT_IND                                      as                            OVERPAYMENT_BALANCE_IND
		, IP_VOID_IND                                        as                                      PLAN_VOID_IND
		, ISS_VOID_IND                                       as                                  SCHEDULE_VOID_IND
		, ISD_VOID_IND                                       as                           SCHEDULE_DETAIL_VOID_IND
		, ISDA_VOID_IND                                      as                   SCHEDULE_PAYMENT_DETAIL_VOID_IND
		, INDM_FREQ_TYP_NM                                   as                                FREQUENCY_TYPE_DESC
		, INDM_RSN_TYP_NM                                    as                         INDEMNITY_REASON_TYPE_DESC
		, INDM_SCH_DTL_STS_TYP_NM                            as           SCHEDULE_PAYMENT_DETAIL_STATUS_TYPE_DESC
		, INDM_SCH_DTL_AMT_TYP_NM                            as                   SCHEDULE_DETAIL_AMOUNT_TYPE_DESC 
				FROM     LOGIC_IPSD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_IPSD                           as ( SELECT * FROM    RENAME_IPSD   ),

---- JOIN LAYER ----

 JOIN_IPSD        as  ( SELECT * 
				FROM  FILTER_IPSD )
 SELECT * FROM  JOIN_IPSD