---- SRC LAYER ----
WITH
SRC_DPC as ( SELECT *     from     STAGING.STG_DETAIL_PAYMENT_CODING ),
SRC_CL as ( SELECT *     from     STAGING.STG_CLAIM ),
 SRC_DPC_CNCLD as ( SELECT *     from     STAGING.STG_DETAIL_PAYMENT_CODING ),
//SRC_DPC as ( SELECT *     from     STG_DETAIL_PAYMENT_CODING) ,
//SRC_CL as ( SELECT *     from     STG_CLAIM) ,
 //SRC_DPC_CNCLD as ( SELECT *     from STG_DETAIL_PAYMENT_CODING    ) ,

---- LOGIC LAYER ----

LOGIC_DPC as ( SELECT 
		  TRIM( CLAIM_NO )                                   as                                           CLAIM_NO 
		, WRNT_DATE                                          as                                          WRNT_DATE 
		, PYMNT_CODE_AMT                                     as                                     PYMNT_CODE_AMT 
		, INTRS_AMT                                          as                                          INTRS_AMT 
		, TRIM( WRQ_STS_CODE_DESC )                          as                                  WRQ_STS_CODE_DESC 
		, TRIM( ACNTB_CODE )                                 as                                         ACNTB_CODE 
		, TRIM( ACNTB_DESC )                                 as                                         ACNTB_DESC 
		, TRIM( BILL_TYPE_F2 )                               as                                       BILL_TYPE_F2 
		, TRIM( BILL_TYPE_F2_DESC )                          as                                  BILL_TYPE_F2_DESC 
		, TRIM( BILL_TYPE_L3 )                               as                                       BILL_TYPE_L3 
		, TRIM( BILL_TYPE_L3_DESC )                          as                                  BILL_TYPE_L3_DESC 
		, TRIM( ORGNL_SYSTM_CODE )                           as                                   ORGNL_SYSTM_CODE 
		, TRIM( ORGNL_SYSTM_DESC )                           as                                   ORGNL_SYSTM_DESC 
		, TRIM( CHECK_NO )                                   as                                           CHECK_NO 
		, TRIM( TCN_NO )                                     as                                             TCN_NO 
		, TRIM( ACDNT_TYPE )                                 as                                         ACDNT_TYPE 
		, TRIM( PYMNT_FUND_TYPE )                            as                                    PYMNT_FUND_TYPE 
		, TRIM( CVRG_TYPE )                                  as                                          CVRG_TYPE 
		, DW_CNTRL_DATE                                      as                                      DW_CNTRL_DATE 
		, WRQ_STS_DATE                                       as                                       WRQ_STS_DATE 

				from SRC_DPC
            ),

LOGIC_CL as ( SELECT 
		  TRIM( CLM_NO )                                     as                                             CLM_NO 
         ,CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND
             from SRC_CL
            ),

LOGIC_DPC_CNCLD as ( SELECT 
		  TRIM( CLAIM_NO )                                   as                                           CLAIM_NO 
		, WRQ_STS_DATE                                       as                                       WRQ_STS_DATE 
		, PYMNT_CODE_AMT                                     as                                     PYMNT_CODE_AMT 
		, INTRS_AMT                                          as                                          INTRS_AMT 
		, TRIM( WRQ_STS_CODE_DESC )                          as                                  WRQ_STS_CODE_DESC 
		, TRIM( ACNTB_CODE )                                 as                                         ACNTB_CODE 
		, TRIM( ACNTB_DESC )                                 as                                         ACNTB_DESC 
		, TRIM( BILL_TYPE_F2 )                               as                                       BILL_TYPE_F2 
		, TRIM( BILL_TYPE_F2_DESC )                          as                                  BILL_TYPE_F2_DESC 
		, TRIM( BILL_TYPE_L3 )                               as                                       BILL_TYPE_L3 
		, TRIM( BILL_TYPE_L3_DESC )                          as                                  BILL_TYPE_L3_DESC 
		, TRIM( ORGNL_SYSTM_CODE )                           as                                   ORGNL_SYSTM_CODE 
		, TRIM( ORGNL_SYSTM_DESC )                           as                                   ORGNL_SYSTM_DESC 
		, TRIM( CHECK_NO )                                   as                                           CHECK_NO 
		, TRIM( TCN_NO )                                     as                                             TCN_NO 
		, TRIM( ACDNT_TYPE )                                 as                                         ACDNT_TYPE 
		, TRIM( PYMNT_FUND_TYPE )                            as                                    PYMNT_FUND_TYPE 
		, TRIM( CVRG_TYPE )                                  as                                          CVRG_TYPE 
		, DW_CNTRL_DATE                                      as                                      DW_CNTRL_DATE 
				from SRC_DPC_CNCLD
            )


---- RENAME LAYER ----
,

RENAME_DPC as ( SELECT 
		  CLAIM_NO                                           as                                           CLAIM_NO
		, WRNT_DATE                                          as                                          WRNT_DATE
		, WRNT_DATE                                          as                                    WRNT_CLNDR_YEAR
		, WRNT_DATE                                          as                                   WRNT_CLNDR_MONTH
		, PYMNT_CODE_AMT                                     as                                     PYMNT_CODE_AMT
		, INTRS_AMT                                          as                                          INTRS_AMT
		, WRQ_STS_CODE_DESC                                  as                                  WRQ_STS_CODE_DESC
		, ACNTB_CODE                                         as                                         ACNTB_CODE
		, ACNTB_DESC                                         as                                         ACNTB_DESC
		, BILL_TYPE_F2                                       as                                       BILL_TYPE_F2
		, BILL_TYPE_F2_DESC                                  as                                  BILL_TYPE_F2_DESC
		, BILL_TYPE_L3                                       as                                       BILL_TYPE_L3
		, BILL_TYPE_L3_DESC                                  as                                  BILL_TYPE_L3_DESC
		, ORGNL_SYSTM_CODE                                   as                                   ORGNL_SYSTM_CODE
		, ORGNL_SYSTM_DESC                                   as                                   ORGNL_SYSTM_DESC
		, CHECK_NO                                           as                                           CHECK_NO
		, TCN_NO                                             as                                             TCN_NO
		, ACDNT_TYPE                                         as                                         ACDNT_TYPE
		, PYMNT_FUND_TYPE                                    as                                    PYMNT_FUND_TYPE
		, CVRG_TYPE                                          as                                          CVRG_TYPE
		, DW_CNTRL_DATE                                      as                                   ETL_PROCESS_DATE
		, WRQ_STS_DATE                                       as                                       WRQ_STS_DATE 
						FROM     LOGIC_DPC   ) ,

RENAME_CL as ( SELECT 
		  CLM_NO                                             as                                         CLM_CLM_NO 
         ,CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND     

              FROM     LOGIC_CL   )
, 
RENAME_DPC_CNCLD as ( SELECT 
		  CLAIM_NO                                           as                                           CLAIM_NO
		, WRQ_STS_DATE                                       as                                          WRNT_DATE
		, WRQ_STS_DATE                                       as                                    WRNT_CLNDR_YEAR
		, WRQ_STS_DATE                                       as                                   WRNT_CLNDR_MONTH
		, PYMNT_CODE_AMT                                     as                                     PYMNT_CODE_AMT
		, INTRS_AMT                                          as                                          INTRS_AMT
		, WRQ_STS_CODE_DESC                                  as                                  WRQ_STS_CODE_DESC
		, ACNTB_CODE                                         as                                         ACNTB_CODE
		, ACNTB_DESC                                         as                                         ACNTB_DESC
		, BILL_TYPE_F2                                       as                                       BILL_TYPE_F2
		, BILL_TYPE_F2_DESC                                  as                                  BILL_TYPE_F2_DESC
		, BILL_TYPE_L3                                       as                                       BILL_TYPE_L3
		, BILL_TYPE_L3_DESC                                  as                                  BILL_TYPE_L3_DESC
		, ORGNL_SYSTM_CODE                                   as                                   ORGNL_SYSTM_CODE
		, ORGNL_SYSTM_DESC                                   as                                   ORGNL_SYSTM_DESC
		, CHECK_NO                                           as                                           CHECK_NO
		, TCN_NO                                             as                                             TCN_NO
		, ACDNT_TYPE                                         as                                         ACDNT_TYPE
		, PYMNT_FUND_TYPE                                    as                                    PYMNT_FUND_TYPE
		, CVRG_TYPE                                          as                                          CVRG_TYPE
		, DW_CNTRL_DATE                                      as                                   ETL_PROCESS_DATE 

						FROM     LOGIC_DPC_CNCLD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DPC                            as ( SELECT * from    RENAME_DPC   ),
FILTER_CL                             as ( SELECT * from    RENAME_CL 
                                            WHERE CLM_REL_SNPSHT_IND ='N'  ),
FILTER_DPC_CNCLD                      as ( SELECT * from    RENAME_DPC_CNCLD 
                                            WHERE WRQ_STS_CODE_DESC IN ('VOIDED','CANCELLED')  AND PYMNT_CODE_AMT <> 0  ),
---- JOIN LAYER ----

CLAIM_NO as ( SELECT * 
				FROM  FILTER_DPC
				INNER JOIN FILTER_CL ON  FILTER_DPC.CLAIM_NO =  FILTER_CL.CLM_CLM_NO  
            ),
---UNION LAYER --------
Payment_UNION AS (
SELECT 
 CLAIM_NO
, WRNT_DATE
, YEAR(WRNT_CLNDR_YEAR) AS WRNT_CLNDR_YEAR
, MONTH(WRNT_CLNDR_MONTH) AS WRNT_CLNDR_MONTH
, CAST(PYMNT_CODE_AMT as DECIMAL(32,2)) AS PYMNT_CODE_AMT
, CAST(INTRS_AMT as DECIMAL(32,2)) AS INTRS_AMT
, CASE WHEN 
BILL_TYPE_F2 IN ('00', '01', '02', '03', '04', '05', '06', '07', '08', '09') THEN 'MDCL_HSP_AMT'
WHEN BILL_TYPE_F2 IN ('10', '11', '12', '13', '14', '15', '16', '17', '18', '19') THEN 'MDCL_CLN_AMT'
WHEN BILL_TYPE_F2 IN ('20', '21', '22', '23', '24', '25', '26', '27', '28', '29') THEN 'MDCL_DR_AMT'
WHEN BILL_TYPE_F2 IN ('30', '31', '32', '33', '34', '35', '36', '37', '38', '39') THEN 'MDCL_NRS_AMT'
WHEN BILL_TYPE_F2 IN ('40', '41', '42', '43', '44', '45', '46', '47', '48', '49') THEN 'MDCL_DRG_AMT'
WHEN BILL_TYPE_F2 IN ('50', '51', '52', '53', '54', '55', '56', '57', '58', '59') THEN 'MDCL_XRY_AMT'
WHEN BILL_TYPE_F2 IN ('60', '61', '62', '63', '64', '65', '66', '67', '68', '69') THEN 'MDCL_LAB_AMT'
WHEN BILL_TYPE_F2 IN ('70', '71', '72', '73', '74', '75', '76', '77', '78', '79') THEN 'MDCL_MSC_AMT'
WHEN BILL_TYPE_F2 IN ('80', '81', '82', '83', '84','86', '87', '88', '89', '91','92', '93') THEN 'MDCL_OTH_AMT' 
WHEN BILL_TYPE_F2  ='90'  THEN 'MDCL_FNR_AMT' 
WHEN BILL_TYPE_F2  ='94'  THEN 'MDCL_CRT_AMT'
WHEN ACNTB_CODE    ='09'  THEN 'INDM_DWR_AMT'
WHEN BILL_TYPE_F2  ='85'  THEN 'INDM_FCD_AMT'
WHEN BILL_TYPE_F2  ='97'  THEN 'INDM_LSP_AMT'
WHEN (BILL_TYPE_F2  ='98' AND BILL_TYPE_L3 <> '125' ) THEN 'INDM_LSA_AMT'
WHEN BILL_TYPE_L3  ='100' THEN 'INDM_PTD_AMT'
WHEN BILL_TYPE_L3  ='101' THEN 'INDM_TTP_AMT'
WHEN BILL_TYPE_L3  ='102' THEN 'INDM_TPP_AMT'
WHEN BILL_TYPE_L3  ='105' THEN 'INDM_PPP_AMT'
WHEN BILL_TYPE_L3  ='106' THEN 'INDM_PCT_AMT'
WHEN BILL_TYPE_L3 IN ('107', '108', '109') THEN 'INDM_DBN_AMT'
WHEN BILL_TYPE_L3  ='113' THEN 'INDM_LMR_AMT'
WHEN BILL_TYPE_L3  ='130' THEN 'INDM_WLT_AMT'
WHEN BILL_TYPE_L3  ='131' THEN 'INDM_LMW_AMT'
WHEN BILL_TYPE_L3  ='125' THEN 'ATRNY_FEE_AMT'
WHEN BILL_TYPE_L3  ='126' THEN 'CNTRCT_DOC_AMT'
WHEN BILL_TYPE_L3 in ('150','160','527','950') THEN 'UNDEFINED' 
ELSE 'INDM_OIN_AMT' 
END AS PYMNT_CODE_CTGRY
, WRQ_STS_CODE_DESC
, ACNTB_CODE
, ACNTB_DESC
, BILL_TYPE_F2
, BILL_TYPE_F2_DESC
, BILL_TYPE_L3
, BILL_TYPE_L3_DESC
, ORGNL_SYSTM_CODE
, ORGNL_SYSTM_DESC
, CHECK_NO
, TCN_NO
, ACDNT_TYPE
, PYMNT_FUND_TYPE
, CVRG_TYPE
, ETL_PROCESS_DATE

			FROM CLAIM_NO
			UNION ALL
SELECT 
CLAIM_NO
, WRQ_STS_DATE AS WRNT_DATE
, YEAR(WRQ_STS_DATE) AS WRNT_CLNDR_YEAR
, MONTH(WRQ_STS_DATE) AS WRNT_CLNDR_MONTH
, 0-CAST(PYMNT_CODE_AMT as DECIMAL(32,2)) AS PYMNT_CODE_AMT
, 0-CAST(INTRS_AMT as DECIMAL(32,2)) AS INTRS_AMT
, CASE WHEN 
BILL_TYPE_F2 IN ('00', '01', '02', '03', '04', '05', '06', '07', '08', '09') THEN 'MDCL_HSP_AMT'
WHEN BILL_TYPE_F2 IN ('10', '11', '12', '13', '14', '15', '16', '17', '18', '19') THEN 'MDCL_CLN_AMT'
WHEN BILL_TYPE_F2 IN ('20', '21', '22', '23', '24', '25', '26', '27', '28', '29') THEN 'MDCL_DR_AMT'
WHEN BILL_TYPE_F2 IN ('30', '31', '32', '33', '34', '35', '36', '37', '38', '39') THEN 'MDCL_NRS_AMT'
WHEN BILL_TYPE_F2 IN ('40', '41', '42', '43', '44', '45', '46', '47', '48', '49') THEN 'MDCL_DRG_AMT'
WHEN BILL_TYPE_F2 IN ('50', '51', '52', '53', '54', '55', '56', '57', '58', '59') THEN 'MDCL_XRY_AMT'
WHEN BILL_TYPE_F2 IN ('60', '61', '62', '63', '64', '65', '66', '67', '68', '69') THEN 'MDCL_LAB_AMT'
WHEN BILL_TYPE_F2 IN ('70', '71', '72', '73', '74', '75', '76', '77', '78', '79') THEN 'MDCL_MSC_AMT'
WHEN BILL_TYPE_F2 IN ('80', '81', '82', '83', '84','86', '87', '88', '89', '91','92', '93') THEN 'MDCL_OTH_AMT' 
WHEN BILL_TYPE_F2  ='90'  THEN 'MDCL_FNR_AMT' 
WHEN BILL_TYPE_F2  ='94'  THEN 'MDCL_CRT_AMT'
WHEN ACNTB_CODE    ='09'  THEN 'INDM_DWR_AMT'
WHEN BILL_TYPE_F2  ='85'  THEN 'INDM_FCD_AMT'
WHEN BILL_TYPE_F2  ='97'  THEN 'INDM_LSP_AMT'
WHEN (BILL_TYPE_F2  ='98' AND BILL_TYPE_L3 <> '125' ) THEN 'INDM_LSA_AMT'
WHEN BILL_TYPE_L3  ='100' THEN 'INDM_PTD_AMT'
WHEN BILL_TYPE_L3  ='101' THEN 'INDM_TTP_AMT'
WHEN BILL_TYPE_L3  ='102' THEN 'INDM_TPP_AMT'
WHEN BILL_TYPE_L3  ='105' THEN 'INDM_PPP_AMT'
WHEN BILL_TYPE_L3  ='106' THEN 'INDM_PCT_AMT'
WHEN BILL_TYPE_L3 IN ('107', '108', '109') THEN 'INDM_DBN_AMT'
WHEN BILL_TYPE_L3  ='113' THEN 'INDM_LMR_AMT'
WHEN BILL_TYPE_L3  ='130' THEN 'INDM_WLT_AMT'
WHEN BILL_TYPE_L3  ='131' THEN 'INDM_LMW_AMT'
WHEN BILL_TYPE_L3  ='125' THEN 'ATRNY_FEE_AMT'
WHEN BILL_TYPE_L3  ='126' THEN 'CNTRCT_DOC_AMT'
WHEN BILL_TYPE_L3 in ('150','160','527','950') THEN 'UNDEFINED' 
ELSE 'INDM_OIN_AMT' 
END AS PYMNT_CODE_CTGRY
, WRQ_STS_CODE_DESC
, ACNTB_CODE
, ACNTB_DESC
, BILL_TYPE_F2
, BILL_TYPE_F2_DESC
, BILL_TYPE_L3
, BILL_TYPE_L3_DESC
, ORGNL_SYSTM_CODE
, ORGNL_SYSTM_DESC
, CHECK_NO
, TCN_NO
, ACDNT_TYPE
, PYMNT_FUND_TYPE
, CVRG_TYPE
, ETL_PROCESS_DATE   
FROM CLAIM_NO as DPC_CNCLD 
WHERE WRQ_STS_CODE_DESC IN ('VOIDED','CANCELLED')  AND PYMNT_CODE_AMT <> 0 )

select 
md5(cast(
    
    coalesce(cast(CLAIM_NO as 
    varchar
), '') || '-' || coalesce(cast(TCN_NO as 
    varchar
), '') || '-' || coalesce(cast(CHECK_NO as 
    varchar
), '') || '-' || coalesce(cast(WRNT_DATE as 
    varchar
), '') || '-' || coalesce(cast(ACNTB_CODE as 
    varchar
), '') || '-' || coalesce(cast(BILL_TYPE_F2 as 
    varchar
), '') || '-' || coalesce(cast(BILL_TYPE_L3 as 
    varchar
), '') || '-' || coalesce(cast(ACDNT_TYPE as 
    varchar
), '') || '-' || coalesce(cast(PYMNT_CODE_AMT as 
    varchar
), '')

 as 
    varchar
))  as UNIQUE_ID_KEY
, CLAIM_NO
, WRNT_DATE
, WRNT_CLNDR_YEAR
, WRNT_CLNDR_MONTH
, PYMNT_CODE_AMT
, INTRS_AMT
, PYMNT_CODE_CTGRY
, WRQ_STS_CODE_DESC
, ACNTB_CODE
, ACNTB_DESC
, BILL_TYPE_F2
, BILL_TYPE_F2_DESC
, BILL_TYPE_L3
, BILL_TYPE_L3_DESC
, ORGNL_SYSTM_CODE
, ORGNL_SYSTM_DESC
, CHECK_NO
, TCN_NO
, ACDNT_TYPE
, PYMNT_FUND_TYPE
, CVRG_TYPE
, ETL_PROCESS_DATE

 from  Payment_UNION