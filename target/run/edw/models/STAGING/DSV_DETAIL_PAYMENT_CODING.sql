
  create or replace  view DEV_EDW.STAGING.DSV_DETAIL_PAYMENT_CODING  as (
    

---- SRC LAYER ----
WITH
SRC_DPC as ( SELECT *     from     STAGING.DST_DETAIL_PAYMENT_CODING ),
//SRC_DPC as ( SELECT *     from     DST_DETAIL_PAYMENT_CODING) ,

---- LOGIC LAYER ----

LOGIC_DPC as ( SELECT 
		  CHECK_NO                                           as                                           CHECK_NO 
		, WRNT_NO                                            as                                            WRNT_NO 
		, TCN_NO                                             as                                             TCN_NO 
		, PAYEE_NAME                                         as                                         PAYEE_NAME 
		, WRNT_DATE                                          as                                          WRNT_DATE 
		, PRVDR_NO                                           as                                           PRVDR_NO 
		, CLAIM_NO                                           as                                           CLAIM_NO 
		, PLCY_NO                                            as                                            PLCY_NO 
		, BSNS_SQNC_NO                                       as                                       BSNS_SQNC_NO 
		, EIN_NO                                             as                                             EIN_NO 
		, REMIT_ADVC_NO                                      as                                      REMIT_ADVC_NO 
		, WRNT_AMT                                           as                                           WRNT_AMT 
		, PYMNT_CODE_AMT                                     as                                     PYMNT_CODE_AMT 
		, ACNTB_CODE                                         as                                         ACNTB_CODE 
		, PYMNT_FUND_TYPE                                    as                                    PYMNT_FUND_TYPE 
		, CVRG_TYPE                                          as                                          CVRG_TYPE 
		, BILL_TYPE_F2                                       as                                       BILL_TYPE_F2 
		, BILL_TYPE_L3                                       as                                       BILL_TYPE_L3 
		, ACDNT_TYPE                                         as                                         ACDNT_TYPE 
		, STS_CODE                                           as                                           STS_CODE 
		, CASE WHEN BILL_TYPE_F2 IN ('00', '01', '02', '03', '04', '05', '06', '07', '08', '09') THEN 'MEDICAL HOSPITAL'
     		   WHEN BILL_TYPE_F2 IN ('10', '11', '12', '13', '14', '15', '16', '17', '18', '19') THEN 'MEDICAL CLINIC'
               WHEN BILL_TYPE_F2 IN ('20', '21', '22', '23', '24', '25', '26', '27', '28', '29') THEN 'MEDICAL DOCTOR'
               WHEN BILL_TYPE_F2 IN ('30', '31', '32', '33', '34', '35', '36', '37', '38', '39') THEN 'MEDICAL NURSING SERVICES'
               WHEN BILL_TYPE_F2 IN ('40', '41', '42', '43', '44', '45', '46', '47', '48', '49') THEN 'MEDICAL DRUGS PHARMACY'
               WHEN BILL_TYPE_F2 IN ('50', '51', '52', '53', '54', '55', '56', '57', '58', '59') THEN 'MEDICAL XRAY RADIOLOGY'
               WHEN BILL_TYPE_F2 IN ('60', '61', '62', '63', '64', '65', '66', '67', '68', '69') THEN 'MEDICAL LAB PATHOLOGY'
               WHEN BILL_TYPE_F2 IN ('70', '71', '72', '73', '74', '75', '76', '77', '78', '79') THEN 'MEDICAL MISC'
               WHEN BILL_TYPE_F2 IN ('80', '81', '82', '83', '84','86', '87', '88', '89', '91','92', '93') THEN 'MEDICAL OTHER' 
               WHEN BILL_TYPE_F2  ='90'  THEN 'MEDICAL FUNERAL' 
               WHEN BILL_TYPE_F2  ='94'  THEN 'MEDICAL COURT'
               WHEN ACNTB_CODE    ='09'  THEN 'INDEMNITY DWRF'
               WHEN BILL_TYPE_F2  ='85'  THEN 'INDEMNITY FACIAL DISFIGUREMENT'
               WHEN BILL_TYPE_F2  ='97'  THEN 'INDEMNITY LUMP SUM SETTLEMENT'
               WHEN (BILL_TYPE_F2  ='98' AND BILL_TYPE_L3 <> '125' ) THEN 'INDEMNITY LUMP SUM ADVANCE'
               WHEN BILL_TYPE_L3  ='100' THEN 'INDEMNITY PTD'
               WHEN BILL_TYPE_L3  ='101' THEN 'INDEMNITY TEMPORARY TOTAL'
               WHEN BILL_TYPE_L3  ='102' THEN 'INDEMNITY TEMPORARY PARTIAL'
               WHEN BILL_TYPE_L3  ='105' THEN 'INDEMNITY PERMANENT PARTIAL'
               WHEN BILL_TYPE_L3  ='106' THEN 'INDEMNITY PCT PERMANENT PARTIAL'
               WHEN BILL_TYPE_L3 IN ('107', '108', '109') THEN 'INDEMNITY DEATH BENEFIT'
               WHEN BILL_TYPE_L3  ='113' THEN 'INDEMNITY LIVING MAINTENANCE REHAB'
               WHEN BILL_TYPE_L3  ='130' THEN 'INDEMNITY WORKING WAGE LOSS'
               WHEN BILL_TYPE_L3  ='131' THEN 'INDEMNITY LIVING MAINTENANCE WAGE LOSS'
               WHEN BILL_TYPE_L3  ='125' THEN 'ATTORNEY FEE'
               WHEN BILL_TYPE_L3  ='126' THEN 'CONTRACT DOCTOR'
               WHEN BILL_TYPE_L3 IN ('150','160','527','950') THEN 'UNDEFINED' 
               ELSE 'INDEMNITY OTHER INDEMNITY'   END        as                         CLAIM_PAYMENT_CATEGORY_DESC
		from SRC_DPC
            )

---- RENAME LAYER ----
,

RENAME_DPC as ( SELECT 
		  CHECK_NO                                           as                                       CHECK_NUMBER
		, WRNT_NO                                            as                                     WARRANT_NUMBER
		, TCN_NO                                             as                                         TCN_NUMBER
		, PAYEE_NAME                                         as                                    PAYEE_FULL_NAME
		, WRNT_DATE                                          as                                       WARRANT_DATE
		, PRVDR_NO                                           as                                    PROVIDER_NUMBER
		, CLAIM_NO                                           as                                       CLAIM_NUMBER
		, PLCY_NO                                            as                                      POLICY_NUMBER
		, BSNS_SQNC_NO                                       as                           BUSINESS_SEQUENCE_NUMBER
		, EIN_NO                                             as                                         EIN_NUMBER
		, REMIT_ADVC_NO                                      as                               REMIT_ADVANCE_NUMBER
		, WRNT_AMT                                           as                                        WARRANT_AMT
		, PYMNT_CODE_AMT                                     as                                PAYMENT_CODE_AMOUNT
		, ACNTB_CODE                                         as                                         ACNTB_CODE
		, PYMNT_FUND_TYPE                                    as                                    PYMNT_FUND_TYPE
		, CVRG_TYPE                                          as                                          CVRG_TYPE
		, BILL_TYPE_F2                                       as                                       BILL_TYPE_F2
		, BILL_TYPE_L3                                       as                                       BILL_TYPE_L3
		, ACDNT_TYPE                                         as                                         ACDNT_TYPE
		, STS_CODE                                           as                                           STS_CODE
		, CLAIM_PAYMENT_CATEGORY_DESC                        as                        CLAIM_PAYMENT_CATEGORY_DESC 
				FROM     LOGIC_DPC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DPC                            as ( SELECT * from    RENAME_DPC   ),

---- JOIN LAYER ----

 JOIN_DPC  as  ( SELECT * 
				FROM  FILTER_DPC )
 SELECT * FROM  JOIN_DPC
  );
