

      create or replace  table DEV_EDW.STAGING.STG_DETAIL_PAYMENT_CODING  as
      (---- SRC LAYER ----
WITH
SRC_PCDQ as ( SELECT *     from     DEV_VIEWS.DBPYQP00.TPYPCDQ ),
SRC_PACN as ( SELECT *     from     DEV_VIEWS.DBDWQP00.TCDPACN ),
SRC_FND as ( SELECT *     from     DEV_VIEWS.DBDWQP00.TCDPFND ),
SRC_CVG as ( SELECT *     from     DEV_VIEWS.DBDWQP00.TCDPCVG ),
SRC_ACD as ( SELECT *     from     DEV_VIEWS.DBDWQP00.TCDPACD ),
SRC_PBTF as ( SELECT *     from     DEV_VIEWS.DBDWQP00.TCDPBTF ),
SRC_PBTL as ( SELECT *     from     DEV_VIEWS.DBDWQP00.TCDPBTL ),
SRC_SYC as ( SELECT *     from     DEV_VIEWS.DBPYQP00.TPYOSYC ),
SRC_WARQ as ( SELECT *     from     DEV_VIEWS.DBPYQP00.TPYWARQ ),
SRC_WRSC as ( SELECT *     from     DEV_VIEWS.DBPYQP00.TPYWRSC ),
//SRC_PCDQ as ( SELECT *     from     TPYPCDQ) ,
//SRC_PACN as ( SELECT *     from     TCDPACN) ,
//SRC_FND as ( SELECT *     from     TCDPFND) ,
//SRC_CVG as ( SELECT *     from     TCDPCVG) ,
//SRC_ACD as ( SELECT *     from     TCDPACD) ,
//SRC_PBTF as ( SELECT *     from     TCDPBTF) ,
//SRC_PBTL as ( SELECT *     from     TCDPBTL) ,
//SRC_SYC as ( SELECT *     from     TPYOSYC) ,
//SRC_WARQ as ( SELECT *     from     TPYWARQ) ,
//SRC_WRSC as ( SELECT *     from     TPYWRSC) ,

---- LOGIC LAYER ----

LOGIC_PCDQ as ( SELECT 
          TRIM( CHECK_NO )                        	 		 AS                                           CHECK_NO 
		, TRIM( TCN_NO ) 			                   	 	 AS                                             TCN_NO 
		, TRIM( ACNTB_CODE ) 	                      		 AS                                         ACNTB_CODE 
		, TRIM( FUND_TYPE )                   		         AS                                          FUND_TYPE 
		, TRIM( CVRG_TYPE )                 		         AS                                          CVRG_TYPE 
		, TRIM( ACDNT_TYPE )                        		 AS                                         ACDNT_TYPE 
		, TRIM( BILL_TYPE_F2 )                      		 AS                                       BILL_TYPE_F2 
		, TRIM( BILL_TYPE_L3 )                      		 AS                                       BILL_TYPE_L3 
		, upper(TRIM( ORGNL_SYSTM_CODE ) )           		 AS                                   ORGNL_SYSTM_CODE 
		, NULLIF(TRIM(CLAIM_NO), '')                  		 AS                                           CLAIM_NO 
		, LPAD(PLCY_NO::VARCHAR,8,'0')                       AS                                            PLCY_NO 
		, BSNS_SQNC_NO                                       AS                                       BSNS_SQNC_NO 
		, NULLIF(UPPER(TRIM(EIN_NO)), '')                    AS                                             EIN_NO 
		, PYMNT_CODE_AMT                                     AS                                     PYMNT_CODE_AMT 
		, cast( WRNT_DATE as DATE )                          AS                                          WRNT_DATE 
		, ENTR_INVC_NO                                       AS                                       ENTR_INVC_NO 
		, TRIM( PYMNT_BILL_NO )					             AS                                      PYMNT_BILL_NO  
		, cast( DW_CNTRL_DATE as DATE )				         AS                                      DW_CNTRL_DATE 
		from SRC_PCDQ
            ),
LOGIC_PACN as ( SELECT 
		  upper( TRIM( ACNTB_TEXT ) )                        AS                                         ACNTB_TEXT 
		, upper( TRIM( ACNTB_DESC ) )                        AS                                         ACNTB_DESC 
		, TRIM( ACNTB_CODE )					             AS                                         ACNTB_CODE 
		from SRC_PACN
            ),
LOGIC_FND as ( SELECT 
		  upper( TRIM( PYMNT_FUND_TEXT ) )                   AS                                    PYMNT_FUND_TEXT 
		, upper( TRIM( PYMNT_FUND_DESC ) )                   AS                                    PYMNT_FUND_DESC 
		, TRIM( PYMNT_FUND_TYPE )				             AS                                    PYMNT_FUND_TYPE 
		from SRC_FND
            ),
LOGIC_CVG as ( SELECT 
		  upper( TRIM( CVRG_TEXT ) )                  		 AS                                          CVRG_TEXT 
		, upper( TRIM( CVRG_DESC ) )                 		 AS                                          CVRG_DESC 
		, TRIM( CVRG_TYPE )					                 AS                                          CVRG_TYPE 
		from SRC_CVG
            ),
LOGIC_ACD as ( SELECT 
		  upper( TRIM( ACDNT_TEXT ) )       		         AS                                         ACDNT_TEXT 
		, upper( TRIM( ACDNT_DESC ) )		                 AS                                         ACDNT_DESC 
		, TRIM( ACDNT_TYPE )				                 AS                                         ACDNT_TYPE 
		from SRC_ACD
            ),
LOGIC_PBTF as ( SELECT 
		  upper( TRIM( BILL_TYPE_F2_TEXT ) )          		 AS                                  BILL_TYPE_F2_TEXT 
		, upper( TRIM( BILL_TYPE_F2_DESC ) )                 AS                                  BILL_TYPE_F2_DESC 
		, TRIM( BILL_TYPE_F2 )					             AS                                       BILL_TYPE_F2 
		from SRC_PBTF
            ),
LOGIC_PBTL as ( SELECT 
		  upper( TRIM( BILL_TYPE_L3_TEXT ) )                 AS                                  BILL_TYPE_L3_TEXT 
		, upper( TRIM( BILL_TYPE_L3_DESC ) )                 AS                                  BILL_TYPE_L3_DESC 
		, TRIM( BILL_TYPE_L3 )				                 AS                                       BILL_TYPE_L3 
		from SRC_PBTL
            ),
LOGIC_SYC as ( SELECT 
		  upper( TRIM( ORGNL_SYSTM_DESC ) )                  AS                                   ORGNL_SYSTM_DESC 
		, TRIM( ORGNL_SYSTM_CODE )				             AS                                   ORGNL_SYSTM_CODE 
		from SRC_SYC
            ),
LOGIC_WARQ as ( SELECT 
		  INTRS_AMT                                          AS                                          INTRS_AMT 
		, WRNT_AMT                                           AS                                           WRNT_AMT 
		, TRIM( WRNT_NO )				                     AS                                            WRNT_NO 
		, cast( STS_DATE as DATE )                           AS                                           STS_DATE 
		, upper( TRIM( STS_CODE_DESC ) )              		 AS                                      STS_CODE_DESC 
		, NULLIF(upper( TRIM( PAYEE_NAME ) ), '')            AS                                         PAYEE_NAME 
		, TRIM( PRVDR_NO )				                     AS                                           PRVDR_NO 
		, NULLIF(upper( TRIM( ADDR ) ), '')                  AS                                               ADDR 
		, NULLIF(upper( TRIM( CITY_NAME ) ) , '')            AS                                          CITY_NAME 
		, NULLIF(upper( TRIM( STATE_CODE ) ) , '')           AS                                         STATE_CODE 
		, NULLIF(TRIM( ZIP_CODE ), '')                       AS                                           ZIP_CODE 
		, NULLIF(TRIM( ZIP_PLUS4_CODE ), '')                 AS                                     ZIP_PLUS4_CODE 
		, REMIT_ADVC_NO                                      AS                                      REMIT_ADVC_NO 
		, TRIM( CHECK_NO )                                   AS                                           CHECK_NO 
		, cast( WRNT_DATE as DATE )                          AS                                          WRNT_DATE 
		from SRC_WARQ
            ),
LOGIC_WRSC as ( SELECT 
		  upper( TRIM( STS_CODE ) )                          AS                                           STS_CODE 
		, upper( TRIM( STS_CODE_DESC ) )                     AS                                      STS_CODE_DESC 
		from SRC_WRSC
            )

---- RENAME LAYER ----
,

RENAME_PCDQ as ( SELECT 
		  CHECK_NO                                           as                                           CHECK_NO
		, TCN_NO                                             as                                             TCN_NO
		, ACNTB_CODE                                         as                                         ACNTB_CODE
		, FUND_TYPE                                          as                                    PYMNT_FUND_TYPE
		, CVRG_TYPE                                          as                                          CVRG_TYPE
		, ACDNT_TYPE                                         as                                         ACDNT_TYPE
		, BILL_TYPE_F2                                       as                                       BILL_TYPE_F2
		, BILL_TYPE_L3                                       as                                       BILL_TYPE_L3
		, ORGNL_SYSTM_CODE                                   as                                   ORGNL_SYSTM_CODE
		, CLAIM_NO                                           as                                           CLAIM_NO
		, PLCY_NO                                            as                                            PLCY_NO
		, BSNS_SQNC_NO                                       as                                       BSNS_SQNC_NO
		, EIN_NO                                             as                                             EIN_NO
		, PYMNT_CODE_AMT                                     as                                     PYMNT_CODE_AMT
		, WRNT_DATE                                          as                                          WRNT_DATE
		, ENTR_INVC_NO                                       as                                       ENTR_INVC_NO
		, PYMNT_BILL_NO                                      as                                      PYMNT_BILL_NO   
		, DW_CNTRL_DATE					         			 as                                      DW_CNTRL_DATE
				FROM     LOGIC_PCDQ   ), 
RENAME_PACN as ( SELECT 
		  ACNTB_TEXT                                         as                                         ACNTB_TEXT
		, ACNTB_DESC                                         as                                         ACNTB_DESC
		, ACNTB_CODE                                         as                                    PACN_ACNTB_CODE 
				FROM     LOGIC_PACN   ), 
RENAME_FND as ( SELECT 
		  PYMNT_FUND_TEXT                                    as                                    PYMNT_FUND_TEXT
		, PYMNT_FUND_DESC                                    as                                    PYMNT_FUND_DESC
		, PYMNT_FUND_TYPE                                    as                                FND_PYMNT_FUND_TYPE 
				FROM     LOGIC_FND   ), 
RENAME_CVG as ( SELECT 
		  CVRG_TEXT                                          as                                          CVRG_TEXT
		, CVRG_DESC                                          as                                          CVRG_DESC
		, CVRG_TYPE                                          as                                      CVG_CVRG_TYPE 
				FROM     LOGIC_CVG   ), 
RENAME_ACD as ( SELECT 
		  ACDNT_TEXT                                         as                                         ACDNT_TEXT
		, ACDNT_DESC                                         as                                         ACDNT_DESC
		, ACDNT_TYPE                                         as                                     ACD_ACDNT_TYPE 
				FROM     LOGIC_ACD   ), 
RENAME_PBTF as ( SELECT 
		  BILL_TYPE_F2_TEXT                                  as                                  BILL_TYPE_F2_TEXT
		, BILL_TYPE_F2_DESC                                  as                                  BILL_TYPE_F2_DESC
		, BILL_TYPE_F2                                       as                                  PBTF_BILL_TYPE_F2 
				FROM     LOGIC_PBTF   ), 
RENAME_PBTL as ( SELECT 
		  BILL_TYPE_L3_TEXT                                  as                                  BILL_TYPE_L3_TEXT
		, BILL_TYPE_L3_DESC                                  as                                  BILL_TYPE_L3_DESC
		, BILL_TYPE_L3                                       as                                  PBTL_BILL_TYPE_L3 
				FROM     LOGIC_PBTL   ), 
RENAME_SYC as ( SELECT 
		  ORGNL_SYSTM_DESC                                   as                                   ORGNL_SYSTM_DESC
		, ORGNL_SYSTM_CODE                                   as                               SYC_ORGNL_SYSTM_CODE 
				FROM     LOGIC_SYC   ), 
RENAME_WARQ as ( SELECT 
		  INTRS_AMT                                          as                                          INTRS_AMT
		, WRNT_AMT                                           as                                           WRNT_AMT
		, WRNT_NO                                            as                                            WRNT_NO
		, STS_DATE                                           as                                       WRQ_STS_DATE
		, STS_CODE_DESC                                      as                                  WRQ_STS_CODE_DESC
		, PAYEE_NAME                                         as                                         PAYEE_NAME
		, PRVDR_NO                                           as                                           PRVDR_NO
		, ADDR                                               as                                               ADDR
		, CITY_NAME                                          as                                          CITY_NAME
		, STATE_CODE                                         as                                         STATE_CODE
		, ZIP_CODE                                           as                                           ZIP_CODE
		, ZIP_PLUS4_CODE                                     as                                     ZIP_PLUS4_CODE
		, REMIT_ADVC_NO                                      as                                      REMIT_ADVC_NO
		, CHECK_NO                                           as                                      WARQ_CHECK_NO
		, WRNT_DATE                                          as                                     WARQ_WRNT_DATE 
				FROM     LOGIC_WARQ   ), 
RENAME_WRSC as ( SELECT 
		  STS_CODE                                           as                                           STS_CODE
		, STS_CODE_DESC                                      as                                 WRSC_STS_CODE_DESC 
				FROM     LOGIC_WRSC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PCDQ                           as ( SELECT * from    RENAME_PCDQ   ),
FILTER_WARQ                           as ( SELECT * from    RENAME_WARQ   ),
FILTER_PACN                           as ( SELECT * from    RENAME_PACN   ),
FILTER_CVG                            as ( SELECT * from    RENAME_CVG   ),
FILTER_PBTF                           as ( SELECT * from    RENAME_PBTF   ),
FILTER_SYC                            as ( SELECT * from    RENAME_SYC   ),
FILTER_FND                            as ( SELECT * from    RENAME_FND   ),
FILTER_ACD                            as ( SELECT * from    RENAME_ACD   ),
FILTER_PBTL                           as ( SELECT * from    RENAME_PBTL   ),
FILTER_WRSC                           as ( SELECT * from    RENAME_WRSC   ),

---- JOIN LAYER ----

WARQ as ( SELECT * 
				FROM  FILTER_WARQ
				LEFT JOIN FILTER_WRSC ON  FILTER_WARQ.WRQ_STS_CODE_DESC =  FILTER_WRSC.WRSC_STS_CODE_DESC  ),
PCDQ as ( SELECT * 
				FROM  FILTER_PCDQ
				INNER JOIN WARQ ON  FILTER_PCDQ.CHECK_NO = WARQ.WARQ_CHECK_NO AND FILTER_PCDQ.WRNT_DATE = WARQ.WARQ_WRNT_DATE
						LEFT JOIN FILTER_PACN ON  FILTER_PCDQ.ACNTB_CODE =  FILTER_PACN.PACN_ACNTB_CODE 
								LEFT JOIN FILTER_CVG ON  FILTER_PCDQ.CVRG_TYPE =  FILTER_CVG.CVG_CVRG_TYPE 
								LEFT JOIN FILTER_PBTF ON  FILTER_PCDQ.BILL_TYPE_F2 =  FILTER_PBTF.PBTF_BILL_TYPE_F2 
								LEFT JOIN FILTER_SYC ON  FILTER_PCDQ.ORGNL_SYSTM_CODE =  FILTER_SYC.SYC_ORGNL_SYSTM_CODE 
								LEFT JOIN FILTER_FND ON  FILTER_PCDQ.PYMNT_FUND_TYPE =  FILTER_FND.FND_PYMNT_FUND_TYPE 
								LEFT JOIN FILTER_ACD ON  FILTER_PCDQ.ACDNT_TYPE =  FILTER_ACD.ACD_ACDNT_TYPE 
								LEFT JOIN FILTER_PBTL ON  FILTER_PCDQ.BILL_TYPE_L3 =  FILTER_PBTL.PBTL_BILL_TYPE_L3  )
SELECT * 
from PCDQ
      );
    