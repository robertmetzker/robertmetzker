

      create or replace  table DEV_EDW.STAGING.STG_INVOICE_HEADER  as
      (---- SRC LAYER ----
WITH
SRC_HDR as ( SELECT *     from     DEV_VIEWS.BASE.INVOICE_HEADER ),
SRC_N as ( SELECT *     from     DEV_VIEWS.BASE.NTWK ),
SRC_CLM as ( SELECT *     from     DEV_VIEWS.BASE.CLM ),
SRC_P1 as ( SELECT *     from     DEV_VIEWS.BASE.PRO ),
SRC_P2 as ( SELECT *     from     DEV_VIEWS.BASE.PRO ),
SRC_P4 as ( SELECT *     from     DEV_VIEWS.BASE.PRO ),
SRC_R1 as ( SELECT *     from     DEV_VIEWS.BASE.REF ),
SRC_R2 as ( SELECT *     from     DEV_VIEWS.BASE.REF ),
SRC_ICD as ( SELECT *     from     DEV_VIEWS.BASE.INVOICE_DIAGNOSIS ),
SRC_P3 as ( SELECT *     from     DEV_VIEWS.BASE.PRO ),
//SRC_HDR as ( SELECT *     from     INVOICE_HEADER) ,
//SRC_N as ( SELECT *     from     NTWK) ,
//SRC_CLM as ( SELECT *     from     CLM) ,
//SRC_P1 as ( SELECT *     from     PRO) ,
//SRC_P2 as ( SELECT *     from     PRO) ,
//SRC_P4 as ( SELECT *     from     PRO) ,
//SRC_R1 as ( SELECT *     from     REF) ,
//SRC_R2 as ( SELECT *     from     REF) ,
//SRC_ICD as ( SELECT *     from     INVOICE_DIAGNOSIS) ,
//SRC_P3 as ( SELECT *     from     PRO) ,

---- LOGIC LAYER ----

LOGIC_HDR as ( SELECT 
		  INVOICE_HEADER_ID                                  AS                                  INVOICE_HEADER_ID 
		, TO_CHAR(RECEIPT_DATE, 'MM/DD/YYYY')||' '|| LPAD(BATCH_NUMBER, 4, '0') ||' '|| LPAD(BATCH_SEQUENCE, 2, '0') ||' '|| LPAD(EXTENSION_NUMBER, 2, '0') AS INVOICE_NUMBER
		, NTWK_ID                                            AS                                            NTWK_ID 
		, CLAIM_ID                                           AS                                           CLAIM_ID 
		, PRO_ID                                             AS                                             PRO_ID 
		, PAY_TO_PRO_ID                                      AS                                      PAY_TO_PRO_ID 
		, OTHER_PHYSICIAN1                                   AS                                   OTHER_PHYSICIAN1 
		, TRIM( INVOICE_TYPE )                               AS                                       INVOICE_TYPE 
		, TRIM( INVOICE_STATUS )                             AS                                     INVOICE_STATUS 
		, cast( ENTRY_DATE as DATE )                         AS                                         ENTRY_DATE 
		, cast( RECEIPT_DATE as DATE )                       AS                                       RECEIPT_DATE 
		, cast( NTWK_RECEIPT_DATE as DATE )                  AS                                  NTWK_RECEIPT_DATE 
		, cast( SERVICE_FROM as DATE )                       AS                                       SERVICE_FROM 
		, cast( SERVICE_TO as DATE )                         AS                                         SERVICE_TO 
		, BATCH_NUMBER                                       AS                                       BATCH_NUMBER 
		, BATCH_SEQUENCE                                     AS                                     BATCH_SEQUENCE 
		, TRIM( BATCH_TYPE )                                 AS                                         BATCH_TYPE 
		, EXTENSION_NUMBER                                   AS                                   EXTENSION_NUMBER 
		, VERSION_NUMBER                                     AS                                     VERSION_NUMBER 
		, PAYEE_ID                                           AS                                           PAYEE_ID 
		, TRIM( NTWK_PAYEE_IND )                             AS                                     NTWK_PAYEE_IND 
		, TRIM( PAYEE_TYPE )                                 AS                                         PAYEE_TYPE 
		, TRIM( PRIOR_AUTH_NUMBER )                          AS                                  PRIOR_AUTH_NUMBER 
		, COVERED_DAYS                                       AS                                       COVERED_DAYS 
		, NON_COVERED_DAYS                                   AS                                   NON_COVERED_DAYS 
		, cast( ADMISSION_DATE as DATE )                     AS                                     ADMISSION_DATE 
		, TRIM( ADMISSION_HOURS )                            AS                                    ADMISSION_HOURS 
		, TRIM( ADMISSION_TYPE )                             AS                                     ADMISSION_TYPE 
		, TRIM( ADMISSION_SOURCE )                           AS                                   ADMISSION_SOURCE 
		, TRIM( NTWK_INVOICE_NUMBER )                        AS                                NTWK_INVOICE_NUMBER 
		, cast( NTWK_APR_DATE as DATE )                      AS                                      NTWK_APR_DATE 
		, TRIM( DISCHARGE_STATUS )                           AS                                   DISCHARGE_STATUS 
		, cast( DISCHARGE_DATE as DATE )                     AS                                     DISCHARGE_DATE 
		, TRIM( DISCHARGE_HOURS )                            AS                                    DISCHARGE_HOURS 
		, cast( BILL_DATE as DATE )                          AS                                          BILL_DATE 
		, TRIM( BILL_TYPE )                                  AS                                          BILL_TYPE 
		, TRIM( PRO_DRG )                                    AS                                            PRO_DRG 
		, TRIM( PATIENT_ACCT_NUMBER )                        AS                                PATIENT_ACCT_NUMBER 
		, REFERRING_PRO_ID                                   AS                                   REFERRING_PRO_ID 
		, TRIM( MOD_SET )                                    AS                                            MOD_SET 
		, TRIM( ULM )                                        AS                                                ULM 
		, DLM                                                AS                                                DLM 
		, TOTAL_APPROVED                                     AS                                     TOTAL_APPROVED 
		from SRC_HDR
            ),
LOGIC_N as ( SELECT 
		  cast( NTWK_NUMBER as TEXT )                        AS                                        NTWK_NUMBER 
		, TRIM( NTWK_ID )                                    AS                                            NTWK_ID 
		from SRC_N
            ),
LOGIC_CLM as ( SELECT 
		  TRIM( CLAIM_NUMBER )                               AS                                       CLAIM_NUMBER 
		, TRIM( EMPLOYER_POLICY_NUMBER )                     AS                             EMPLOYER_POLICY_NUMBER 
		, CLAIM_ID                                           AS                                           CLAIM_ID 
		from SRC_CLM
            ),
LOGIC_P1 as ( SELECT 
		  TRIM( PRO_NUM )                                   AS                                           PRO_NUM 
		, PRO_ID                                             AS                                             PRO_ID 
		from SRC_P1
            ),
LOGIC_P2 as ( SELECT 
		  TRIM( PRO_NUM )                                   AS                                           PRO_NUM 
		, PRO_ID                                             AS                                             PRO_ID 
		from SRC_P2
            ),
LOGIC_P4 as ( SELECT 
		  TRIM( PRO_NUM )                                   AS                                           PRO_NUM 
		, PRO_ID                                             AS                                             PRO_ID 
		from SRC_P4
            ),
LOGIC_R1 as ( SELECT 
		  TRIM( REF_DSC )                                    AS                                            REF_DSC 
		, TRIM( REF_IDN )                                    AS                                            REF_IDN 
		, TRIM( REF_DGN )                                    AS                                            REF_DGN 
		, cast( REF_EXP_DTE as DATE )                        AS                                        REF_EXP_DTE 
		from SRC_R1
            ),
LOGIC_R2 as ( SELECT 
		  TRIM( REF_DSC )                                    AS                                            REF_DSC 
		, TRIM( REF_IDN )                                    AS                                            REF_IDN 
		, TRIM( REF_DGN )                                    AS                                            REF_DGN 
		, cast( REF_EXP_DTE as DATE )                        AS                                        REF_EXP_DTE 
		from SRC_R2
            ),
LOGIC_ICD as ( SELECT 
		  TRIM( DIAGNOSIS_CODE )                             AS                                     DIAGNOSIS_CODE 
		, INVOICE_HEADER_ID                                  AS                                  INVOICE_HEADER_ID 
		, SEQUENCE_NUMBER                                    AS                                    SEQUENCE_NUMBER 
		from SRC_ICD
            ),
LOGIC_P3 as ( SELECT 
		  TRIM( PRO_NUM )                                   AS                                           PRO_NUM 
		, PRO_ID                                             AS                                             PRO_ID 
		from SRC_P3
            )

---- RENAME LAYER ----
,

RENAME_HDR as ( SELECT 
		  INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID
		, INVOICE_NUMBER                                     as                                     INVOICE_NUMBER
		, NTWK_ID                                            as                                            NTWK_ID
		, CLAIM_ID                                           as                                           CLAIM_ID
		, PRO_ID                                             as                                       SRVCN_PRO_ID
		, PAY_TO_PRO_ID                                      as                                       PAYTO_PRO_ID
		, OTHER_PHYSICIAN1                                   as                                   OTHER_PHYSICIAN1
		, INVOICE_TYPE                                       as                                       INVOICE_TYPE
		, INVOICE_STATUS                                     as                                     INVOICE_STATUS
		, ENTRY_DATE                                         as                                         ENTRY_DATE
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE
		, NTWK_RECEIPT_DATE                                  as                                  NTWK_RECEIPT_DATE
		, SERVICE_FROM                                       as                                       SERVICE_FROM
		, SERVICE_TO                                         as                                         SERVICE_TO
		, BATCH_NUMBER                                       as                                       BATCH_NUMBER
		, BATCH_SEQUENCE                                     as                                     BATCH_SEQUENCE
		, BATCH_TYPE                                         as                                         BATCH_TYPE
		, EXTENSION_NUMBER                                   as                                   EXTENSION_NUMBER
		, VERSION_NUMBER                                     as                                     VERSION_NUMBER
		, PAYEE_ID                                           as                                           PAYEE_ID
		, NTWK_PAYEE_IND                                     as                                     NTWK_PAYEE_IND
		, PAYEE_TYPE                                         as                                         PAYEE_TYPE
		, PRIOR_AUTH_NUMBER                                  as                                  PRIOR_AUTH_NUMBER
		, COVERED_DAYS                                       as                                       COVERED_DAYS
		, NON_COVERED_DAYS                                   as                                   NON_COVERED_DAYS
		, ADMISSION_DATE                                     as                                     ADMISSION_DATE
		, ADMISSION_HOURS                                    as                                    ADMISSION_HOURS
		, ADMISSION_TYPE                                     as                                     ADMISSION_TYPE
		, ADMISSION_SOURCE                                   as                                   ADMISSION_SOURCE
		, NTWK_INVOICE_NUMBER                                as                                NTWK_INVOICE_NUMBER
		, NTWK_APR_DATE                                      as                                      NTWK_APR_DATE
		, DISCHARGE_STATUS                                   as                                   DISCHARGE_STATUS
		, DISCHARGE_DATE                                     as                                     DISCHARGE_DATE
		, DISCHARGE_HOURS                                    as                                    DISCHARGE_HOURS
		, BILL_DATE                                          as                                          BILL_DATE
		, BILL_TYPE                                          as                                          BILL_TYPE
		, PRO_DRG                                            as                                            PRO_DRG
		, PATIENT_ACCT_NUMBER                                as                                PATIENT_ACCT_NUMBER
		, REFERRING_PRO_ID                                   as                                   REFERRING_PRO_ID
		, MOD_SET                                            as                                            MOD_SET
		, ULM                                                as                                                ULM
		, DLM                                                as                                                DLM
		, TOTAL_APPROVED                                     AS                                     TOTAL_APPROVED 
				FROM     LOGIC_HDR   ), 
RENAME_N as ( SELECT 
		  NTWK_NUMBER                                        as                                         MCO_NUMBER
		, NTWK_ID                                            as                                          N_NTWK_ID 
				FROM     LOGIC_N   ), 
RENAME_CLM as ( SELECT 
		  CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		, EMPLOYER_POLICY_NUMBER                             as                                      POLICY_NUMBER
		, CLAIM_ID                                           as                                       CLM_CLAIM_ID 
				FROM     LOGIC_CLM   ), 
RENAME_P1 as ( SELECT 
		  PRO_NUM                                           as                                 SRVCN_PEACH_NUMBER
		, PRO_ID                                             as                                          P1_PRO_ID 
				FROM     LOGIC_P1   ), 
RENAME_P2 as ( SELECT 
		  PRO_NUM                                           as                                 PAYTO_PEACH_NUMBER
		, PRO_ID                                             as                                          P2_PRO_ID 
				FROM     LOGIC_P2   ), 
RENAME_P4 as ( SELECT 
		  PRO_NUM                                           as                              PRESCRIBING_PHYISCIAN
		, PRO_ID                                             as                                          P4_PRO_ID 
				FROM     LOGIC_P4   ), 
RENAME_R1 as ( SELECT 
		  REF_DSC                                            as                                  INVOICE_TYPE_DESC
		, REF_IDN                                            as                                         R1_REF_IDN
		, REF_DGN                                            as                                         R1_REF_DGN
		, REF_EXP_DTE                                        as                                     R1_REF_EXP_DTE 
				FROM     LOGIC_R1   ), 
RENAME_R2 as ( SELECT 
		  REF_DSC                                            as                                INVOICE_STATUS_DESC
		, REF_IDN                                            as                                         R2_REF_IDN
		, REF_DGN                                            as                                         R2_REF_DGN
		, REF_EXP_DTE                                        as                                     R2_REF_EXP_DTE 
				FROM     LOGIC_R2   ), 
RENAME_ICD as ( SELECT 
		  DIAGNOSIS_CODE                                     as                                     DIAGNOSIS_CODE
		, INVOICE_HEADER_ID                                  as                              ICD_INVOICE_HEADER_ID
		, SEQUENCE_NUMBER                                    as                                ICD_SEQUENCE_NUMBER 
				FROM     LOGIC_ICD   ), 
RENAME_P3 as ( SELECT 
		  PRO_NUM                                           as                             REFERRING_PEACH_NUMBER
		, PRO_ID                                             as                                          P3_PRO_ID 
				FROM     LOGIC_P3   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_HDR                            as ( SELECT * from    RENAME_HDR   ),
FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),
FILTER_R1                             as ( SELECT * from    RENAME_R1 
				WHERE R1_REF_DGN = 'BLT' and R1_REF_EXP_DTE > current_date()  ),
FILTER_R2                             as ( SELECT * from    RENAME_R2 
				WHERE R2_REF_DGN = 'AST' and R2_REF_EXP_DTE > current_date()  ),
FILTER_P1                             as ( SELECT * from    RENAME_P1   ),
FILTER_P2                             as ( SELECT * from    RENAME_P2   ),
FILTER_P3                             as ( SELECT * from    RENAME_P3   ),
FILTER_P4                             as ( SELECT * from    RENAME_P4   ),
FILTER_N                              as ( SELECT * from    RENAME_N   ),
FILTER_ICD                            as ( SELECT * from    RENAME_ICD 
				WHERE ICD_SEQUENCE_NUMBER = '1'  ),

---- JOIN LAYER ----

HDR as ( SELECT * 
				FROM  FILTER_HDR
				LEFT JOIN FILTER_CLM ON  FILTER_HDR.CLAIM_ID =  FILTER_CLM.CLM_CLAIM_ID 
								LEFT JOIN FILTER_R1 ON  FILTER_HDR.INVOICE_TYPE =  FILTER_R1.R1_REF_IDN 
								LEFT JOIN FILTER_R2 ON  FILTER_HDR.INVOICE_STATUS =  FILTER_R2.R2_REF_IDN 
								LEFT JOIN FILTER_P1 ON  FILTER_HDR.SRVCN_PRO_ID =  FILTER_P1.P1_PRO_ID 
								LEFT JOIN FILTER_P2 ON  FILTER_HDR.PAYTO_PRO_ID =  FILTER_P2.P2_PRO_ID 
								LEFT JOIN FILTER_P3 ON  FILTER_HDR.REFERRING_PRO_ID =  FILTER_P3.P3_PRO_ID 
								LEFT JOIN FILTER_P4 ON  FILTER_HDR.OTHER_PHYSICIAN1 =  FILTER_P4.P4_PRO_ID 
								LEFT JOIN FILTER_N ON  FILTER_HDR.NTWK_ID =  FILTER_N.N_NTWK_ID 
								LEFT JOIN FILTER_ICD ON  FILTER_HDR.INVOICE_HEADER_ID =  FILTER_ICD.ICD_INVOICE_HEADER_ID  )
SELECT * 
from HDR
      );
    