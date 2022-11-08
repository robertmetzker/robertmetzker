---- SRC LAYER ----
WITH
SRC_CARE824 as ( SELECT *     from     DEV_VIEWS.BASE.CARE824 ),
//SRC_CARE824 as ( SELECT *     from     CARE824) ,

---- LOGIC LAYER ----

LOGIC_CARE824 as ( SELECT 
		  CARE824_RID                                        AS                                        CARE824_RID 
		, EDI_ID                                             AS                                             EDI_ID 
		, upper( TRIM( MCO_ID ) )                            AS                                             MCO_ID 
		, upper( TRIM( ACKNOW_CODE ) )                       AS                                        ACKNOW_CODE 
		, upper( TRIM( REFERENCE_NO ) )                      AS                                       REFERENCE_NO 
		, upper( TRIM( TOTAL_ITEMS ) )                       AS                                        TOTAL_ITEMS 
		, upper( TRIM( TRANS_RESPONSE_ID ) )                 AS                                  TRANS_RESPONSE_ID 
		, upper( TRIM( BILLING_ACCT_NO ) )                   AS                                    BILLING_ACCT_NO 
		, upper( TRIM( MEDICAL_REC_NO ) )                    AS                                     MEDICAL_REC_NO 
		, upper( TRIM( EMPLOYERS_ID_NO ) )                   AS                                    EMPLOYERS_ID_NO 
		, upper( TRIM( PATIENT_ACCT_NO ) )                   AS                                    PATIENT_ACCT_NO 
		, upper( TRIM( BWC_CLAIM_NO ) )                      AS                                       BWC_CLAIM_NO 
		, upper( TRIM( TEAM_NAME ) )                         AS                                          TEAM_NAME 
		, cast( START_SERV_DATE as DATE )                    AS                                    START_SERV_DATE 
		, upper( TRIM( CONTACT_PHONE ) )                     AS                                      CONTACT_PHONE 
		, upper( TRIM( CONTACT_NAME ) )                      AS                                       CONTACT_NAME 
		, upper( TRIM( CLAIMANT_NAME ) )                     AS                                      CLAIMANT_NAME 
		, upper( TRIM( CLAIMANT_SSN ) )                      AS                                       CLAIMANT_SSN 
		, upper( TRIM( ID_NO_QUALIFIER ) )                   AS                                    ID_NO_QUALIFIER 
		, MCO_RID                                            AS                                            MCO_RID 
		, cast( RECEIPT_DATE as DATE )                       AS                                       RECEIPT_DATE 
		, EDI_HDR_ID_837                                     AS                                     EDI_HDR_ID_837 
		, upper( TRIM( SERV_LEGACY ) )                       AS                                        SERV_LEGACY 
		, upper( TRIM( SERV_NPI ) )                          AS                                           SERV_NPI 
		, upper( TRIM( SERV_PEACH ) )                        AS                                         SERV_PEACH 
		from SRC_CARE824
            )

---- RENAME LAYER ----
,

RENAME_CARE824 as ( SELECT 
		  CARE824_RID                                        as                                        CARE824_RID
		, EDI_ID                                             as                                             EDI_ID
		, MCO_ID                                             as                                             MCO_ID
		, ACKNOW_CODE                                        as                                        ACKNOW_CODE
		, REFERENCE_NO                                       as                                       REFERENCE_NO
		, TOTAL_ITEMS                                        as                                        TOTAL_ITEMS
		, TRANS_RESPONSE_ID                                  as                                  TRANS_RESPONSE_ID
		, BILLING_ACCT_NO                                    as                                    BILLING_ACCT_NO
		, MEDICAL_REC_NO                                     as                                     MEDICAL_REC_NO
		, EMPLOYERS_ID_NO                                    as                                    EMPLOYERS_ID_NO
		, PATIENT_ACCT_NO                                    as                                    PATIENT_ACCT_NO
		, BWC_CLAIM_NO                                       as                                       BWC_CLAIM_NO
		, TEAM_NAME                                          as                                          TEAM_NAME
		, START_SERV_DATE                                    as                                    START_SERV_DATE
		, CONTACT_PHONE                                      as                                      CONTACT_PHONE
		, CONTACT_NAME                                       as                                       CONTACT_NAME
		, CLAIMANT_NAME                                      as                                      CLAIMANT_NAME
		, CLAIMANT_SSN                                       as                                       CLAIMANT_SSN
		, ID_NO_QUALIFIER                                    as                                    ID_NO_QUALIFIER
		, MCO_RID                                            as                                            MCO_RID
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE
		, EDI_HDR_ID_837                                     as                                     EDI_HDR_ID_837
		, SERV_LEGACY                                        as                                        SERV_LEGACY
		, SERV_NPI                                           as                                           SERV_NPI
		, SERV_PEACH                                         as                                         SERV_PEACH 
				FROM     LOGIC_CARE824   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CARE824                        as ( SELECT * from    RENAME_CARE824   ),

---- JOIN LAYER ----

 JOIN_CARE824  as  ( SELECT * 
				FROM  FILTER_CARE824 )
 SELECT * FROM  JOIN_CARE824