

      create or replace  table DEV_EDW.STAGING.STG_EDI_HEADER  as
      (---- SRC LAYER ----
WITH
SRC_HEADER as ( SELECT *     from     DEV_VIEWS.BASE.EDI_HEADER ),
//SRC_HEADER as ( SELECT *     from     EDI_HEADER) ,

---- LOGIC LAYER ----

LOGIC_HEADER as ( SELECT 
		  EDI_HDR_ID                                         AS                                         EDI_HDR_ID 
		, EDI_ID                                             AS                                             EDI_ID 
		, upper( TRIM( TRAN_TYPE ) )                         AS                                          TRAN_TYPE 
		, upper( TRIM( PARTNER_ID ) )                        AS                                         PARTNER_ID 
		, upper( TRIM( PURPOSE_CODE ) )                      AS                                       PURPOSE_CODE 
		, upper( TRIM( RFRNCE_BATCH_NUM ) )                  AS                                   RFRNCE_BATCH_NUM 
		, upper( TRIM( BWC_SUBMITTER ) )                     AS                                      BWC_SUBMITTER 
		, upper( TRIM( TRANSACTION_DATE ) )                  AS                                   TRANSACTION_DATE 
		, upper( TRIM( TIME_CODE ) )                         AS                                          TIME_CODE 
		, upper( TRIM( TRANSACTION_TIME ) )                  AS                                   TRANSACTION_TIME 
		, upper( TRIM( ACTION_CODE ) )                       AS                                        ACTION_CODE 
		, upper( TRIM( BWC_RECEIVER ) )                      AS                                       BWC_RECEIVER 
		, upper( TRIM( BWC_SENDER ) )                        AS                                         BWC_SENDER 
		, upper( TRIM( ISA_GSA_ST_CTL ) )                    AS                                     ISA_GSA_ST_CTL 
		, upper( TRIM( EDI_CONTACT_NAME ) )                  AS                                   EDI_CONTACT_NAME 
		, upper( TRIM( EDI_CONTACT_PHONE ) )                 AS                                  EDI_CONTACT_PHONE 
		, upper( TRIM( EDI_CONTACT_FAX ) )                   AS                                    EDI_CONTACT_FAX 
		, upper( TRIM( EDI_CONTACT_EMAIL ) )                 AS                                  EDI_CONTACT_EMAIL 
		, upper( TRIM( EDI_CONTACT_EXT ) )                   AS                                    EDI_CONTACT_EXT 
		from SRC_HEADER
            )

---- RENAME LAYER ----
,

RENAME_HEADER as ( SELECT 
		  EDI_HDR_ID                                         as                                         EDI_HDR_ID
		, EDI_ID                                             as                                             EDI_ID
		, TRAN_TYPE                                          as                                          TRAN_TYPE
		, PARTNER_ID                                         as                                         PARTNER_ID
		, PURPOSE_CODE                                       as                                       PURPOSE_CODE
		, RFRNCE_BATCH_NUM                                   as                                   RFRNCE_BATCH_NUM
		, BWC_SUBMITTER                                      as                                      BWC_SUBMITTER
		, TRANSACTION_DATE                                   as                                   TRANSACTION_DATE
		, TIME_CODE                                          as                                          TIME_CODE
		, TRANSACTION_TIME                                   as                                   TRANSACTION_TIME
		, ACTION_CODE                                        as                                        ACTION_CODE
		, BWC_RECEIVER                                       as                                       BWC_RECEIVER
		, BWC_SENDER                                         as                                         BWC_SENDER
		, ISA_GSA_ST_CTL                                     as                                     ISA_GSA_ST_CTL
		, EDI_CONTACT_NAME                                   as                                   EDI_CONTACT_NAME
		, EDI_CONTACT_PHONE                                  as                                  EDI_CONTACT_PHONE
		, EDI_CONTACT_FAX                                    as                                    EDI_CONTACT_FAX
		, EDI_CONTACT_EMAIL                                  as                                  EDI_CONTACT_EMAIL
		, EDI_CONTACT_EXT                                    as                                    EDI_CONTACT_EXT 
				FROM     LOGIC_HEADER   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_HEADER                         as ( SELECT * from    RENAME_HEADER   ),

---- JOIN LAYER ----

 JOIN_HEADER  as  ( SELECT * 
				FROM  FILTER_HEADER )
 SELECT * FROM  JOIN_HEADER
      );
    