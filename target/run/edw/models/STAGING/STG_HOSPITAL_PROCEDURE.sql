

      create or replace  table DEV_EDW.STAGING.STG_HOSPITAL_PROCEDURE  as
      (---- SRC LAYER ----
WITH
SRC_INV_PROC as ( SELECT *     from     DEV_VIEWS.BASE.INVOICE_PROCEDURE ),
SRC_BASE as ( SELECT *     from     DEV_VIEWS.BASE.ICD_BASE ),
SRC_GENERAL as ( SELECT *     from    DEV_VIEWS.BASE.ICD_GENERAL ),
//SRC_INV_PROC as ( SELECT *     from     INVOICE_PROCEDURE) ,
//SRC_BASE as ( SELECT *     from     ICD_BASE) ,
//SRC_GENERAL as ( SELECT *     from     ICD_GENERAL) ,

---- LOGIC LAYER ----

LOGIC_INV_PROC as ( SELECT 
		  INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID 
		, SEQUENCE_NUMBER                                    as                                    SEQUENCE_NUMBER 
		, upper( TRIM( PROCEDURE_CODE ) )                    as                                     PROCEDURE_CODE 
		, cast( PROCEDURE_DATE as DATE )                     as                                     PROCEDURE_DATE 
		, ICD_VERSION                                        as                                        ICD_VERSION 
		from SRC_INV_PROC
            ),
LOGIC_BASE as ( SELECT 
		  ICD_RID                                            as                                            ICD_RID 
		, upper( TRIM( ICD_TYPE ) )                          as                                           ICD_TYPE 
		, upper( TRIM( CODE ) )                              as                                               CODE 
		, VERSION                                            as                                            VERSION 
		, upper( TRIM( ENTRY_USER_ID ) )                     as                                      ENTRY_USER_ID 
		, cast( ENTRY_DATE as DATE )                         as                                         ENTRY_DATE 
		from SRC_BASE
            ),
LOGIC_GENERAL as ( SELECT 
		  cast( EFFECTIVE_DATE as DATE )                     as                                     EFFECTIVE_DATE 
		, cast( EXPIRATION_DATE as DATE )                    as                                    EXPIRATION_DATE 
		, upper( TRIM( ENTRY_USER_ID ) )                     as                              GENERAL_ENTRY_USER_ID 
		, cast( ENTRY_DATE as DATE )                         as                                 GENERAL_ENTRY_DATE 
		, upper( TRIM( ULM ) )                               as                                                ULM 
		, cast( DLM as DATE )                                as                                                DLM 
		, upper( TRIM( GENDER ) )                            as                                             GENDER 
		, MIN_AGE                                            as                                            MIN_AGE 
		, MAX_AGE                                            as                                            MAX_AGE 
		, upper( TRIM( DESCRIPTION ) )                       as                                        DESCRIPTION 
		, upper( TRIM( COVERED_FLAG ) )                      as                                       COVERED_FLAG 
		, ICD_RID                                            as                                    GENERAL_ICD_RID 
		from SRC_GENERAL
            )

---- RENAME LAYER ----
,

RENAME_INV_PROC as ( SELECT 
		  INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID
		, SEQUENCE_NUMBER                                    as                                    SEQUENCE_NUMBER
		, PROCEDURE_CODE                                     as                                     PROCEDURE_CODE
		, PROCEDURE_DATE                                     as                                     PROCEDURE_DATE
		, ICD_VERSION                                        as                                        ICD_VERSION 
				FROM     LOGIC_INV_PROC   ), 
RENAME_BASE as ( SELECT 
		  ICD_RID                                            as                                            ICD_RID
		, ICD_TYPE                                           as                                           ICD_TYPE
		, CODE                                               as                                               CODE
		, VERSION                                            as                                            VERSION
		, ENTRY_USER_ID                                      as                                      ENTRY_USER_ID
		, ENTRY_DATE                                         as                                         ENTRY_DATE 
				FROM     LOGIC_BASE   ), 
RENAME_GENERAL as ( SELECT 
		  EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                                    EXPIRATION_DATE
		, GENERAL_ENTRY_USER_ID                              as                              GENERAL_ENTRY_USER_ID
		, GENERAL_ENTRY_DATE                                 as                                 GENERAL_ENTRY_DATE
		, ULM                                                as                                                ULM
		, DLM                                                as                                                DLM
		, GENDER                                             as                                             GENDER
		, MIN_AGE                                            as                                            MIN_AGE
		, MAX_AGE                                            as                                            MAX_AGE
		, DESCRIPTION                                        as                                        DESCRIPTION
		, COVERED_FLAG                                       as                                       COVERED_FLAG
		, GENERAL_ICD_RID                                    as                                    GENERAL_ICD_RID 
				FROM     LOGIC_GENERAL   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_INV_PROC                       as ( SELECT * from    RENAME_INV_PROC   ),
FILTER_BASE                           as ( SELECT * from    RENAME_BASE 
                                            WHERE ICD_TYPE = 'P'  ),
FILTER_GENERAL                        as ( SELECT * from    RENAME_GENERAL   ),

---- JOIN LAYER ----
INV_PROC as ( SELECT * 
				FROM  FILTER_INV_PROC
				LEFT JOIN FILTER_BASE ON  FILTER_INV_PROC.PROCEDURE_CODE = FILTER_BASE.CODE  AND FILTER_INV_PROC.ICD_VERSION = FILTER_BASE.VERSION),

BASE as ( SELECT * 
				FROM  INV_PROC
				LEFT JOIN FILTER_GENERAL ON  INV_PROC.ICD_RID =  FILTER_GENERAL.GENERAL_ICD_RID AND COALESCE(INV_PROC.PROCEDURE_DATE,'2099-12-31 00:00:00')
BETWEEN FILTER_GENERAL.EFFECTIVE_DATE AND FILTER_GENERAL.EXPIRATION_DATE  )

SELECT * 
from BASE
      );
    