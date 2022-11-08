
  create or replace  view DEV_EDW.STAGING.DSV_MEDICAL_INVOICE_LINE_EDIT_EOB  as (
    

---- SRC LAYER ----
WITH
SRC_FLF as ( SELECT *     from     STAGING.DST_MEDICAL_INVOICE_LINE_EDIT_EOB ),
//SRC_FLF as ( SELECT *     from     DST_MEDICAL_INVOICE_LINE_EDIT_EOB) ,

---- LOGIC LAYER ----

LOGIC_FLF as ( SELECT 
		  INVOICE_NUMBER                                     AS                                     INVOICE_NUMBER 
		, LINE_SEQUENCE                                      AS                                      LINE_SEQUENCE 
		, RECEIPT_DATE                                       AS                                       RECEIPT_DATE 
		, LINE_EDIT_SOURCE                                   AS                                   LINE_EDIT_SOURCE 
		, LINE_EDIT_PHASE_APPLIED                            AS                            LINE_EDIT_PHASE_APPLIED 
		, LINE_EDIT_DISPOSITION                              AS                              LINE_EDIT_DISPOSITION 
		, EDIT_CODE                                          AS                                          EDIT_CODE 
		, LINE_EOB_SOURCE                                    AS                                    LINE_EOB_SOURCE 
		, LINE_EOB_PHASE_APPLIED                             AS                             LINE_EOB_PHASE_APPLIED 
		, EOB_DISPOSITION                                    AS                                    EOB_DISPOSITION 
		, EOB_CODE                                           AS                                           EOB_CODE 
		, EDIT_ENTRY_DATE                                    AS                                    EDIT_ENTRY_DATE 
		, EOB_ENTRY_DATE                                     AS                                     EOB_ENTRY_DATE 
		from SRC_FLF
            )

---- RENAME LAYER ----
,

RENAME_FLF as ( SELECT 
		  INVOICE_NUMBER                                     as                             MEDICAL_INVOICE_NUMBER
		, LINE_SEQUENCE                                      as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE
		, LINE_EDIT_SOURCE                                   as                                   LINE_EDIT_SOURCE
		, LINE_EDIT_PHASE_APPLIED                            as                            LINE_EDIT_PHASE_APPLIED
		, LINE_EDIT_DISPOSITION                              as                              LINE_EDIT_DISPOSITION
		, EDIT_CODE                                          as                                          EDIT_CODE
		, LINE_EOB_SOURCE                                    as                                    LINE_EOB_SOURCE
		, LINE_EOB_PHASE_APPLIED                             as                             LINE_EOB_PHASE_APPLIED
		, EOB_DISPOSITION                                    as                                    EOB_DISPOSITION
		, EOB_CODE                                           as                                           EOB_CODE
		, EDIT_ENTRY_DATE                                    as                                    EDIT_ENTRY_DATE
		, EOB_ENTRY_DATE                                     as                                     EOB_ENTRY_DATE 
				FROM     LOGIC_FLF   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_FLF                            as ( SELECT * from    RENAME_FLF   ),

---- JOIN LAYER ----

 JOIN_FLF  as  ( SELECT * 
				FROM  FILTER_FLF )
 SELECT * FROM  JOIN_FLF
  );
