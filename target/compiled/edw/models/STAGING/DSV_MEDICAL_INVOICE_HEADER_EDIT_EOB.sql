

---- SRC LAYER ----
WITH
SRC_FLF as ( SELECT *     from     STAGING.DST_MEDICAL_INVOICE_HEADER_EDIT_EOB ),
//SRC_FLF as ( SELECT *     from     DST_MEDICAL_INVOICE_HEADER_EDIT_EOB) ,

---- LOGIC LAYER ----

LOGIC_FLF as ( SELECT 
		  INVOICE_NUMBER                                     AS                                     INVOICE_NUMBER 
		, RECEIPT_DATE                                       AS                                       RECEIPT_DATE 
		, HDR_EDIT_SOURCE                                    AS                                    HDR_EDIT_SOURCE 
		, CAST(HDR_EDIT_PHASE_APPLIED AS NUMBER)             AS                             HDR_EDIT_PHASE_APPLIED 
		, HDR_EDIT_DISPOSITION                               AS                               HDR_EDIT_DISPOSITION 
		, EDIT_CODE                                          AS                                          EDIT_CODE 
		, HDR_EOB_SOURCE                                     AS                                     HDR_EOB_SOURCE 
		, CAST(HDR_EOB_PHASE_APPLIED AS NUMBER)              AS               				 HDR_EOB_PHASE_APPLIED 
		, EOB_DISPOSITION                                    AS                                    EOB_DISPOSITION 
		, EOB_CODE                                           AS                                           EOB_CODE 
		, EOB_ENTRY_DATE                                     AS                                     EOB_ENTRY_DATE 
		, EDIT_ENTRY_DATE                                    AS                                    EDIT_ENTRY_DATE 
		from SRC_FLF
            )

---- RENAME LAYER ----
,

RENAME_FLF as ( SELECT 
		  INVOICE_NUMBER                                     as                             MEDICAL_INVOICE_NUMBER
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE
		, HDR_EDIT_SOURCE                                    as                                    HDR_EDIT_SOURCE
		, HDR_EDIT_PHASE_APPLIED                             as                             HDR_EDIT_PHASE_APPLIED
		, HDR_EDIT_DISPOSITION                               as                               HDR_EDIT_DISPOSITION
		, EDIT_CODE                                          as                                          EDIT_CODE
		, HDR_EOB_SOURCE                                     as                                     HDR_EOB_SOURCE
		, HDR_EOB_PHASE_APPLIED                              as                              HDR_EOB_PHASE_APPLIED
		, EOB_DISPOSITION                                    as                                    EOB_DISPOSITION
		, EOB_CODE                                           as                                           EOB_CODE
		, EOB_ENTRY_DATE                                     as                                     EOB_ENTRY_DATE
		, EDIT_ENTRY_DATE                                    as                                    EDIT_ENTRY_DATE 
				FROM     LOGIC_FLF   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_FLF                            as ( SELECT * from    RENAME_FLF   ),

---- JOIN LAYER ----

 JOIN_FLF  as  ( SELECT * 
				FROM  FILTER_FLF )
 SELECT * FROM  JOIN_FLF