

---- SRC LAYER ----
WITH
SRC_ENTRY as ( SELECT *     from     STAGING.DST_EDIT_EOB_ENTRY ),
//SRC_ENTRY as ( SELECT *     from     DST_EDIT_EOB_ENTRY) ,

---- LOGIC LAYER ----

LOGIC_ENTRY as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, SOURCE                                             AS                                             SOURCE 
		, PHASE_APPLIED                                      AS                                      PHASE_APPLIED 
		, DISPOSITION                                        AS                                        DISPOSITION 
		, REF_DSC                                            AS                                            REF_DSC 
		from SRC_ENTRY
            )

---- RENAME LAYER ----
,

RENAME_ENTRY as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, SOURCE                                             as                                        SOURCE_CODE 
		, PHASE_APPLIED                                      as                                 ADJUDICATION_PHASE
		, DISPOSITION                                        as                                   DISPOSITION_CODE
		, REF_DSC                                            as                                   DISPOSITION_DESC 
				FROM     LOGIC_ENTRY   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ENTRY                          as ( SELECT * from    RENAME_ENTRY   ),

---- JOIN LAYER ----

 JOIN_ENTRY  as  ( SELECT * 
				FROM  FILTER_ENTRY )
 SELECT * FROM  JOIN_ENTRY