---- SRC LAYER ----
WITH
SRC_HDR_EOB as ( SELECT *     from     STAGING.STG_INVOICE_HEADER_EOB ),
SRC_HDR_EDIT as ( SELECT *     from     STAGING.STG_INVOICE_HEADER_EDIT ),
SRC_LINE_EOB as ( SELECT *     from     STAGING.STG_INVOICE_LINE_EOB ),
SRC_LINE_EDIT as ( SELECT *     from     STAGING.STG_INVOICE_LINE_EDIT ),
SRC_REF as ( SELECT *     from     STAGING.STG_CAM_REF ),
//SRC_HDR_EOB as ( SELECT *     from     STG_INVOICE_HEADER_EOB) ,
//SRC_HDR_EDIT as ( SELECT *     from     STG_INVOICE_HEADER_EDIT) ,
//SRC_LINE_EOB as ( SELECT *     from     STG_INVOICE_LINE_EOB) ,
//SRC_LINE_EDIT as ( SELECT *     from     STG_INVOICE_LINE_EDIT) ,
//SRC_REF as ( SELECT *     from     STG_CAM_REF) ,

----- LOGIC LAYER ----

LOGIC_HDR_EOB as ( SELECT 
		  TRIM( SOURCE )                                     AS                                             SOURCE 
		, TRIM( PHASE_APPLIED )                              AS                                      PHASE_APPLIED 
		, NULL                                               AS                                        DISPOSITION 
		from SRC_HDR_EOB
            ),
LOGIC_HDR_EDIT as ( SELECT 
		  TRIM( SOURCE )                                     AS                                             SOURCE 
		, TRIM( PHASE_APPLIED )                              AS                                      PHASE_APPLIED 
		, TRIM( DISPOSITION )                                AS                                        DISPOSITION 
		from SRC_HDR_EDIT
            ),
LOGIC_LINE_EOB as ( SELECT 
		  TRIM( SOURCE )                                     AS                                             SOURCE 
		, TRIM( PHASE_APPLIED )                              AS                                      PHASE_APPLIED 
		, NULL                                               AS                                        DISPOSITION 
		from SRC_LINE_EOB
            ),
LOGIC_LINE_EDIT as ( SELECT 
		  TRIM( SOURCE )                                     AS                                             SOURCE 
		, TRIM( PHASE_APPLIED )                              AS                                      PHASE_APPLIED 
		, TRIM( DISPOSITION )                                AS                                        DISPOSITION 
		from SRC_LINE_EDIT
            ),
LOGIC_REF as ( SELECT 
		  TRIM( REF_DGN )                                    AS                                            REF_DGN 
		, TRIM( REF_IDN )                                    AS                                            REF_IDN 
		, REF_EFF_DTE                                        AS                                        REF_EFF_DTE 
		, REF_EXP_DTE                                        AS                                        REF_EXP_DTE 
		, TRIM( REF_DSC )                                    AS                                            REF_DSC 
		from SRC_REF
            )

---- RENAME LAYER ----
,

RENAME_HDR_EOB as ( SELECT 
		  SOURCE                                             as                                             SOURCE
		, PHASE_APPLIED                                      as                                      PHASE_APPLIED
		, DISPOSITION                                        as                                        DISPOSITION 
				FROM     LOGIC_HDR_EOB   ), 
RENAME_HDR_EDIT as ( SELECT 
		  SOURCE                                             as                                             SOURCE
		, PHASE_APPLIED                                      as                                      PHASE_APPLIED
		, DISPOSITION                                        as                                        DISPOSITION 
				FROM     LOGIC_HDR_EDIT   ), 
RENAME_LINE_EOB as ( SELECT 
		  SOURCE                                             as                                             SOURCE
		, PHASE_APPLIED                                      as                                      PHASE_APPLIED
		, DISPOSITION                                        as                                        DISPOSITION 
				FROM     LOGIC_LINE_EOB   ), 
RENAME_LINE_EDIT as ( SELECT 
		  SOURCE                                             as                                             SOURCE
		, PHASE_APPLIED                                      as                                      PHASE_APPLIED
		, DISPOSITION                                        as                                        DISPOSITION 
				FROM     LOGIC_LINE_EDIT   ), 
RENAME_REF as ( SELECT 
		  REF_DGN                                            as                                            REF_DGN
		, REF_IDN                                            as                                            REF_IDN
		, REF_EFF_DTE                                        as                                        REF_EFF_DTE
		, REF_EXP_DTE                                        as                                        REF_EXP_DTE
		, REF_DSC                                            as                                            REF_DSC 
				FROM     LOGIC_REF   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_HDR_EOB                        as ( SELECT * from    RENAME_HDR_EOB   ),
FILTER_HDR_EDIT                       as ( SELECT * from    RENAME_HDR_EDIT   ),
FILTER_LINE_EOB                       as ( SELECT * from    RENAME_LINE_EOB   ),
FILTER_LINE_EDIT                      as ( SELECT * from    RENAME_LINE_EDIT   ),
FILTER_REF                            as ( SELECT * from    RENAME_REF 
				WHERE REF_DGN = 'EDS' AND NVL(REF_EXP_DTE, CURRENT_DATE +1) > CURRENT_DATE  ),

-- UNION LAYER
UNION_EDIT_EOB_ENTRY AS ( SELECT * from    FILTER_HDR_EOB UNION
							SELECT * from    FILTER_HDR_EDIT UNION
							SELECT * from    FILTER_LINE_EOB UNION
							SELECT * from    FILTER_LINE_EDIT),

---- JOIN LAYER ----

EDIT_EOB_ENTRY as ( SELECT * 
				FROM  UNION_EDIT_EOB_ENTRY
				LEFT JOIN FILTER_REF ON  UNION_EDIT_EOB_ENTRY.DISPOSITION =  FILTER_REF.REF_IDN  )
SELECT
md5(cast(
    
    coalesce(cast(SOURCE as 
    varchar
), '') || '-' || coalesce(cast(PHASE_APPLIED as 
    varchar
), '') || '-' || coalesce(cast(DISPOSITION as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY 
, * 
from EDIT_EOB_ENTRY