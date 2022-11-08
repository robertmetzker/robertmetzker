

      create or replace  table DEV_EDW.STAGING.STG_EDIT_EOB  as
      (---- SRC LAYER ----
WITH
SRC_EOB as ( SELECT *     from     DEV_VIEWS.BASE.EDIT_EOB ),
SRC_EER as ( SELECT *     from     DEV_VIEWS.BASE.REF ),
//SRC_EOB as ( SELECT *     from     EDIT_EOB) ,
//SRC_EER as ( SELECT *     from     REF) ,

---- LOGIC LAYER ----

LOGIC_EOB as ( SELECT 
		  EDIT_RID                                           AS                                           EDIT_RID 
		, EOB_RID                                            AS                                            EOB_RID 
		, cast( EFFECTIVE_DATE as DATE )                     AS                                     EFFECTIVE_DATE 
		, cast( EXPIRATION_DATE as DATE )                    AS                                    EXPIRATION_DATE 
		, upper( TRIM( RELATIONSHIP_TYPE ) )                 AS                                  RELATIONSHIP_TYPE 
		, cast( ENTRY_DATE as DATE )                         AS                                         ENTRY_DATE 
		, upper( TRIM( ENTRY_USER_ID ) )                     AS                                      ENTRY_USER_ID 
		, cast( DLM as DATE )                                AS                                                DLM 
		, upper( ULM )                                       AS                                                ULM 
		from SRC_EOB
            ),
LOGIC_EER as ( SELECT 
		  upper( TRIM( REF_IDN ) )                           AS                                            REF_IDN 
		, upper( TRIM( REF_DGN ) )                           AS                                            REF_DGN 
		, upper( TRIM( REF_DSC ) )                           AS                                            REF_DSC 
		, cast( REF_EXP_DTE as DATE )                        AS                                        REF_EXP_DTE 
		from SRC_EER
            )

---- RENAME LAYER ----
,

RENAME_EOB as ( SELECT 
		  EDIT_RID                                           as                                           EDIT_RID
		, EOB_RID                                            as                                            EOB_RID
		, EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                                    EXPIRATION_DATE
		, RELATIONSHIP_TYPE                                  as                                  RELATIONSHIP_TYPE
		, ENTRY_DATE                                         as                                         ENTRY_DATE
		, ENTRY_USER_ID                                      as                                      ENTRY_USER_ID
		, DLM                                                as                                                DLM
		, ULM                                                as                                                ULM 
				FROM     LOGIC_EOB   ), 
RENAME_EER as ( SELECT 
		  REF_IDN                                            as                                        EER_REF_IDN
		, REF_DGN                                            as                                        EER_REF_DGN
		, REF_DSC                                            as                             RELATIONSHIP_TYPE_DESC
		, REF_EXP_DTE                                        as                                    EER_REF_EXP_DTE 
				FROM     LOGIC_EER   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_EOB                            as ( SELECT * from    RENAME_EOB   ),
FILTER_EER                            as ( SELECT * from    RENAME_EER 
				WHERE EER_REF_DGN = 'EER' AND EER_REF_EXP_DTE > current_date  ),

---- JOIN LAYER ----

EOB as ( SELECT * 
				FROM  FILTER_EOB
				LEFT JOIN FILTER_EER ON  FILTER_EOB.RELATIONSHIP_TYPE =  FILTER_EER.EER_REF_IDN  )
SELECT * 
from EOB
      );
    