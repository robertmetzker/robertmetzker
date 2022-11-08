

      create or replace  table DEV_EDW.STAGING.DST_EDIT  as
      (---- SRC LAYER ----
WITH
SRC_EDIT as ( SELECT *     from      STAGING.STG_EDIT ),
//SRC_EDIT as ( SELECT *     from     STG_EDIT) ,

---- LOGIC LAYER ----

LOGIC_EDIT as ( SELECT 
		  EDIT_RID                                           AS                                           EDIT_RID 
		, TRIM( CODE )                                       AS                                               CODE 
		, TRIM( CATEGORY )                                   AS                                           CATEGORY 
		, TRIM( APPLIED_BY )                                 AS                                         APPLIED_BY 
		, ENTRY_DATE                                         AS                                         ENTRY_DATE 
		, TRIM( ENTRY_USER_ID )                              AS                                      ENTRY_USER_ID 
		, TRIM( DATE_RANGE_TYPE )                            AS                                    DATE_RANGE_TYPE 
		, GEN_EDIT_RID                                       AS                                       GEN_EDIT_RID 
		, EFFECTIVE_DATE                                     AS                                     EFFECTIVE_DATE 
		, CASE WHEN EXPIRATION_DATE > CURRENT_DATE THEN NULL ELSE  EXPIRATION_DATE END
						                                     AS                                    EXPIRATION_DATE 
		, TRIM( DISPOSITION )                                AS                                        DISPOSITION 
		, TRIM( SHORT_DESCRIPTION )                          AS                                  SHORT_DESCRIPTION 
		, TRIM( LONG_DESCRIPTION )                           AS                                   LONG_DESCRIPTION 
		, GEN_ENTRY_DATE                                     AS                                     GEN_ENTRY_DATE 
		, TRIM( GEN_ENTRY_USER_ID )                          AS                                  GEN_ENTRY_USER_ID 
		, DLM                                                AS                                                DLM 
		, TRIM( ULM )                                        AS                                                ULM 
		, TRIM( HPPN_APPLICABLE )                            AS                                    HPPN_APPLICABLE 
		, EDITOR_PHASE                                       AS                                       EDITOR_PHASE 
		, PAYMENT_PCT                                        AS                                        PAYMENT_PCT 
		, TRIM( CATEGORY_DSC )                               AS                                       CATEGORY_DSC 
		, TRIM( DATE_RANGE_TYPE_DSC )                        AS                                DATE_RANGE_TYPE_DSC 
		, TRIM( APPLIED_BY_DSC )                             AS                                     APPLIED_BY_DSC 
		, TRIM( DISPOSITION_DSC )                            AS                                    DISPOSITION_DSC 
		from SRC_EDIT
            )

---- RENAME LAYER ----
,

RENAME_EDIT as ( SELECT 
		  EDIT_RID                                           as                                           EDIT_RID
		, CODE                                               as                                               CODE
		, CATEGORY                                           as                                           CATEGORY
		, APPLIED_BY                                         as                                         APPLIED_BY
		, ENTRY_DATE                                         as                                         ENTRY_DATE
		, ENTRY_USER_ID                                      as                                      ENTRY_USER_ID
		, DATE_RANGE_TYPE                                    as                                    DATE_RANGE_TYPE
		, GEN_EDIT_RID                                       as                                       GEN_EDIT_RID
		, EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                                    EXPIRATION_DATE
		, DISPOSITION                                        as                                        DISPOSITION
		, SHORT_DESCRIPTION                                  as                                  SHORT_DESCRIPTION
		, LONG_DESCRIPTION                                   as                                   LONG_DESCRIPTION
		, GEN_ENTRY_DATE                                     as                                     GEN_ENTRY_DATE
		, GEN_ENTRY_USER_ID                                  as                                  GEN_ENTRY_USER_ID
		, DLM                                                as                                                DLM
		, ULM                                                as                                                ULM
		, HPPN_APPLICABLE                                    as                                    HPPN_APPLICABLE
		, EDITOR_PHASE                                       as                                       EDITOR_PHASE
		, PAYMENT_PCT                                        as                                        PAYMENT_PCT
		, CATEGORY_DSC                                       as                                       CATEGORY_DSC
		, DATE_RANGE_TYPE_DSC                                as                                DATE_RANGE_TYPE_DSC
		, APPLIED_BY_DSC                                     as                                     APPLIED_BY_DSC
		, DISPOSITION_DSC                                    as                                    DISPOSITION_DSC 
				FROM     LOGIC_EDIT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_EDIT                           as ( SELECT * from    RENAME_EDIT  
QUALIFY(ROW_NUMBER() OVER(PARTITION BY CODE ORDER BY EFFECTIVE_DATE DESC)) =1 ),

---- JOIN LAYER ----

 JOIN_EDIT  as  ( SELECT * 
				FROM  FILTER_EDIT )
 SELECT 
 md5(cast(
    
    coalesce(cast(CODE as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
 ,  * 
 FROM  JOIN_EDIT
      );
    