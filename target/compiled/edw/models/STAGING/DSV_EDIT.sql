

---- SRC LAYER ----
WITH
SRC_EDIT as ( SELECT *     from      STAGING.DST_EDIT ),
//SRC_EDIT as ( SELECT *     from     DST_EDIT) ,

---- LOGIC LAYER ----

LOGIC_EDIT as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, CODE                                               AS                                               CODE 
		, CATEGORY                                           AS                                           CATEGORY 
		, APPLIED_BY                                         AS                                         APPLIED_BY 
		, ENTRY_DATE                                         AS                                         ENTRY_DATE 
		, ENTRY_USER_ID                                      AS                                      ENTRY_USER_ID 
		, EFFECTIVE_DATE                                     AS                                     EFFECTIVE_DATE 
		, EXPIRATION_DATE                                    AS                                    EXPIRATION_DATE 
		, SHORT_DESCRIPTION                                  AS                                  SHORT_DESCRIPTION 
		, LONG_DESCRIPTION                                   AS                                   LONG_DESCRIPTION 
		, CATEGORY_DSC                                       AS                                       CATEGORY_DSC 
		, APPLIED_BY_DSC                                     AS                                     APPLIED_BY_DSC  
		, DATE_RANGE_TYPE_DSC                                AS                                DATE_RANGE_TYPE_DSC
		from SRC_EDIT
            )

---- RENAME LAYER ----
,

RENAME_EDIT as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CODE                                               as                                          EDIT_CODE
		, CATEGORY                                           as                                 EDIT_CATEGORY_CODE
		, APPLIED_BY                                         as                                         APPLIED_BY
		, ENTRY_DATE                                         as                                         ENTRY_DATE
		, ENTRY_USER_ID                                      as                                      ENTRY_USER_ID
		, EFFECTIVE_DATE                                     as                                EDIT_EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                                      EDIT_END_DATE
		, SHORT_DESCRIPTION                                  as                             EDIT_SHORT_DESCRIPTION
		, LONG_DESCRIPTION                                   as                              EDIT_LONG_DESCRIPTION
		, CATEGORY_DSC                                       as                          EDIT_CATEGORY_DESCRIPTION
		, APPLIED_BY_DSC                                     as                             APPLIED_BY_DESCRIPTION  
		, DATE_RANGE_TYPE_DSC                                AS                                   DATE_RANGE_DESC
				FROM     LOGIC_EDIT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_EDIT                           as ( SELECT * from    RENAME_EDIT   ),

---- JOIN LAYER ----

 JOIN_EDIT  as  ( SELECT * 
				FROM  FILTER_EDIT )
 SELECT * FROM  JOIN_EDIT