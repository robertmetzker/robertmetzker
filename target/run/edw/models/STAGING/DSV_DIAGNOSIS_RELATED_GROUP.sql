
  create or replace  view DEV_EDW.STAGING.DSV_DIAGNOSIS_RELATED_GROUP  as (
    

---- SRC LAYER ----
WITH
SRC_DRG as ( SELECT *     from     STAGING.DST_DIAGNOSIS_RELATED_GROUP ),
//SRC_DRG as ( SELECT *     from     DST_DIAGNOSIS_RELATED_GROUP) ,

---- LOGIC LAYER ----

LOGIC_DRG as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, DRG_CODE                                           AS                                           DRG_CODE 
		, PA_TYPE_DESC                                       AS                                       PA_TYPE_DESC 
		, DRG_TYPE_DESC                                      AS                                      DRG_TYPE_DESC 
		, REVIEW                                    		 AS                                             REVIEW 
		, DESCRIPTION                                        AS                                        DESCRIPTION 
		, EFFECTIVE_DATE									 AS                                     EFFECTIVE_DATE
		, EXPIRATION_DATE									 AS                                    EXPIRATION_DATE
		from SRC_DRG
            )

---- RENAME LAYER ----
,

RENAME_DRG as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, DRG_CODE                                           as                       DIAGNOSIS_RELATED_GROUP_CODE
		, PA_TYPE_DESC                                       as                                       PA_TYPE_DESC
		, DRG_TYPE_DESC                                      as                                      DRG_TYPE_DESC
		, REVIEW                                             as                                         REVIEW_IND
		, DESCRIPTION                                        as                      DIAGNOSIS_RELATED_GROUP_TITLE
		, EFFECTIVE_DATE                                     as                                 DRG_EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                                DRG_EXPIRATION_DATE 
				FROM     LOGIC_DRG   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DRG                            as ( SELECT * from    RENAME_DRG   ),

---- JOIN LAYER ----

 JOIN_DRG  as  ( SELECT * 
				FROM  FILTER_DRG )
 SELECT * FROM  JOIN_DRG
  );
