
  create or replace  view DEV_EDW.STAGING.DSV_ICD_ADMISSION_PRESENCE  as (
    

---- SRC LAYER ----
WITH
SRC_REF as ( SELECT *     from     STAGING.DST_ICD_ADMISSION_PRESENCE ),
//SRC_REF as ( SELECT *     from     DST_ICD_ADMISSION_PRESENCE) ,

---- LOGIC LAYER ----

LOGIC_REF as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, REF_IDN                                            AS                                            REF_IDN 
		, REF_DSC                                            AS                                            REF_DSC 
		from SRC_REF
            )

---- RENAME LAYER ----
,

RENAME_REF as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, REF_IDN                                            as                          PRESENT_ON_ADMISSION_CODE
		, REF_DSC                                            as                          PRESENT_ON_ADMISSION_DESC 
				FROM     LOGIC_REF   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_REF                            as ( SELECT * from    RENAME_REF   ),

---- JOIN LAYER ----

 JOIN_REF  as  ( SELECT * 
				FROM  FILTER_REF )
 SELECT * FROM  JOIN_REF
  );
