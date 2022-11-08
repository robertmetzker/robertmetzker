
  create or replace  view DEV_EDW.STAGING.DSV_HOSPITAL_ICD_PROCEDURE  as (
    

---- SRC LAYER ----
WITH
SRC_ICD as ( SELECT *     from     STAGING.DST_HOSPITAL_ICD_PROCEDURE ),
//SRC_ICD as ( SELECT *     from     DST_HOSPITAL_ICD_PROCEDURE) ,

---- LOGIC LAYER ----

LOGIC_ICD as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, CODE                                               AS                                               CODE 
		, DESCRIPTION                                        AS                                        DESCRIPTION 
		, VERSION                                            AS                                            VERSION 
		, GENDER                                             AS                                             GENDER 
		, GENDER_DESC                                        AS                                        GENDER_DESC 
		, MIN_AGE                                            AS                                            MIN_AGE 
		, MAX_AGE                                            AS                                            MAX_AGE 
		, COVERED_FLAG                                       AS                                       COVERED_FLAG 
		, EFFECTIVE_DATE                                     AS                                     EFFECTIVE_DATE 
		, EXPIRATION_DATE                                    AS                                    EXPIRATION_DATE 
		from SRC_ICD
            )

---- RENAME LAYER ----
,

RENAME_ICD as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CODE                                               as                                 ICD_PROCEDURE_CODE
		, DESCRIPTION                                        as                                      ICD_CODE_DESC
		, VERSION                                            as                            ICD_CODE_VERSION_NUMBER
		, GENDER                                             as                               GENDER_SPECIFIC_CODE
		, GENDER_DESC                                        as                               GENDER_SPECIFIC_DESC
		, MIN_AGE                                            as                                            MIN_AGE
		, MAX_AGE                                            as                                            MAX_AGE
		, COVERED_FLAG                                       as                                       COVERED_FLAG
		, EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                                    EXPIRATION_DATE 
				FROM     LOGIC_ICD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ICD                            as ( SELECT * from    RENAME_ICD   ),

---- JOIN LAYER ----

 JOIN_ICD  as  ( SELECT * 
				FROM  FILTER_ICD )
 SELECT * FROM  JOIN_ICD
  );
