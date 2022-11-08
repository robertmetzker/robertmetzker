---- SRC LAYER ----
WITH
SRC_ICD as ( SELECT *     from     STAGING.STG_ICD_PROCEDURE ),
SRC_REF as ( SELECT *     from     STAGING.STG_CAM_REF ),
//SRC_ICD as ( SELECT *     from     STG_ICD_PROCEDURE) ,
//SRC_REF as ( SELECT *     from     STG_CAM_REF) ,

---- LOGIC LAYER ----

LOGIC_ICD as ( SELECT 
                                  
              
          md5(cast(
    
    coalesce(cast(CODE as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
		, TRIM( CODE )                                       AS                                               CODE 
		, VERSION                                            AS                                            VERSION 
		, TRIM( ICD_TYPE )                                   AS                                           ICD_TYPE 
		, TRIM( DESCRIPTION )                                AS                                        DESCRIPTION 
		, EFFECTIVE_DATE                                     AS                                     EFFECTIVE_DATE 
		, EXPIRATION_DATE                                    AS                                    EXPIRATION_DATE 
		, ENTRY_DATE                                         AS                                         ENTRY_DATE 
		, TRIM( GENDER )                                     AS                                             GENDER 
		, TRIM( COVERED_FLAG )                               AS                                       COVERED_FLAG 
		, MIN_AGE                                            AS                                            MIN_AGE 
		, MAX_AGE                                            AS                                            MAX_AGE 
		from SRC_ICD
            ),
LOGIC_REF as ( SELECT 
		  TRIM( REF_DSC )                                    AS                                            REF_DSC 
		, TRIM( REF_DGN )                                    AS                                            REF_DGN 
		, TRIM( REF_IDN )                                    AS                                            REF_IDN 
		from SRC_REF
            )

---- RENAME LAYER ----
,

RENAME_ICD as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CODE                                               as                                               CODE
		, VERSION                                            as                                            VERSION
		, ICD_TYPE                                           as                                           ICD_TYPE
		, DESCRIPTION                                        as                                        DESCRIPTION
		, EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                                    EXPIRATION_DATE
		, ENTRY_DATE                                         as                                         ENTRY_DATE
		, GENDER                                             as                                             GENDER
		, COVERED_FLAG                                       as                                       COVERED_FLAG
		, MIN_AGE                                            as                                            MIN_AGE
		, MAX_AGE                                            as                                            MAX_AGE 
				FROM     LOGIC_ICD   ), 
RENAME_REF as ( SELECT 
		  REF_DSC                                            as                                        GENDER_DESC
		, REF_DGN                                            as                                            REF_DGN
		, REF_IDN                                            as                                            REF_IDN 
				FROM     LOGIC_REF   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ICD                            as ( SELECT * from    RENAME_ICD   ),
FILTER_REF                            as ( SELECT * from    RENAME_REF 
				WHERE REF_DGN = 'GNA'  ),

---- JOIN LAYER ----

ICD as ( SELECT * 
				FROM  FILTER_ICD
				LEFT JOIN FILTER_REF ON  FILTER_ICD.GENDER =  FILTER_REF.REF_IDN 
				 ),

-- ETL Layer --
 ETL AS ( select *,  row_number() over(PARTITION BY CODE ORDER BY EFFECTIVE_DATE DESC) AS ROWN
           from ICD 
		   qualify ROWN = 1
 				)

 SELECT * 
from ETL