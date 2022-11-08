---- SRC LAYER ----
WITH
SRC_REF as ( SELECT *     from     STAGING.STG_CAM_REF ),
//SRC_REF as ( SELECT *     from     STG_CAM_REF) ,

---- LOGIC LAYER ----

LOGIC_REF as ( SELECT 
		   md5(cast(
    
    coalesce(cast(REF_IDN as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                                      UNIQUE_ID_KEY 
		, TRIM( REF_IDN )                                    as                              PLACE_OF_SERVICE_CODE 
		, TRIM( REF_DSC )                                    as                              PLACE_OF_SERVICE_DESC 
		, case when PLACE_OF_SERVICE_CODE IN  ('01', '03', '04', '12', '15', '16', '17', '18', '41', '42', '60', '99') THEN 'Y' ELSE 'N' END
                                                             as                                  OUT_OF_OFFICE_IND 
		, REF_EFF_DTE                                        as                    PLACE_OF_SERVICE_EFFECTIVE_DATE 
		, REF_EXP_DTE                                        as                          PLACE_OF_SERVICE_END_DATE 
		, TRIM( REF_DGN )                                    as                                            REF_DGN 
		, CONDITIONAL_CHANGE_EVENT(PLACE_OF_SERVICE_DESC)OVER(PARTITION BY PLACE_OF_SERVICE_CODE ORDER BY PLACE_OF_SERVICE_EFFECTIVE_DATE, PLACE_OF_SERVICE_END_DATE) ETL_SORT_ORDR
		from SRC_REF
		WHERE REF_DGN = 'POS'
            )

---- RENAME LAYER ----
,

RENAME_REF as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, PLACE_OF_SERVICE_CODE                              as                              PLACE_OF_SERVICE_CODE
		, PLACE_OF_SERVICE_DESC                              as                              PLACE_OF_SERVICE_DESC
		, OUT_OF_OFFICE_IND                                  as                                  OUT_OF_OFFICE_IND
		, PLACE_OF_SERVICE_EFFECTIVE_DATE                    as                    PLACE_OF_SERVICE_EFFECTIVE_DATE
		, PLACE_OF_SERVICE_END_DATE                          as                          PLACE_OF_SERVICE_END_DATE
		, REF_DGN                                            as                                            REF_DGN
		, ETL_SORT_ORDR                                      as                                      ETL_SORT_ORDR  
				FROM     LOGIC_REF   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_REF                            as ( SELECT * from    RENAME_REF 
                                             ),

---- JOIN LAYER ----

 JOIN_REF  as  ( SELECT * 
				FROM  FILTER_REF ),

-- ETL LAYER FOR MERGE				
 PLC_SRVC AS (SELECT 
   UNIQUE_ID_KEY                     
, PLACE_OF_SERVICE_CODE             
, PLACE_OF_SERVICE_DESC             
, OUT_OF_OFFICE_IND                 
, MIN(PLACE_OF_SERVICE_EFFECTIVE_DATE )  PLACE_OF_SERVICE_EFFECTIVE_DATE
, MAX(PLACE_OF_SERVICE_END_DATE)    PLACE_OF_SERVICE_END_DATE
, REF_DGN                           
  FROM  JOIN_REF
  GROUP BY 1,2,3,4,7, ETL_SORT_ORDR)
  
SELECT 
 UNIQUE_ID_KEY                     
, PLACE_OF_SERVICE_CODE             
, PLACE_OF_SERVICE_DESC             
, OUT_OF_OFFICE_IND                 
, PLACE_OF_SERVICE_EFFECTIVE_DATE
, NULLIF( PLACE_OF_SERVICE_END_DATE, '2099-12-31')  AS PLACE_OF_SERVICE_END_DATE
, REF_DGN
FROM PLC_SRVC
QUALIFY(ROW_NUMBER()OVER (PARTITION BY PLACE_OF_SERVICE_CODE ORDER BY PLACE_OF_SERVICE_EFFECTIVE_DATE DESC, PLACE_OF_SERVICE_END_DATE DESC))=1