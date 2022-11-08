

---- SRC LAYER ----
WITH
SRC_POS as ( SELECT *     from     STAGING.DST_PLACE_OF_SERVICE ),
//SRC_POS as ( SELECT *     from     DST_PLACE_OF_SERVICE) ,

---- LOGIC LAYER ----

LOGIC_POS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, PLACE_OF_SERVICE_CODE                              as                         LINE_PLACE_OF_SERVICE_CODE 
		, PLACE_OF_SERVICE_DESC                              as                         LINE_PLACE_OF_SERVICE_DESC 
		, OUT_OF_OFFICE_IND                                  as                                  OUT_OF_OFFICE_IND 
		, PLACE_OF_SERVICE_EFFECTIVE_DATE                    as                    PLACE_OF_SERVICE_EFFECTIVE_DATE 
		, PLACE_OF_SERVICE_END_DATE                          as                          PLACE_OF_SERVICE_END_DATE 
		from SRC_POS
            )

---- RENAME LAYER ----
,

RENAME_POS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, LINE_PLACE_OF_SERVICE_CODE                         as                         LINE_PLACE_OF_SERVICE_CODE
		, LINE_PLACE_OF_SERVICE_DESC                         as                         LINE_PLACE_OF_SERVICE_DESC
		, OUT_OF_OFFICE_IND                                  as                                  OUT_OF_OFFICE_IND
		, PLACE_OF_SERVICE_EFFECTIVE_DATE                    as                    PLACE_OF_SERVICE_EFFECTIVE_DATE
		, PLACE_OF_SERVICE_END_DATE                          as                          PLACE_OF_SERVICE_END_DATE 
				FROM     LOGIC_POS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_POS                            as ( SELECT * from    RENAME_POS ),

---- JOIN LAYER ----

 JOIN_POS  as  ( SELECT * 
				FROM  FILTER_POS )
 SELECT * FROM  JOIN_POS