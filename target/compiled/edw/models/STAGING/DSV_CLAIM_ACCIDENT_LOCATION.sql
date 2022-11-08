

---- SRC LAYER ----
WITH
SRC_C as ( SELECT *     from     STAGING.DST_CLAIM_ACCIDENT_LOCATION ),
//SRC_C as ( SELECT *     from     DST_CLAIM_ACCIDENT_LOCATION) ,

---- LOGIC LAYER ----

LOGIC_C as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, OCCR_PRMS_TYP_NM                                   as                                   OCCR_PRMS_TYP_NM 
		, CLM_OCCR_LOC_CNTRY_NM                              as                              CLM_OCCR_LOC_CNTRY_NM 
		, CLM_OCCR_LOC_STT_CD                                as                                CLM_OCCR_LOC_STT_CD 
		, CLM_OCCR_LOC_STT_NM                                as                                CLM_OCCR_LOC_STT_NM 
		, CLM_OCCR_LOC_CNTY_NM                               as                               CLM_OCCR_LOC_CNTY_NM 
		, CLM_OCCR_LOC_CITY_NM                               as                               CLM_OCCR_LOC_CITY_NM 
		, CLM_OCCR_LOC_POST_CD                               as                               CLM_OCCR_LOC_POST_CD 
		, CLM_OCCR_LOC_NM                                    as                                    CLM_OCCR_LOC_NM 
		, CLM_OCCR_LOC_STR_1                                 as                                 CLM_OCCR_LOC_STR_1 
		, CLM_OCCR_LOC_STR_2                                 as                                 CLM_OCCR_LOC_STR_2 
		, CLM_OCCR_LOC_COMT                                  as                                  CLM_OCCR_LOC_COMT 
		from SRC_C
            )

---- RENAME LAYER ----
,

RENAME_C as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, OCCR_PRMS_TYP_NM                                   as                         ACCIDENT_PREMISE_TYPE_DESC
		, CLM_OCCR_LOC_CNTRY_NM                              as                          ACCIDENT_LOCATION_COUNTRY
		, CLM_OCCR_LOC_STT_CD                                as                       ACCIDENT_LOCATION_STATE_CODE
		, CLM_OCCR_LOC_STT_NM                                as                            ACCIDENT_LOCATION_STATE
		, CLM_OCCR_LOC_CNTY_NM                               as                           ACCIDENT_LOCATION_COUNTY
		, CLM_OCCR_LOC_CITY_NM                               as                             ACCIDENT_LOCATION_CITY
		, CLM_OCCR_LOC_POST_CD                               as                         ACCIDENT_LOCATION_ZIP_CODE
		, CLM_OCCR_LOC_NM                                    as                             ACCIDENT_LOCATION_NAME
		, CLM_OCCR_LOC_STR_1                                 as                            ACCIDENT_ADDRESS_LINE_1
		, CLM_OCCR_LOC_STR_2                                 as                            ACCIDENT_ADDRESS_LINE_2
		, CLM_OCCR_LOC_COMT                                  as                          ACCIDENT_LOCATION_COMMENT 
				FROM     LOGIC_C   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_C                              as ( SELECT * from    RENAME_C   ),

---- JOIN LAYER ----

 JOIN_C  as  ( SELECT * 
				FROM  FILTER_C )
 SELECT 
  UNIQUE_ID_KEY
, ACCIDENT_PREMISE_TYPE_DESC
, ACCIDENT_LOCATION_COUNTRY
, ACCIDENT_LOCATION_STATE_CODE
, ACCIDENT_LOCATION_STATE
, ACCIDENT_LOCATION_COUNTY
, ACCIDENT_LOCATION_CITY
, ACCIDENT_LOCATION_ZIP_CODE
, ACCIDENT_LOCATION_NAME
, ACCIDENT_ADDRESS_LINE_1
, ACCIDENT_ADDRESS_LINE_2
, ACCIDENT_LOCATION_COMMENT
FROM  JOIN_C