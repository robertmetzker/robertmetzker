

---- SRC LAYER ----
WITH
SRC_AC as ( SELECT *     from     STAGING.DST_ACTIVITY ),
//SRC_AC as ( SELECT *     from     DST_ACTIVITY) ,

---- LOGIC LAYER ----


LOGIC_AC as ( SELECT 
		   md5(cast(
    
    coalesce(cast(ACTV_DTL_DESC as 
    varchar
), '')

 as 
    varchar
))  as                    UNIQUE_ID_KEY                
		  ,ACTV_DTL_DESC                                                  as                    ACTV_DTL_DESC 
		from SRC_AC
            )

---- RENAME LAYER ----
,

RENAME_AC as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, ACTV_DTL_DESC                                      as                               ACTIVITY_DETAIL_DESC 
				FROM     LOGIC_AC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_AC                             as ( SELECT * from    RENAME_AC 
                                            WHERE ACTIVITY_DETAIL_DESC is not null  ),

---- ETL LAYER ----

 ETL  as  ( SELECT DISTINCT ACTIVITY_DETAIL_DESC,UNIQUE_ID_KEY
				FROM  FILTER_AC )
 SELECT * FROM  ETL