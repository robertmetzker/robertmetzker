---- SRC LAYER ----
WITH
SRC_DRG as ( SELECT *     from     STAGING.STG_DRG ),
//SRC_DRG as ( SELECT *     from     STG_DRG) ,

---- LOGIC LAYER ----

LOGIC_DRG as ( SELECT 
		  TRIM( DRG_CODE )                                   AS                                           DRG_CODE 
		, TRIM( PA_TYPE_DESC )                               AS                                       PA_TYPE_DESC 
		, TRIM( DRG_TYPE_DESC )                              AS                                      DRG_TYPE_DESC 
		, REVIEW                                     		 AS                                             REVIEW 
		, TRIM( DESCRIPTION )                                AS                                        DESCRIPTION 
		, EFFECTIVE_DATE                       				 AS                       				EFFECTIVE_DATE
		, EXPIRATION_DATE                					 AS                					   EXPIRATION_DATE
		from SRC_DRG
            )

---- RENAME LAYER ----
,

RENAME_DRG as ( SELECT 
		  DRG_CODE                                           as                                           DRG_CODE
		, PA_TYPE_DESC                                       as                                       PA_TYPE_DESC
		, DRG_TYPE_DESC                                      as                                      DRG_TYPE_DESC
		, REVIEW                                             as                                             REVIEW
		, DESCRIPTION                                        as                                        DESCRIPTION
		, EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                                    EXPIRATION_DATE 
				FROM     LOGIC_DRG   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DRG                            as ( SELECT * from    RENAME_DRG  
							QUALIFY (ROW_NUMBER()OVER(PARTITION BY DRG_CODE ORDER BY EFFECTIVE_DATE DESC))=1  ),

---- JOIN LAYER ----

 JOIN_DRG  as  ( SELECT * 
				FROM  FILTER_DRG ),
------ ETL LAYER ----------------

ETL AS ( SELECT md5(cast(
    
    coalesce(cast(DRG_CODE as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY, * FROM JOIN_DRG)
SELECT * FROM ETL