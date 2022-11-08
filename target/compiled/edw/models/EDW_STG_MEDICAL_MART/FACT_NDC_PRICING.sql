

---- SRC LAYER ----
WITH
SRC_NDC as ( SELECT *     from     EDW_STAGING_DIM.DIM_NDC ),
SRC_NDC_PRICE as ( SELECT *     from     STAGING.DSV_NDC_DRUG_PRICING ),
//SRC_NDC as ( SELECT *     from     DIM_NDC) ,
//SRC_NDC_PRICE as ( SELECT *     from     DSV_NDC_DRUG_PRICING) ,

---- LOGIC LAYER ----

LOGIC_NDC as ( SELECT 
		  NDC_GPI_HKEY                                       as                                       NDC_GPI_HKEY 
		, TRIM(NDC_11_CODE)                                  as                                        NDC_11_CODE 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE
		, NDC_VERSION_NUMBER                                 as                                 NDC_VERSION_NUMBER 

		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
		from SRC_NDC
            ),
LOGIC_NDC_PRICE as ( SELECT 
		  PRICE_TYPE_CODE                                    as                                    PRICE_TYPE_CODE
		,  NDC_VERSION_NUMBER                                as                                 NDC_VERSION_NUMBER
		, md5(cast(
    
    coalesce(cast(PRICE_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
)) as                                  PRICING_TYPE_HKEY 
		, EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE 
		, case when EFFECTIVE_DATE is null then -1
			when replace(cast(EFFECTIVE_DATE::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
			when replace(cast(EFFECTIVE_DATE::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
			else replace(cast(EFFECTIVE_DATE::DATE as varchar),'-','')::INTEGER 
		END 												 as 								EFFECTIVE_DATE_KEY
		, END_DATE                                           as                                           END_DATE 
		, case when END_DATE is null then -1
			when replace(cast(END_DATE::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
			when replace(cast(END_DATE::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
			else replace(cast(END_DATE::DATE as varchar),'-','')::INTEGER 
		END 												 as 								      END_DATE_KEY 		
		, DRUG_AMOUNT                                        as                                        DRUG_AMOUNT 
		, NDC_11_CODE                                        as                                        NDC_11_CODE 
		from SRC_NDC_PRICE
            )

---- RENAME LAYER ----
,

RENAME_NDC as ( SELECT 
		  NDC_GPI_HKEY                                       as                                       NDC_GPI_HKEY
		, NDC_11_CODE                                        as                                    DIM_NDC_11_CODE
		, RECORD_EFFECTIVE_DATE                              as                          NDC_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                NDC_RECORD_END_DATE
		, CURRENT_RECORD_IND                                 as                             NDC_CURRENT_RECORD_IND
		, NDC_VERSION_NUMBER                                 as                             NDC_NDC_VERSION_NUMBER 
				FROM     LOGIC_NDC   ), 
RENAME_NDC_PRICE as ( SELECT 
		  PRICING_TYPE_HKEY                                  as                                  PRICING_TYPE_HKEY
		, EFFECTIVE_DATE_KEY                                 as                                 EFFECTIVE_DATE_KEY
		, END_DATE_KEY                                       as                                       END_DATE_KEY
		, DRUG_AMOUNT                                        as                                        DRUG_AMOUNT
		, NDC_VERSION_NUMBER                                 as                                 NDC_VERSION_NUMBER
		, PRICE_TYPE_CODE                                    as                                    PRICE_TYPE_CODE
		, EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE
		, NDC_11_CODE                                        as                              DIM_PRICE_NDC_11_CODE                     
				FROM     LOGIC_NDC_PRICE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_NDC_PRICE                      as ( SELECT * from    RENAME_NDC_PRICE 
                                   WHERE EFFECTIVE_DATE_KEY< END_DATE_KEY   ),
FILTER_NDC                            as ( SELECT * from    RENAME_NDC   ),

---- JOIN LAYER ----

NDC_PRICE as ( SELECT * 
				FROM  FILTER_NDC_PRICE
				INNER JOIN FILTER_NDC ON  coalesce( FILTER_NDC_PRICE.DIM_PRICE_NDC_11_CODE, '00000000000') =  FILTER_NDC.DIM_NDC_11_CODE
				 AND EFFECTIVE_DATE BETWEEN NDC_RECORD_EFFECTIVE_DATE AND coalesce( NDC_RECORD_END_DATE, '2099-12-31'))

-- ETL join layer to handle that are outside of date range MD5('-2222')    
,
ETL_FLF as ( SELECT FIL_NDC_PRICE.* 
					, NDC_DATERANGE.DIM_NDC_11_CODE AS NDC_DATERANGE_NDC_11_CODE
					, NDC_DATERANGE.NDC_GPI_HKEY AS NDC_DATERANGE_NDC_GPI_HKEY
				FROM  NDC_PRICE FIL_NDC_PRICE
				LEFT JOIN FILTER_NDC NDC_DATERANGE ON  coalesce( FIL_NDC_PRICE.DIM_PRICE_NDC_11_CODE, 'Z') =  NDC_DATERANGE.DIM_NDC_11_CODE
				 AND NDC_DATERANGE.NDC_CURRENT_RECORD_IND = 'Y' )


SELECT distinct 
	 (CASE WHEN NDC_GPI_HKEY IS NOT NULL THEN NDC_GPI_HKEY
                WHEN NDC_DATERANGE_NDC_GPI_HKEY IS NOT NULL THEN md5(cast(
    
    coalesce(cast(-2222 as 
    varchar
), '')

 as 
    varchar
))
				ELSE md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
)) END)::VARCHAR(32) AS NDC_GPI_HKEY
   , NDC_VERSION_NUMBER			
   , coalesce(PRICING_TYPE_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS PRICING_TYPE_HKEY
   , EFFECTIVE_DATE_KEY
   , END_DATE_KEY
   , DRUG_AMOUNT
   , CURRENT_TIMESTAMP() AS LOAD_DATETIME
   , try_to_timestamp('Invalid') AS UPDATE_DATETIME
   , 'BWCODS/RNP' AS PRIMARY_SOURCE_SYSTEM 
from ETL_FLF