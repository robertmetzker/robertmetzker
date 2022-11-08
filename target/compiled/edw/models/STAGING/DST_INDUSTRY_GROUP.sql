---- SRC LAYER ----
WITH
SRC_IG             as ( SELECT *     FROM     STAGING.STG_INDUSTRY_GROUP ),
//SRC_IG             as ( SELECT *     FROM     STG_INDUSTRY_GROUP) ,

---- LOGIC LAYER ----


LOGIC_IG as ( SELECT 
		  TRIM( SIC_TYP_CD )                                 as                                         SIC_TYP_CD 
		, TRIM( SIC_TYP_NM )                                 as                                         SIC_TYP_NM 
		, SIC_TYP_EFF_DT                                     as                                     SIC_TYP_EFF_DT 
		, SIC_TYP_END_DT                                     as                                     SIC_TYP_END_DT 
		, TRIM( SIC_TYP_VOID_IND )                           as                                   SIC_TYP_VOID_IND 
		FROM SRC_IG
            )

---- RENAME LAYER ----
,

RENAME_IG         as ( SELECT 
		  SIC_TYP_CD                                         as                                         SIC_TYP_CD
		, SIC_TYP_NM                                         as                                         SIC_TYP_NM
		, SIC_TYP_EFF_DT                                     as                                     SIC_TYP_EFF_DT
		, SIC_TYP_END_DT                                     as                                     SIC_TYP_END_DT
		, SIC_TYP_VOID_IND                                   as                                   SIC_TYP_VOID_IND 
				FROM     LOGIC_IG   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_IG                             as ( SELECT * FROM    RENAME_IG 
                                            WHERE SIC_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

 JOIN_IG          as  ( SELECT * 
				FROM  FILTER_IG ),

------ETL LAYER------------
ETL AS(SELECT md5(cast(
    
    coalesce(cast(SIC_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
,SIC_TYP_CD
,SIC_TYP_NM
,SIC_TYP_EFF_DT
,SIC_TYP_END_DT
,SIC_TYP_VOID_IND
FROM JOIN_IG
QUALIFY (ROW_NUMBER() OVER (PARTITION BY SIC_TYP_CD ORDER BY SIC_TYP_END_DT DESC) ) = 1)

SELECT * FROM ETL