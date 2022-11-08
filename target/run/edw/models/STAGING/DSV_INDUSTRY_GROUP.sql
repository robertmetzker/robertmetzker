
  create or replace  view DEV_EDW.STAGING.DSV_INDUSTRY_GROUP  as (
    

---- SRC LAYER ----
WITH
SRC_IG             as ( SELECT *     FROM     STAGING.DST_INDUSTRY_GROUP ),
//SRC_IG             as ( SELECT *     FROM     DST_INDUSTRY_GROUP) ,

---- LOGIC LAYER ----


LOGIC_IG as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, SIC_TYP_CD                                         as                                         SIC_TYP_CD 
		, SIC_TYP_NM                                         as                                         SIC_TYP_NM 
		, SIC_TYP_EFF_DT                                     as                                     SIC_TYP_EFF_DT 
		, SIC_TYP_END_DT                                     as                                     SIC_TYP_END_DT 
		FROM SRC_IG
            )

---- RENAME LAYER ----
,

RENAME_IG         as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, SIC_TYP_CD                                         as                                INDUSTRY_GROUP_CODE
		, SIC_TYP_NM                                         as                                INDUSTRY_GROUP_DESC
		, SIC_TYP_EFF_DT                                     as                      INDUSTRY_GROUP_EFFECTIVE_DATE
		, SIC_TYP_END_DT                                     as                            INDUSTRY_GROUP_END_DATE 
				FROM     LOGIC_IG   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_IG                             as ( SELECT * FROM    RENAME_IG   ),

---- JOIN LAYER ----

 JOIN_IG          as  ( SELECT * 
				FROM  FILTER_IG )
 SELECT * FROM  JOIN_IG
  );
