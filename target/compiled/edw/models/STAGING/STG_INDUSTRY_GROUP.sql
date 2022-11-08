---- SRC LAYER ----
WITH
SRC_SIC            as ( SELECT *     from     DEV_VIEWS.PCMP.SIC_TYPE ),
//SRC_SIC            as ( SELECT *     FROM     SIC_TYPE) ,

---- LOGIC LAYER ----


LOGIC_SIC as ( SELECT 
		  upper( TRIM( SIC_TYP_CD ) )                        as                                         SIC_TYP_CD 
		, upper( TRIM( SIC_TYP_NM ) )                        as                                         SIC_TYP_NM 
		, cast( SIC_TYP_EFF_DT as DATE )                     as                                     SIC_TYP_EFF_DT 
		, cast( SIC_TYP_END_DT as DATE )                     as                                     SIC_TYP_END_DT 
		, upper( SIC_TYP_VOID_IND )                          as                                   SIC_TYP_VOID_IND 
		FROM SRC_SIC
            )

---- RENAME LAYER ----
,

RENAME_SIC        as ( SELECT 
		  SIC_TYP_CD                                         as                                         SIC_TYP_CD
		, SIC_TYP_NM                                         as                                         SIC_TYP_NM
		, SIC_TYP_EFF_DT                                     as                                     SIC_TYP_EFF_DT
		, SIC_TYP_END_DT                                     as                                     SIC_TYP_END_DT
		, SIC_TYP_VOID_IND                                   as                                   SIC_TYP_VOID_IND 
				FROM     LOGIC_SIC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_SIC                            as ( SELECT * FROM    RENAME_SIC   ),

---- JOIN LAYER ----

 JOIN_SIC         as  ( SELECT * 
				FROM  FILTER_SIC )
 SELECT * FROM  JOIN_SIC