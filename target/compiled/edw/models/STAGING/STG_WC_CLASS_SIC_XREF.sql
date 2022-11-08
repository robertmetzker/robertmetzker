---- SRC LAYER ----
WITH
SRC_X as ( SELECT *     from     DEV_VIEWS.PCMP.WC_CLASS_SIC_XREF ),
SRC_S as ( SELECT *     from     DEV_VIEWS.PCMP.SIC_TYPE ),
//SRC_X as ( SELECT *     from     WC_CLASS_SIC_XREF) ,
//SRC_S as ( SELECT *     from     SIC_TYPE) ,

---- LOGIC LAYER ----

LOGIC_X as ( SELECT 
		  upper( TRIM( WC_CLS_SIC_XREF_CLS_CD ) )            AS                             WC_CLS_SIC_XREF_CLS_CD 
		, upper( TRIM( SIC_TYP_CD ) )                        AS                                         SIC_TYP_CD 
		, cast( WC_CLS_SIC_XREF_EFF_DT as DATE )             AS                             WC_CLS_SIC_XREF_EFF_DT 
		, cast( WC_CLS_SIC_XREF_END_DT as DATE )             AS                             WC_CLS_SIC_XREF_END_DT 
		, WC_CLS_SIC_XREF_ID                                 AS                                 WC_CLS_SIC_XREF_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_X
            ),
LOGIC_S as ( SELECT 
		  upper( TRIM( SIC_TYP_NM ) )                        AS                                         SIC_TYP_NM 
		, upper( TRIM( SIC_TYP_CD ) )                        AS                                         SIC_TYP_CD 
		, upper( SIC_TYP_VOID_IND )                          AS                                   SIC_TYP_VOID_IND 
		from SRC_S
            )

---- RENAME LAYER ----
,

RENAME_X as ( SELECT 
		  WC_CLS_SIC_XREF_CLS_CD                             as                             WC_CLS_SIC_XREF_CLS_CD
		, SIC_TYP_CD                                         as                                         SIC_TYP_CD
		, WC_CLS_SIC_XREF_EFF_DT                             as                             WC_CLS_SIC_XREF_EFF_DT
		, WC_CLS_SIC_XREF_END_DT                             as                             WC_CLS_SIC_XREF_END_DT
		, WC_CLS_SIC_XREF_ID                                 as                                 WC_CLS_SIC_XREF_ID
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_X   ), 
RENAME_S as ( SELECT 
		  SIC_TYP_NM                                         as                                         SIC_TYP_NM
		, SIC_TYP_CD                                         as                                       S_SIC_TYP_CD
		, SIC_TYP_VOID_IND                                   as                                   SIC_TYP_VOID_IND 
				FROM     LOGIC_S   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_X                              as ( SELECT * from    RENAME_X   ),
FILTER_S                              as ( SELECT * from    RENAME_S 
				WHERE SIC_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

X as ( SELECT * 
				FROM  FILTER_X
				INNER JOIN FILTER_S ON  FILTER_X.SIC_TYP_CD =  FILTER_S.S_SIC_TYP_CD  )
SELECT * 
from X