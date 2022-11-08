---- SRC LAYER ----
WITH
SRC_A as ( SELECT *     from     STAGING.STG_BENEFIT_TYPE ),
//SRC_A as ( SELECT *     from     STG_BENEFIT_TYPE) ,

---- LOGIC LAYER ----

LOGIC_A as ( SELECT 
		  TRIM( BNFT_TYP_CD )                                as                                        BNFT_TYP_CD 
		, TRIM( BNFT_TYP_NM )                                as                                        BNFT_TYP_NM 
		, TRIM( BNFT_CTG_TYP_NM )                            as                                    BNFT_CTG_TYP_NM 
		, TRIM( BNFT_RPT_TYP_NM )                            as                                    BNFT_RPT_TYP_NM 
		, TRIM( JUR_TYP_CD )                                 as                                         JUR_TYP_CD 
		, TRIM( JUR_TYP_NM )                                 as                                         JUR_TYP_NM 
		from SRC_A
            )

---- RENAME LAYER ----
,

RENAME_A as ( SELECT 
		  BNFT_TYP_CD                                        as                                        BNFT_TYP_CD
		, BNFT_TYP_NM                                        as                                        BNFT_TYP_NM
		, BNFT_CTG_TYP_NM                                    as                                    BNFT_CTG_TYP_NM
		, BNFT_RPT_TYP_NM                                    as                                    BNFT_RPT_TYP_NM
		, JUR_TYP_CD                                         as                                         JUR_TYP_CD
		, JUR_TYP_NM                                         as                                         JUR_TYP_NM 
				FROM     LOGIC_A   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_A                              as ( SELECT * from    RENAME_A   ),

---- JOIN LAYER ----

 JOIN_A  as  ( SELECT * 
				FROM  FILTER_A )
 SELECT 
 DISTINCT 
  md5(cast(
    
    coalesce(cast(BNFT_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(JUR_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(BNFT_RPT_TYP_NM as 
    varchar
), '')

 as 
    varchar
)) As UNIQUE_ID_KEY
, BNFT_TYP_CD
, BNFT_TYP_NM
, BNFT_CTG_TYP_NM
, BNFT_RPT_TYP_NM
, JUR_TYP_CD
, JUR_TYP_NM
  FROM  JOIN_A