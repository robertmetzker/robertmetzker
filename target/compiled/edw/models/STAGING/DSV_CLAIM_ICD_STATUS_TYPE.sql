

---- SRC LAYER ----
WITH
SRC_ICD as ( SELECT *     from     STAGING.DST_CLAIM_ICD_STATUS_TYPE ),
//SRC_ICD as ( SELECT *     from     DST_CLAIM_ICD_STATUS_TYPE) ,

---- LOGIC LAYER ----


LOGIC_ICD as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD 
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND 
		, ICD_STS_TYP_NM                                     as                                     ICD_STS_TYP_NM 
		from SRC_ICD
            )

---- RENAME LAYER ----
,

RENAME_ICD as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, ICD_STS_TYP_CD                                     as                               ICD_STATUS_TYPE_CODE
		, CLM_ICD_STS_PRI_IND                                as                              CLAIM_ICD_PRIMARY_IND
		, ICD_STS_TYP_NM                                     as                               ICD_STATUS_TYPE_NAME 
				FROM     LOGIC_ICD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ICD                            as ( SELECT * from    RENAME_ICD   ),

---- JOIN LAYER ----

 JOIN_ICD  as  ( SELECT * 
				FROM  FILTER_ICD )
 SELECT * FROM  JOIN_ICD