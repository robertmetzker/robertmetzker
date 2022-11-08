---- SRC LAYER ----
WITH
SRC_REF as ( SELECT *     from     STAGING.STG_CAM_REF ),
//SRC_REF as ( SELECT *     from     STG_CAM_REF) ,

---- LOGIC LAYER ----

LOGIC_REF as ( SELECT 
		  md5(cast(
    
    coalesce(cast(REF_IDN as 
    varchar
), '')

 as 
    varchar
))          AS                                      UNIQUE_ID_KEY
		, TRIM( REF_DGN )                                    AS                                            REF_DGN         
		, TRIM( REF_IDN )                                    AS                                            REF_IDN 
		, REF_EFF_DTE                                        AS                                        REF_EFF_DTE 
		, REF_EXP_DTE                                        AS                                        REF_EXP_DTE 
		, REF_DLM                                            AS                                            REF_DLM 
		, TRIM( REF_ULM )                                    AS                                            REF_ULM 
		, TRIM( REF_DSC )                                    AS                                            REF_DSC 
		, REF_ENT_DTE                                        AS                                        REF_ENT_DTE 
		, TRIM( REF_ENT_UID )                                AS                                        REF_ENT_UID 
		from SRC_REF
            )

---- RENAME LAYER ----
,

RENAME_REF as ( SELECT
          UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
        , REF_DGN                                            as                                            REF_DGN
		, REF_IDN                                            as                                            REF_IDN
		, REF_EFF_DTE                                        as                                        REF_EFF_DTE
		, REF_EXP_DTE                                        as                                        REF_EXP_DTE
		, REF_DLM                                            as                                            REF_DLM
		, REF_ULM                                            as                                            REF_ULM
		, REF_DSC                                            as                                            REF_DSC
		, REF_ENT_DTE                                        as                                        REF_ENT_DTE
		, REF_ENT_UID                                        as                                        REF_ENT_UID 
				FROM     LOGIC_REF   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_REF                            as ( SELECT * from    RENAME_REF 
				WHERE REF_DGN = 'POA'  ),

---- JOIN LAYER ----

 JOIN_REF  as  ( SELECT * 
				FROM  FILTER_REF )
 SELECT * FROM  JOIN_REF