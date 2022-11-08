

---- SRC LAYER ----
WITH
SRC_EOB as ( SELECT *     from     STAGING.DST_EOB ),
//SRC_EOB as ( SELECT *     from     DST_EOB) ,

---- LOGIC LAYER ----

LOGIC_EOB as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, CODE                                               AS                                               CODE 
		, SHORT_DESCRIPTION                                  AS                                  SHORT_DESCRIPTION 
		, LONG_DESCRIPTION                                   AS                                   LONG_DESCRIPTION 
		, CATEGORY                                           AS                                           CATEGORY 
		, ECAT_REF_DSC                                       AS                                       ECAT_REF_DSC 
		, EOB_TYPE                                           AS                                           EOB_TYPE 
		, ETYP_REF_DSC                                       AS                                       ETYP_REF_DSC 
		, APPLIED_BY                                         AS                                         APPLIED_BY 
		, REF_DSC                                            AS                                            REF_DSC 
		, EFFECTIVE_DATE                                     AS                                     EFFECTIVE_DATE 
		, EXPIRATION_DATE                                    AS                                    EXPIRATION_DATE 
		from SRC_EOB
            )

---- RENAME LAYER ----
,

RENAME_EOB as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CODE                                               as                                           EOB_CODE
		, SHORT_DESCRIPTION                                  as                              EOB_SHORT_DESCRIPTION
		, LONG_DESCRIPTION                                   as                               EOB_LONG_DESCRIPTION
		, CATEGORY                                           as                                  EOB_CATEGORY_CODE
		, ECAT_REF_DSC                                       as                           EOB_CATEGORY_DESCRIPTION
		, EOB_TYPE                                           as                                      EOB_TYPE_CODE
		, ETYP_REF_DSC                                       as                               EOB_TYPE_DESCRIPTION
		, APPLIED_BY                                         as                                         APPLIED_BY
		, REF_DSC                                            as                                       APPLIED_DESC
		, EFFECTIVE_DATE                                     as                                 EOB_EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                                       EOB_END_DATE 
				FROM     LOGIC_EOB   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_EOB                            as ( SELECT * from    RENAME_EOB   ),

---- JOIN LAYER ----

 JOIN_EOB  as  ( SELECT * 
				FROM  FILTER_EOB )
 SELECT * FROM  JOIN_EOB