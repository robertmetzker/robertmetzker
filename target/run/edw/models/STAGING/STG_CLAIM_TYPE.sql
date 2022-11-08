

      create or replace  table DEV_EDW.STAGING.STG_CLAIM_TYPE  as
      (---- SRC LAYER ----
WITH
SRC_CT as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_TYPE ),
//SRC_CT as ( SELECT *     from     CLAIM_TYPE) ,

---- LOGIC LAYER ----


LOGIC_CT as ( SELECT 
		  upper( TRIM( CLM_TYP_CD ) )                        as                                         CLM_TYP_CD 
		, upper( TRIM( CLM_TYP_NM ) )                        as                                         CLM_TYP_NM 
		from SRC_CT
            )

---- RENAME LAYER ----
,

RENAME_CT as ( SELECT 
		  CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM 
				FROM     LOGIC_CT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CT                             as ( SELECT * from    RENAME_CT   ),

---- JOIN LAYER ----

 JOIN_CT  as  ( SELECT * 
				FROM  FILTER_CT )
 SELECT * FROM  JOIN_CT
      );
    