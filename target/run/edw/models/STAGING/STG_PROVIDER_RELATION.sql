

      create or replace  table DEV_EDW.STAGING.STG_PROVIDER_RELATION  as
      (---- SRC LAYER ----
WITH
SRC_TMPRLTN as ( SELECT *     from     DEV_VIEWS.BWC_PEACH.TMPRLTN ),
//SRC_TMPRLTN as ( SELECT *     from     TMPRLTN) ,

---- LOGIC LAYER ----

LOGIC_TMPRLTN as ( SELECT 
		  cast( PR_PRVDR_BASE_NMBR as TEXT )                 AS                                 PR_PRVDR_BASE_NMBR 
		, LPAD(cast(PR_PRVDR_SFX_NMBR as TEXT), 4, '0')      AS                                  PR_PRVDR_SFX_NMBR  
		, cast( SC_PRVDR_BASE_NMBR as TEXT )                 AS                                 SC_PRVDR_BASE_NMBR
		, LPAD(cast(SC_PRVDR_SFX_NMBR as TEXT), 4, '0')      AS                                  SC_PRVDR_SFX_NMBR 
		, RLTN_CRT_DTTM                                      AS                                      RLTN_CRT_DTTM 
		, upper( CRT_USER_CODE )                             AS                                      CRT_USER_CODE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, upper( DCTVT_USER_CODE )                           AS                                    DCTVT_USER_CODE 
		, upper( UPDT_PRGRM_NAME )                           AS                                    UPDT_PRGRM_NAME 
		from SRC_TMPRLTN
            )

---- RENAME LAYER ----
,

RENAME_TMPRLTN as ( SELECT 
		  PR_PRVDR_BASE_NMBR                                 as                                 PR_PRVDR_BASE_NMBR
		, PR_PRVDR_SFX_NMBR                                  as                                  PR_PRVDR_SFX_NMBR
		, SC_PRVDR_BASE_NMBR                                 as                                 SC_PRVDR_BASE_NMBR
		, SC_PRVDR_SFX_NMBR                                  as                                  SC_PRVDR_SFX_NMBR
		, RLTN_CRT_DTTM                                      as                                      RLTN_CRT_DTTM
		, CRT_USER_CODE                                      as                                      CRT_USER_CODE
		, DCTVT_DTTM                                         as                                         DCTVT_DTTM
		, DCTVT_USER_CODE                                    as                                    DCTVT_USER_CODE
		, UPDT_PRGRM_NAME                                    as                                    UPDT_PRGRM_NAME 
				FROM     LOGIC_TMPRLTN   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_TMPRLTN                        as ( SELECT * from    RENAME_TMPRLTN   ),

---- JOIN LAYER ----

 JOIN_TMPRLTN  as  ( SELECT * 
				FROM  FILTER_TMPRLTN )
 SELECT * FROM  JOIN_TMPRLTN
      );
    