

      create or replace  table DEV_EDW.STAGING.STG_PARTICIPATION_TYPE  as
      (---- SRC LAYER ----
WITH
SRC_PT as ( SELECT *     from     DEV_VIEWS.PCMP.PARTICIPATION_TYPE ),
//SRC_PT as ( SELECT *     from     PARTICIPATION_TYPE) ,

---- LOGIC LAYER ----


LOGIC_PT as ( SELECT 
		  upper( TRIM( PTCP_TYP_CD ) )                       as                                        PTCP_TYP_CD 
		, upper( TRIM( PTCP_TYP_NM ) )                       as                                        PTCP_TYP_NM 
		, upper( BLK_HDR_DSPLY_IND )                         as                                  BLK_HDR_DSPLY_IND 
		, upper( PTCP_TYP_VOID_IND )                         as                                  PTCP_TYP_VOID_IND 
		from SRC_PT
            )

---- RENAME LAYER ----
,

RENAME_PT as ( SELECT 
		  PTCP_TYP_CD                                        as                                        PTCP_TYP_CD
		, PTCP_TYP_NM                                        as                                        PTCP_TYP_NM
		, BLK_HDR_DSPLY_IND                                  as                                  BLK_HDR_DSPLY_IND
		, PTCP_TYP_VOID_IND                                  as                                  PTCP_TYP_VOID_IND 
				FROM     LOGIC_PT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PT                             as ( SELECT * from    RENAME_PT   ),

---- JOIN LAYER ----

 JOIN_PT  as  ( SELECT * 
				FROM  FILTER_PT )
 SELECT * FROM  JOIN_PT
      );
    