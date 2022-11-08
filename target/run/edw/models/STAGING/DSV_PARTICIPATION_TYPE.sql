
  create or replace  view DEV_EDW.STAGING.DSV_PARTICIPATION_TYPE  as (
    

---- SRC LAYER ----
WITH
SRC_PT as ( SELECT *     from     STAGING.DST_PARTICIPATION_TYPE ),
//SRC_PT as ( SELECT *     from     DST_PARTICIPATION_TYPE) ,

---- LOGIC LAYER ----


LOGIC_PT as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD 
		, PTCP_TYP_NM                                        as                                        PTCP_TYP_NM 
		, PTCP_PRI_IND                                       as                                       PTCP_PRI_IND            
		from SRC_PT
            )

---- RENAME LAYER ----
,

RENAME_PT as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, PTCP_TYP_CD                                        as                            PARTICIPATION_TYPE_CODE
		, PTCP_TYP_NM                                        as                            PARTICIPATION_TYPE_DESC
		, PTCP_PRI_IND				                         as                          PARTICIPATION_PRIMARY_IND 
				FROM     LOGIC_PT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PT                             as ( SELECT * from    RENAME_PT   ),

---- JOIN LAYER ----

 JOIN_PT  as  ( SELECT * 
				FROM  FILTER_PT )
 SELECT * 
 
 FROM  JOIN_PT
  );
