

      create or replace  table DEV_EDW.STAGING.DST_PARTICIPATION_TYPE  as
      (---- SRC LAYER ----
WITH
SRC_PT as ( SELECT *     from     STAGING.STG_PARTICIPATION_TYPE ),
//SRC_PT as ( SELECT *     from     STG_PARTICIPATION_TYPE) ,
--- Based on Requirement from DA, CTE has been hardcoded with 'Y', 'N' values for CROSS JOIN
SRC_PT_IND as (select 'Y' as PTCP_PRI_IND UNION SELECT 'N' as PTCP_PRI_IND ),


---- LOGIC LAYER ----


LOGIC_PT as ( SELECT 
           
		  TRIM( PTCP_TYP_CD )                                as                                        PTCP_TYP_CD 
		, TRIM( PTCP_TYP_NM )                                as                                        PTCP_TYP_NM 
	    ,  PTCP_TYP_VOID_IND                                 as                                  PTCP_TYP_VOID_IND         
		from SRC_PT
            ),


LOGIC_PT_IND as ( SELECT 
	      PTCP_PRI_IND                                 as                                             PTCP_PRI_IND         
		from SRC_PT_IND
            )   

---- RENAME LAYER ----
,

RENAME_PT as ( SELECT 
		  PTCP_TYP_CD                                        as                                        PTCP_TYP_CD
		, PTCP_TYP_NM                                        as                                        PTCP_TYP_NM
		, PTCP_TYP_VOID_IND                                  as                                  PTCP_TYP_VOID_IND 
				FROM     LOGIC_PT   ),

RENAME_PT_IND as ( SELECT 
		  PTCP_PRI_IND                                       as                                       PTCP_PRI_IND 
				FROM     LOGIC_PT_IND   )

---- FILTER LAYER (uses aliases) ---- 
,
FILTER_PT                             as ( SELECT * from    RENAME_PT  ),
FILTER_PT_IND                         as ( SELECT * from    RENAME_PT_IND ),

---- JOIN LAYER ----

 JOIN_PT  as  ( SELECT * 
				FROM  FILTER_PT  CROSS JOIN FILTER_PT_IND)
 SELECT 
       	 md5(cast(
    
    coalesce(cast(PTCP_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(PTCP_PRI_IND as 
    varchar
), '')

 as 
    varchar
))  as  UNIQUE_ID_KEY 
		, PTCP_TYP_CD
		, PTCP_TYP_NM
		, PTCP_PRI_IND 
  FROM  JOIN_PT
      );
    