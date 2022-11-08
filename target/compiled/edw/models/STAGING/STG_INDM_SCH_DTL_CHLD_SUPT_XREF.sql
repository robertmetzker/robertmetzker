---- SRC LAYER ----
WITH
SRC_ISDCS          as ( SELECT *     FROM     DEV_VIEWS.PCMP.INDM_SCH_DTL_CHLD_SUPT ),
//SRC_ISDCS          as ( SELECT *     FROM     INDM_SCH_DTL_CHLD_SUPT) ,

---- LOGIC LAYER ----


LOGIC_ISDCS as ( SELECT 
		  INDM_SCH_DTL_CHLD_SUPT_ID                          as                          INDM_SCH_DTL_CHLD_SUPT_ID 
		, INDM_SCH_DTL_AMT_ID                                as                                INDM_SCH_DTL_AMT_ID 
		, CUST_CHLD_SUPT_ID                                  as                                  CUST_CHLD_SUPT_ID 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		FROM SRC_ISDCS
            )

---- RENAME LAYER ----
,

RENAME_ISDCS      as ( SELECT 
		  INDM_SCH_DTL_CHLD_SUPT_ID                          as                          INDM_SCH_DTL_CHLD_SUPT_ID
		, INDM_SCH_DTL_AMT_ID                                as                                INDM_SCH_DTL_AMT_ID
		, CUST_CHLD_SUPT_ID                                  as                                  CUST_CHLD_SUPT_ID
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_ISDCS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ISDCS                          as ( SELECT * FROM    RENAME_ISDCS   ),

---- JOIN LAYER ----

 JOIN_ISDCS       as  ( SELECT * 
				FROM  FILTER_ISDCS )
 SELECT * FROM  JOIN_ISDCS