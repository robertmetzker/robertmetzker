---- SRC LAYER ----
WITH
SRC_CAWWSD as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_AVERAGE_WAGE_WG_SRC_DTL ),
//SRC_CAWWSD as ( SELECT *     from     CLAIM_AVERAGE_WAGE_WG_SRC_DTL) ,

---- LOGIC LAYER ----

LOGIC_CAWWSD as ( SELECT 
		  CAWWSD_ID                                          AS                                          CAWWSD_ID 
		, CLM_AVG_WG_ID                                      AS                                      CLM_AVG_WG_ID 
		, CLM_WG_SRC_DTL_ID                                  AS                                  CLM_WG_SRC_DTL_ID 
		, upper( CAWWSD_INCL_IND )                           AS                                    CAWWSD_INCL_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CAWWSD
            )

---- RENAME LAYER ----
,

RENAME_CAWWSD as ( SELECT 
		  CAWWSD_ID                                          as                                          CAWWSD_ID
		, CLM_AVG_WG_ID                                      as                                      CLM_AVG_WG_ID
		, CLM_WG_SRC_DTL_ID                                  as                                  CLM_WG_SRC_DTL_ID
		, CAWWSD_INCL_IND                                    as                                    CAWWSD_INCL_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CAWWSD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CAWWSD                         as ( SELECT * from    RENAME_CAWWSD   ),

---- JOIN LAYER ----

 JOIN_CAWWSD  as  ( SELECT * 
				FROM  FILTER_CAWWSD )
 SELECT * FROM  JOIN_CAWWSD