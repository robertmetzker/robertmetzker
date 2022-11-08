

      create or replace  table DEV_EDW.STAGING.STG_USERS  as
      (---- SRC LAYER ----
WITH
SRC_U as ( SELECT *     from     DEV_VIEWS.PCMP.USERS ),
SRC_UT as ( SELECT *     from     DEV_VIEWS.PCMP.USERS_TYPE ),
SRC_U2 as ( SELECT *     from     DEV_VIEWS.PCMP.USERS ),
SRC_U3 as ( SELECT *     from     DEV_VIEWS.PCMP.USERS ),
//SRC_U as ( SELECT *     from     PCMP.USERS) ,
//SRC_UT as ( SELECT *     from     PCMP.USERS_TYPE) ,
//SRC_U2 as ( SELECT *     from     PCMP.USERS) ,
//SRC_U3             as ( SELECT *     FROM     USERS) ,

---- LOGIC LAYER ----

LOGIC_U as ( SELECT 
		  USER_ID                                            AS                                            USER_ID 
		, upper( TRIM( USER_TYP_CD ) )                       AS                                        USER_TYP_CD 
		, upper( TRIM( USER_LGN_NM ) )                       AS                                        USER_LGN_NM 
		, upper( TRIM( USER_NM_FST ) )                       AS                                        USER_NM_FST 
		, upper( USER_NM_MID )                               AS                                        USER_NM_MID 
		, upper( TRIM( USER_NM_LST ) )                       AS                                        USER_NM_LST 
		, upper( TRIM( USER_DRV_UPCS_NM ) )                  AS                                   USER_DRV_UPCS_NM 
		, upper( TRIM( USER_TTL ) )                          AS                                           USER_TTL 
		, upper( TRIM( USER_PH_NO ) )                        AS                                         USER_PH_NO 
		, upper( TRIM( USER_PH_EXT_NO ) )                    AS                                     USER_PH_EXT_NO 
		, upper( TRIM( USER_EMAIL ) )                        AS                                         USER_EMAIL 
		, cast( USER_EFF_DT as DATE )                        AS                                        USER_EFF_DT 
		, cast( USER_END_DT as DATE )                        AS                                        USER_END_DT 
		, upper( USER_SUPR_IND )                             AS                                      USER_SUPR_IND 
		, USER_SUPR_ID                                       AS                                       USER_SUPR_ID 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( USER_VOID_IND )                             AS                                      USER_VOID_IND 
		from SRC_U
            ),
LOGIC_UT as ( SELECT 
		  upper( TRIM( USER_TYP_NM ) )                       AS                                        USER_TYP_NM 
		, upper( TRIM( USER_TYP_CD ) )                       AS                                        USER_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_UT
            ),
LOGIC_U2 as ( SELECT 
		  upper( TRIM( USER_LGN_NM ) )                       AS                                        USER_LGN_NM 
		, upper( TRIM( USER_NM_FST ) )                       AS                                        USER_NM_FST 
		, upper( USER_NM_MID )                               AS                                        USER_NM_MID 
		, upper( TRIM( USER_NM_LST ) )                       AS                                        USER_NM_LST 
		, upper( TRIM( USER_DRV_UPCS_NM ) )                  AS                                   USER_DRV_UPCS_NM 
		, USER_ID                                            AS                                            USER_ID
		, USER_SUPR_ID                                       as                                       USER_SUPR_ID 
		from SRC_U2
            ),

LOGIC_U3 as ( SELECT 
		  USER_ID                                            as                                            USER_ID 
		, upper( TRIM( USER_LGN_NM ) )                       as                                        USER_LGN_NM 
		, upper( TRIM( USER_NM_FST ) )                       as                                        USER_NM_FST 
		, upper( USER_NM_MID )                               as                                        USER_NM_MID 
		, upper( TRIM( USER_NM_LST ) )                       as                                        USER_NM_LST 
		, upper( TRIM( USER_DRV_UPCS_NM ) )                  as                                   USER_DRV_UPCS_NM 
		FROM SRC_U3 
		   ),

---- RENAME LAYER ----

RENAME_U as ( SELECT 
		  USER_ID                                            as                                            USER_ID
		, USER_TYP_CD                                        as                                        USER_TYP_CD
		, USER_LGN_NM                                        as                                        USER_LGN_NM
		, USER_NM_FST                                        as                                        USER_NM_FST
		, USER_NM_MID                                        as                                        USER_NM_MID
		, USER_NM_LST                                        as                                        USER_NM_LST
		, USER_DRV_UPCS_NM                                   as                                   USER_DRV_UPCS_NM
		, USER_TTL                                           as                                           USER_TTL
		, USER_PH_NO                                         as                                         USER_PH_NO
		, USER_PH_EXT_NO                                     as                                     USER_PH_EXT_NO
		, USER_EMAIL                                         as                                         USER_EMAIL
		, USER_EFF_DT                                        as                                        USER_EFF_DT
		, USER_END_DT                                        as                                        USER_END_DT
		, USER_SUPR_IND                                      as                                      USER_SUPR_IND
		, USER_SUPR_ID                                       as                                       USER_SUPR_ID
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, USER_VOID_IND                                      as                                      USER_VOID_IND 
				FROM     LOGIC_U   ), 
RENAME_UT as ( SELECT 
		  USER_TYP_NM                                        as                                        USER_TYP_NM
		, USER_TYP_CD                                        as                                     UT_USER_TYP_CD
		, VOID_IND                                           as                                        UT_VOID_IND 
				FROM     LOGIC_UT   ), 
RENAME_U2 as ( SELECT 
		  USER_LGN_NM                                        as                                  SUPERVISOR_LGN_NM
		, USER_NM_FST                                        as                                  SUPERVISOR_NM_FST
		, USER_NM_MID                                        as                                  SUPERVISOR_NM_MID
		, USER_NM_LST                                        as                                  SUPERVISOR_NM_LST
		, USER_DRV_UPCS_NM                                   as                             SUPERVISOR_DRV_UPCS_NM
		, USER_ID                                            as                                         U2_USER_ID
		, USER_SUPR_ID                                       as                                    U2_USER_SUPR_ID 
				FROM     LOGIC_U2   ),
RENAME_U3         as ( SELECT 
		  USER_ID                                            as                                         U3_USER_ID
		, USER_LGN_NM                                        as                                    DIRECTOR_LGN_NM
		, USER_NM_FST                                        as                                    DIRECTOR_NM_FST
		, USER_NM_MID                                        as                                    DIRECTOR_NM_MID
		, USER_NM_LST                                        as                                    DIRECTOR_NM_LST
		, USER_DRV_UPCS_NM                                   as                               DIRECTOR_DRV_UPCS_NM 
				FROM     LOGIC_U3   ),

---- FILTER LAYER (uses aliases) ----

FILTER_U                              as ( SELECT * from    RENAME_U   ),
FILTER_U2                             as ( SELECT * from    RENAME_U2   ),
FILTER_UT                             as ( SELECT * from    RENAME_UT 
				                        WHERE UT_VOID_IND = 'N'  ),
FILTER_U3                             as ( SELECT * FROM    RENAME_U3   ),										

---- JOIN LAYER ----

U as ( SELECT * 
				FROM  FILTER_U
				        LEFT JOIN FILTER_U2 ON  FILTER_U.USER_SUPR_ID =  FILTER_U2.U2_USER_ID 
						LEFT JOIN FILTER_UT ON  FILTER_U.USER_TYP_CD =  FILTER_UT.UT_USER_TYP_CD  
						LEFT JOIN FILTER_U3 ON  FILTER_U2.U2_USER_SUPR_ID =  FILTER_U3.U3_USER_ID  )
SELECT * 
from U
      );
    