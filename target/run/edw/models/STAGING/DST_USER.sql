

      create or replace  table DEV_EDW.STAGING.DST_USER  as
      (---- SRC LAYER ----
WITH
SRC_U as ( SELECT *     from     STAGING.STG_USERS ),
//SRC_U as ( SELECT *     from     STG_USERS) ,

---- LOGIC LAYER ----

LOGIC_U as ( SELECT 
          TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		, TRIM( USER_NM_LST )                                as                                        USER_NM_LST 
		, TRIM( USER_NM_FST )                                as                                        USER_NM_FST 
		, TRIM( USER_NM_MID )                                as                                        USER_NM_MID 
		, USER_NM_LST||','||USER_NM_FST                      as                                        USER_FULL_NAME
		, TRIM( USER_TYP_NM )                                as                                        USER_TYP_NM 
		, TRIM( USER_EMAIL )                                 as                                        USER_EMAIL 
		, TRIM( USER_PH_NO )                                 as                                        USER_PH_NO 
		, TRIM( USER_PH_EXT_NO )                             as                                        USER_PH_EXT_NO 
		, TRIM( SUPERVISOR_NM_LST )                          as                                        SUPERVISOR_NM_LST 
		, TRIM( SUPERVISOR_NM_FST )                          as                                        SUPERVISOR_NM_FST 
		, TRIM( SUPERVISOR_NM_MID )                          as                                        SUPERVISOR_NM_MID 
		, SUPERVISOR_NM_LST||','||SUPERVISOR_NM_FST          as                                        SUPERVISOR_FULL_NAME
		, TRIM( DIRECTOR_NM_LST )                            as                                        DIRECTOR_NM_LST 
		, TRIM( DIRECTOR_NM_FST )                            as                                        DIRECTOR_NM_FST 
		, TRIM( DIRECTOR_NM_MID )                            as                                        DIRECTOR_NM_MID 
		, DIRECTOR_NM_LST||','||DIRECTOR_NM_FST              as                                        DIRECTOR_FULL_NAME 
		, TRIM( USER_VOID_IND )                              as                                        USER_VOID_IND 
		, USER_EFF_DT                                        as                                        USER_EFF_DT 
		, USER_END_DT                                        as                                        USER_END_DT 
		, AUDIT_USER_CREA_DTM                                as                                        AUDIT_USER_CREA_DTM 
		, USER_TYP_CD                                        as                                        USER_TYP_CD 
		, USER_DRV_UPCS_NM                                   as                                        USER_DRV_UPCS_NM 
		, AUDIT_USER_UPDT_DTM                                as                                        AUDIT_USER_UPDT_DTM 
		
		from SRC_U
            )

---- RENAME LAYER ----
,

RENAME_U as ( SELECT 
		  USER_LGN_NM                                        as                                        USER_LGN_NM
		, USER_NM_LST                                        as                                        USER_NM_LST
		, USER_NM_FST                                        as                                        USER_NM_FST
		, USER_NM_MID                                        as                                        USER_NM_MID
		, USER_FULL_NAME                                     as                                        USER_FULL_NAME
		, USER_TYP_NM                                        as                                        USER_TYP_NM
		, USER_EMAIL                                         as                                        USER_EMAIL
		, USER_PH_NO                                         as                                        USER_PH_NO
		, USER_PH_EXT_NO                                     as                                        USER_PH_EXT_NO
		, SUPERVISOR_NM_LST                                  as                                        SUPERVISOR_NM_LST
		, SUPERVISOR_NM_FST                                  as                                        SUPERVISOR_NM_FST
		, SUPERVISOR_NM_MID                                  as                                        SUPERVISOR_NM_MID
		, SUPERVISOR_FULL_NAME                               as                                        SUPERVISOR_FULL_NAME
		, DIRECTOR_NM_LST                                    as                                        DIRECTOR_NM_LST
		, DIRECTOR_NM_FST                                    as                                        DIRECTOR_NM_FST
		, DIRECTOR_NM_MID                                    as                                        DIRECTOR_NM_MID
		, DIRECTOR_FULL_NAME                                 as                                        DIRECTOR_FULL_NAME
		, USER_VOID_IND                                      as                                        USER_VOID_IND
		, USER_EFF_DT                                        as                                        USER_EFF_DT
		, USER_END_DT                                        as                                        USER_END_DT
		, AUDIT_USER_CREA_DTM                                as                                        AUDIT_USER_CREA_DTM 
		, USER_TYP_CD                                        as                                        USER_TYP_CD 
		, USER_DRV_UPCS_NM                                   as                                        USER_DRV_UPCS_NM 
		, AUDIT_USER_UPDT_DTM                                as                                        AUDIT_USER_UPDT_DTM 

			
				FROM     LOGIC_U   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_U as ( SELECT * from    RENAME_U 
                      QUALIFY(ROW_NUMBER()OVER (PARTITION BY USER_LGN_NM ORDER BY USER_VOID_IND,NVL(USER_EFF_DT, CURRENT_DATE) DESC,NVL(AUDIT_USER_UPDT_DTM,AUDIT_USER_CREA_DTM) DESC)) = 1  ),
											
---- JOIN LAYER ----

 JOIN_U  as  ( SELECT * 
				FROM  FILTER_U ),
                
 -- ETL Layer -----------
 
 ETL AS ( 
  SELECT 
md5(cast(
    
    coalesce(cast(USER_LGN_NM as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY
, USER_LGN_NM
, USER_NM_LST
, USER_NM_FST
, USER_NM_MID
, USER_FULL_NAME
, USER_TYP_NM

, case when USER_TYP_CD in ('INTRN', 'CONV') then 'BWC'
       when USER_TYP_CD = 'EXTRN' and upper(USER_NM_FST) like 'WEB-%' then substring(upper(USER_NM_FST), 5)
       when USER_TYP_CD = 'EXTRN' and upper(USER_NM_FST) is null then substring(upper(trim(USER_DRV_UPCS_NM)), 13)
       else upper(USER_NM_FST) end as USER_ROLE_TYPE
, USER_EMAIL
, USER_PH_NO
, USER_PH_EXT_NO
, SUPERVISOR_NM_LST
, SUPERVISOR_NM_FST
, SUPERVISOR_NM_MID
, SUPERVISOR_FULL_NAME
, DIRECTOR_NM_LST
, DIRECTOR_NM_FST
, DIRECTOR_NM_MID
, DIRECTOR_FULL_NAME
, USER_VOID_IND
, USER_EFF_DT
, USER_END_DT
, AUDIT_USER_CREA_DTM
 
 from JOIN_U
      QUALIFY (ROW_NUMBER()OVER (PARTITION BY USER_LGN_NM ORDER BY USER_EFF_DT DESC,AUDIT_USER_CREA_DTM DESC) = 1)
 )
 SELECT * FROM  ETL
      );
    