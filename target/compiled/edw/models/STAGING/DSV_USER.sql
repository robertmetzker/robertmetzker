

---- SRC LAYER ----
WITH
SRC_U as ( SELECT *     from     STAGING.DST_USER ),
//SRC_U as ( SELECT *     from     DST_USER) ,

---- LOGIC LAYER ----

LOGIC_U as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, USER_LGN_NM                                        as                                        USER_LGN_NM 
		, USER_NM_LST                                        as                                        USER_NM_LST 
		, USER_NM_FST                                        as                                        USER_NM_FST
	    , USER_NM_MID                                        as                                        USER_NM_MID
		, USER_FULL_NAME                                     as                                     USER_FULL_NAME 
		, USER_TYP_NM                                        as                                        USER_TYP_NM 
		, USER_ROLE_TYPE                                     as                                     USER_ROLE_TYPE 
		, USER_EMAIL                                         as                                         USER_EMAIL 
		, USER_PH_NO                                         as                                         USER_PH_NO 
		, USER_PH_EXT_NO                                     as                                     USER_PH_EXT_NO 
		, SUPERVISOR_NM_LST                                  as                                  SUPERVISOR_NM_LST 
		, SUPERVISOR_NM_FST                                  as                                  SUPERVISOR_NM_FST 
		, SUPERVISOR_NM_MID                                  as                                  SUPERVISOR_NM_MID
		, SUPERVISOR_FULL_NAME                               as                               SUPERVISOR_FULL_NAME 
		, DIRECTOR_NM_LST                                    as                                    DIRECTOR_NM_LST 
		, DIRECTOR_NM_FST                                    as                                    DIRECTOR_NM_FST 
		, DIRECTOR_NM_MID                                    as                                    DIRECTOR_NM_MID 
		, DIRECTOR_FULL_NAME                                 as                                 DIRECTOR_FULL_NAME  
		from SRC_U
            )

---- RENAME LAYER ----
,

RENAME_U as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, USER_LGN_NM                                        as                                    USER_LOGIN_NAME
		, USER_NM_LST                                        as                                     USER_LAST_NAME
		, USER_NM_FST                                        as                                    USER_FIRST_NAME
		, USER_NM_MID                                        as                                   USER_MIDDLE_NAME
		, USER_FULL_NAME                                     as                                     USER_FULL_NAME
		, USER_TYP_NM                                        as                                          USER_TYPE
		, USER_ROLE_TYPE                                     as                                          USER_ROLE
		, USER_EMAIL                                         as                                         USER_EMAIL
		, USER_PH_NO                                         as                                  USER_PHONE_NUMBER
		, USER_PH_EXT_NO                                     as                        USER_PHONE_NUMBER_EXTENSION
		, SUPERVISOR_NM_LST                                  as                               SUPERVISOR_LAST_NAME
		, SUPERVISOR_NM_FST                                  as                              SUPERVISOR_FIRST_NAME
		, SUPERVISOR_NM_MID                                  as                             SUPERVISOR_MIDDLE_NAME
		, SUPERVISOR_FULL_NAME                               as                               SUPERVISOR_FULL_NAME 
		, DIRECTOR_NM_LST                                    as                                 DIRECTOR_LAST_NAME 
		, DIRECTOR_NM_FST                                    as                                DIRECTOR_FIRST_NAME
		, DIRECTOR_NM_MID                                    as                               DIRECTOR_MIDDLE_NAME 
		, DIRECTOR_FULL_NAME                                 as                                 DIRECTOR_FULL_NAME  
				FROM     LOGIC_U   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_U                              as ( SELECT * from    RENAME_U   ),

---- JOIN LAYER ----

 JOIN_U  as  ( SELECT * 
				FROM  FILTER_U )
 SELECT * FROM  JOIN_U