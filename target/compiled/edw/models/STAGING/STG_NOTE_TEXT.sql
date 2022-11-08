---- SRC LAYER ----
WITH
SRC_NT as ( SELECT *     from     DEV_VIEWS.PCMP.NOTE_TEXT ),
//SRC_NT as ( SELECT *     from     NOTE_TEXT) ,

---- LOGIC LAYER ----

LOGIC_NT as ( SELECT 
		  NOTE_TEXT_ID                                       AS                                       NOTE_TEXT_ID 
		, NOTE_ID                                            AS                                            NOTE_ID 
		, NOTE_TEXT_NO                                       AS                                       NOTE_TEXT_NO 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( NOTE_TEXT_VOID_IND )                        AS                                 NOTE_TEXT_VOID_IND 
		, upper( TRIM( NOTE_TEXT_TEXT ) )                    AS                                     NOTE_TEXT_TEXT 
		from SRC_NT
            )

---- RENAME LAYER ----
,

RENAME_NT as ( SELECT 
		  NOTE_TEXT_ID                                       as                                       NOTE_TEXT_ID
		, NOTE_ID                                            as                                            NOTE_ID
		, NOTE_TEXT_NO                                       as                                       NOTE_TEXT_NO
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, NOTE_TEXT_VOID_IND                                 as                                 NOTE_TEXT_VOID_IND
		, NOTE_TEXT_TEXT                                     as                                     NOTE_TEXT_TEXT 
				FROM     LOGIC_NT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_NT                             as ( SELECT * from    RENAME_NT 
				WHERE NOTE_TEXT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

 JOIN_NT  as  ( SELECT * 
				FROM  FILTER_NT )
 SELECT * FROM  JOIN_NT