

      create or replace  table DEV_EDW.STAGING.STG_EDI_INFO  as
      (---- SRC LAYER ----
WITH
SRC_INFO as ( SELECT *     from     DEV_VIEWS.BASE.EDI_INFO ),
//SRC_INFO as ( SELECT *     from     EDI_INFO) ,

---- LOGIC LAYER ----

LOGIC_INFO as ( SELECT 
		  EDI_ID                                             AS                                             EDI_ID 
		, upper( TRIM( TYPE ) )                              AS                                               TYPE 
		, upper( TRIM( DESCRIPTION ) )                       AS                                        DESCRIPTION 
		, upper( TRIM( DIRECTION ) )                         AS                                          DIRECTION 
		, upper( TRIM( COMMAND ) )                           AS                                            COMMAND 
		, upper( TRIM( FILENAME ) )                          AS                                           FILENAME 
		, cast( EXEC_DATE as DATE )                          AS                                          EXEC_DATE 
		, RESPONSE                                           AS                                           RESPONSE 
		from SRC_INFO
            )

---- RENAME LAYER ----
,

RENAME_INFO as ( SELECT 
		  EDI_ID                                             as                                        EDI_INFO_ID
		, TYPE                                               as                                               TYPE
		, DESCRIPTION                                        as                                        DESCRIPTION
		, DIRECTION                                          as                                          DIRECTION
		, COMMAND                                            as                                            COMMAND
		, FILENAME                                           as                                           FILENAME
		, EXEC_DATE                                          as                                          EXEC_DATE
		, RESPONSE                                           as                                           RESPONSE 
				FROM     LOGIC_INFO   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_INFO                           as ( SELECT * from    RENAME_INFO   ),

---- JOIN LAYER ----

 JOIN_INFO  as  ( SELECT * 
				FROM  FILTER_INFO )
 SELECT * FROM  JOIN_INFO
      );
    