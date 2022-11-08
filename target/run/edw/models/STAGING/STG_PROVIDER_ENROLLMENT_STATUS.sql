

      create or replace  table DEV_EDW.STAGING.STG_PROVIDER_ENROLLMENT_STATUS  as
      (---- SRC LAYER ----
WITH
SRC_TMPESTS as ( SELECT *     from     DEV_VIEWS.DBMOBP00.TMPESTS ),
SRC_TMPESTT as ( SELECT *     from     DEV_VIEWS.BWC_PEACH.TMPESTT ),
SRC_TMPPSRT as ( SELECT *     from     DEV_VIEWS.DBMOBP00.TMPPSRT ),
//SRC_TMPESTS as ( SELECT *     from     TMPESTS) ,
//SRC_TMPESTT as ( SELECT *     from     TMPESTT) ,
//SRC_TMPPSRT as ( SELECT *     from     TMPPSRT) ,

---- LOGIC LAYER ----

LOGIC_TMPESTS as ( SELECT 
		  cast( PRVDR_BASE_NMBR as TEXT )                    AS                                    PRVDR_BASE_NMBR 
		, LPAD(cast(PRVDR_SFX_NMBR as TEXT), 4, '0')         AS                                     PRVDR_SFX_NMBR 
		, upper( TRIM( ENRL_STS_TYPE_CODE ) )                AS                                 ENRL_STS_TYPE_CODE 
		, ESTS_CRT_DTTM                                      AS                                      ESTS_CRT_DTTM 
		, upper( TRIM( PRVDR_STS_RSN_CODE ) )                AS                                 PRVDR_STS_RSN_CODE 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		, upper( TRIM( CRT_USER_CODE ) )                     AS                                      CRT_USER_CODE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, upper( TRIM( DCTVT_USER_CODE ) )                   AS                                    DCTVT_USER_CODE 
		, upper( TRIM( UPDT_PRGRM_NAME ) )                   AS                                    UPDT_PRGRM_NAME 
		from SRC_TMPESTS
            ),
LOGIC_TMPESTT as ( SELECT 
		  upper( TRIM( ENRL_STS_NAME ) )                     AS                                      ENRL_STS_NAME 
		, cast( CRT_DTTM  as TIMESTAMP)                      AS                                           CRT_DTTM 
		, cast( EFCTV_DATE as DATE )                         AS                                         EFCTV_DATE 
		, cast( ENDNG_DATE as DATE )                         AS                                         ENDNG_DATE 
		, upper( TRIM( CRT_USER_CODE ) )                     AS                                      CRT_USER_CODE 
		, cast(DCTVT_DTTM  as TIMESTAMP)                     AS                                         DCTVT_DTTM 
		, upper( TRIM( DCTVT_USER_CODE ) )                   AS                                    DCTVT_USER_CODE 
		, upper( TRIM( UPDT_PRGRM_NAME ) )                   AS                                    UPDT_PRGRM_NAME 
		, upper( TRIM( ENRL_STS_TYPE_CODE ) )                AS                                 ENRL_STS_TYPE_CODE 
		from SRC_TMPESTT
            ),
LOGIC_TMPPSRT as ( SELECT 
		  TRIM( STS_RSN_TYPE_NAME )                          AS                                  STS_RSN_TYPE_NAME 
		, CRT_DTTM                                           AS                                           CRT_DTTM 
		, cast( EFCTV_DATE as DATE )                         AS                                         EFCTV_DATE 
		, cast( ENDNG_DATE as DATE )                         AS                                         ENDNG_DATE 
		, upper( TRIM( CRT_USER_CODE ) )                     AS                                      CRT_USER_CODE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, upper( TRIM( DCTVT_USER_CODE ) )                   AS                                    DCTVT_USER_CODE 
		, upper( TRIM( UPDT_PRGRM_NAME ) )                   AS                                    UPDT_PRGRM_NAME 
		, upper( TRIM( PRVDR_STS_RSN_CODE ) )                AS                                 PRVDR_STS_RSN_CODE 
		from SRC_TMPPSRT
            )

---- RENAME LAYER ----
,

RENAME_TMPESTS as ( SELECT 
		  PRVDR_BASE_NMBR                                    as                                    PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                     PRVDR_SFX_NMBR
		, ENRL_STS_TYPE_CODE                                 as                                 ENRL_STS_TYPE_CODE
		, ESTS_CRT_DTTM                                      as                                      ESTS_CRT_DTTM
		, PRVDR_STS_RSN_CODE                                 as                                 PRVDR_STS_RSN_CODE
		, EFCTV_DATE                                         as                                         EFCTV_DATE
		, ENDNG_DATE                                         as                                         ENDNG_DATE
		, CRT_USER_CODE                                      as                                      CRT_USER_CODE
		, DCTVT_DTTM                                         as                                         DCTVT_DTTM
		, DCTVT_USER_CODE                                    as                                    DCTVT_USER_CODE
		, UPDT_PRGRM_NAME                                    as                                    UPDT_PRGRM_NAME 
				FROM     LOGIC_TMPESTS   ), 
RENAME_TMPESTT as ( SELECT 
		  ENRL_STS_NAME                                      as                                      ENRL_STS_NAME
		, CRT_DTTM                                           as                                   TMPESTT_CRT_DTTM
		, EFCTV_DATE                                         as                                 TMPESTT_EFCTV_DATE
		, ENDNG_DATE                                         as                                 TMPESTT_ENDNG_DATE
		, CRT_USER_CODE                                      as                              TMPESTT_CRT_USER_CODE
		, DCTVT_DTTM                                         as                                 TMPESTT_DCTVT_DTTM
		, DCTVT_USER_CODE                                    as                            TMPESTT_DCTVT_USER_CODE
		, UPDT_PRGRM_NAME                                    as                            TMPESTT_UPDT_PRGRM_NAME
		, ENRL_STS_TYPE_CODE                                 as                         TMPESTT_ENRL_STS_TYPE_CODE 
				FROM     LOGIC_TMPESTT   ), 
RENAME_TMPPSRT as ( SELECT 
		  STS_RSN_TYPE_NAME                                  as                                  STS_RSN_TYPE_NAME
		, CRT_DTTM                                           as                                   TMPPSRT_CRT_DTTM
		, EFCTV_DATE                                         as                                 TMPPSRT_EFCTV_DATE
		, ENDNG_DATE                                         as                                 TMPPSRT_ENDNG_DATE
		, CRT_USER_CODE                                      as                              TMPPSRT_CRT_USER_CODE
		, DCTVT_DTTM                                         as                                 TMPPSRT_DCTVT_DTTM
		, DCTVT_USER_CODE                                    as                            TMPPSRT_DCTVT_USER_CODE
		, UPDT_PRGRM_NAME                                    as                            TMPPSRT_UPDT_PRGRM_NAME
		, PRVDR_STS_RSN_CODE                                 as                         TMPPSRT_PRVDR_STS_RSN_CODE 
				FROM     LOGIC_TMPPSRT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_TMPESTS                        as ( SELECT * from    RENAME_TMPESTS   ),
FILTER_TMPESTT                        as ( SELECT * from    RENAME_TMPESTT   ),
FILTER_TMPPSRT                        as ( SELECT * from    RENAME_TMPPSRT   ),

---- JOIN LAYER ----

TMPESTS as ( SELECT * 
				FROM  FILTER_TMPESTS
				INNER JOIN FILTER_TMPESTT ON  FILTER_TMPESTS.ENRL_STS_TYPE_CODE =  FILTER_TMPESTT.TMPESTT_ENRL_STS_TYPE_CODE 
						LEFT JOIN FILTER_TMPPSRT ON  FILTER_TMPESTS.PRVDR_STS_RSN_CODE =  FILTER_TMPPSRT.TMPPSRT_PRVDR_STS_RSN_CODE  )
SELECT * 
from TMPESTS
      );
    