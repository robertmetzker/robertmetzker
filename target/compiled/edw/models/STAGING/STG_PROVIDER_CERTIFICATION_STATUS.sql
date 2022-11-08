---- SRC LAYER ----
WITH
SRC_TMPCSTS as ( SELECT *     from     DEV_VIEWS.DBMOBP00.TMPCSTS ),
SRC_TMPCSTT as ( SELECT *     from     DEV_VIEWS.BWC_PEACH.TMPCSTT ),
SRC_TMPPSRT as ( SELECT *     from     DEV_VIEWS.DBMOBP00.TMPPSRT ),
//SRC_TMPCSTS as ( SELECT *     from     TMPCSTS) ,
//SRC_TMPCSTT as ( SELECT *     from     TMPCSTT) ,
//SRC_TMPPSRT as ( SELECT *     from     TMPPSRT) ,

---- LOGIC LAYER ----

LOGIC_TMPCSTS as ( SELECT 
		  cast( PRVDR_BASE_NMBR as TEXT )                    as                                    PRVDR_BASE_NMBR 
		, cast(lpad(PRVDR_SFX_NMBR, 4,'0') as TEXT )         as                                     PRVDR_SFX_NMBR 
		, upper( TRIM( CRTF_STS_TYPE_CODE ) )                as                                 CRTF_STS_TYPE_CODE 
		, CSTS_CRT_DTTM                                      as                                      CSTS_CRT_DTTM 
		, upper( TRIM( PRVDR_STS_RSN_CODE ) )                as                                 PRVDR_STS_RSN_CODE 
		, EFCTV_DATE                                         as                                         EFCTV_DATE 
		, ENDNG_DATE                                         as                                         ENDNG_DATE 
		, upper( TRIM( CRT_USER_CODE ) )                     as                                      CRT_USER_CODE 
		, DCTVT_DTTM                                         as                                         DCTVT_DTTM 
		, upper( TRIM( DCTVT_USER_CODE ) )                   as                                    DCTVT_USER_CODE 
		, upper( TRIM( UPDT_PRGRM_NAME ) )                   as                                    UPDT_PRGRM_NAME 
		from SRC_TMPCSTS
            ),
LOGIC_TMPCSTT as ( SELECT 
		  upper( TRIM( CRTF_STS_TYPE_NAME ) )                as                                 CRTF_STS_TYPE_NAME 
		, cast( CRT_DTTM as TIMESTAMP )                      as                                           CRT_DTTM 
		, cast( EFCTV_DATE as DATE )                         as                                      EFCTV_DATE 
		, cast( ENDNG_DATE as DATE )                         as                                         ENDNG_DATE 
		, upper( TRIM( CRT_USER_CODE ) )                     as                                      CRT_USER_CODE 
		, cast( DCTVT_DTTM as TIMESTAMP )                    as                                         DCTVT_DTTM 
		, upper( TRIM( DCTVT_USER_CODE ) )                   as                                    DCTVT_USER_CODE 
		, upper( TRIM( UPDT_PRGRM_NAME ) )                   as                                    UPDT_PRGRM_NAME 
		, upper( TRIM( CRTF_STS_TYPE_CODE ) )                as                                 CRTF_STS_TYPE_CODE 
		from SRC_TMPCSTT
            ),
LOGIC_TMPPSRT as ( SELECT 
		  TRIM( STS_RSN_TYPE_NAME )                          as                                  STS_RSN_TYPE_NAME 
		, CRT_DTTM                                           as                                           CRT_DTTM 
		, cast( EFCTV_DATE as DATE )                         as                                         EFCTV_DATE 
		, cast( ENDNG_DATE as DATE )                         as                                         ENDNG_DATE 
		, upper( TRIM( CRT_USER_CODE ) )                     as                                      CRT_USER_CODE 
		, DCTVT_DTTM                                         as                                         DCTVT_DTTM 
		, upper( TRIM( DCTVT_USER_CODE ) )                   as                                    DCTVT_USER_CODE 
		, upper( TRIM( UPDT_PRGRM_NAME ) )                   as                                    UPDT_PRGRM_NAME 
		, upper( TRIM( PRVDR_STS_RSN_CODE ) )                as                                 PRVDR_STS_RSN_CODE 
		from SRC_TMPPSRT
            )

---- RENAME LAYER ----
,

RENAME_TMPCSTS as ( SELECT 
		  PRVDR_BASE_NMBR                                    as                                    PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                     PRVDR_SFX_NMBR
		, CRTF_STS_TYPE_CODE                                 as                                 CRTF_STS_TYPE_CODE
		, CSTS_CRT_DTTM                                      as                                      CSTS_CRT_DTTM
		, PRVDR_STS_RSN_CODE                                 as                                 PRVDR_STS_RSN_CODE
		, EFCTV_DATE                                         as                                         EFCTV_DATE
		, ENDNG_DATE                                         as                                         ENDNG_DATE
		, CRT_USER_CODE                                      as                                      CRT_USER_CODE
		, DCTVT_DTTM                                         as                                         DCTVT_DTTM
		, DCTVT_USER_CODE                                    as                                    DCTVT_USER_CODE
		, UPDT_PRGRM_NAME                                    as                                    UPDT_PRGRM_NAME 
				FROM     LOGIC_TMPCSTS   ), 
RENAME_TMPCSTT as ( SELECT 
		  CRTF_STS_TYPE_NAME                                 as                                 CRTF_STS_TYPE_NAME
		, CRT_DTTM                                           as                                   TMPCSTT_CRT_DTTM
		, EFCTV_DATE                                         as                                 TMPCSTT_EFCTV_DATE
		, ENDNG_DATE                                         as                                 TMPCSTT_ENDNG_DATE
		, CRT_USER_CODE                                      as                              TMPCSTT_CRT_USER_CODE
		, DCTVT_DTTM                                         as                                 TMPCSTT_DCTVT_DTTM
		, DCTVT_USER_CODE                                    as                            TMPCSTT_DCTVT_USER_CODE
		, UPDT_PRGRM_NAME                                    as                            TMPCSTT_UPDT_PRGRM_NAME
		, CRTF_STS_TYPE_CODE                                 as                         TMPCSTT_CRTF_STS_TYPE_CODE 
				FROM     LOGIC_TMPCSTT   ), 
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
FILTER_TMPCSTS                        as ( SELECT * from    RENAME_TMPCSTS   ),
FILTER_TMPCSTT                        as ( SELECT * from    RENAME_TMPCSTT   ),
FILTER_TMPPSRT                        as ( SELECT * from    RENAME_TMPPSRT   ),

---- JOIN LAYER ----

TMPCSTS as ( SELECT * 
				FROM  FILTER_TMPCSTS
				INNER JOIN FILTER_TMPCSTT ON  FILTER_TMPCSTS.CRTF_STS_TYPE_CODE =  FILTER_TMPCSTT.TMPCSTT_CRTF_STS_TYPE_CODE 
								LEFT JOIN FILTER_TMPPSRT ON  FILTER_TMPCSTS.PRVDR_STS_RSN_CODE =  FILTER_TMPPSRT.TMPPSRT_PRVDR_STS_RSN_CODE  )
SELECT * 
from TMPCSTS