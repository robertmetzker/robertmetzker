

      create or replace  table DEV_EDW.STAGING.STG_PROVIDER_PROGRAM_FOCUS  as
      (---- SRC LAYER ----
WITH
SRC_OPPF as ( SELECT *     from     DEV_VIEWS.BWC_PEACH.TMPOPPF ),
SRC_OPPT as ( SELECT *     from     DEV_VIEWS.BWC_PEACH.TMPOPPT ),
SRC_PPFT as ( SELECT *     from     DEV_VIEWS.BWC_PEACH.TMPPPFT ),
//SRC_OPPF as ( SELECT *     from     TMPOPPF) ,
//SRC_OPPT as ( SELECT *     from     TMPOPPT) ,
//SRC_PPFT as ( SELECT *     from     TMPPPFT) ,

---- LOGIC LAYER ----

LOGIC_OPPF as ( SELECT 
		  upper( TRIM( PRVDR_BASE_NMBR ) )                   AS                                    PRVDR_BASE_NMBR 
		, LPAD(cast(PRVDR_SFX_NMBR as TEXT), 4, '0')         AS                                     PRVDR_SFX_NMBR 
		, upper( TRIM( PRVDR_PRGRM_CODE ) )                  AS                                   PRVDR_PRGRM_CODE 
		, PRGRM_PTP_PRD_NMBR                                 AS                                 PRGRM_PTP_PRD_NMBR 
		, upper( TRIM( PRVDR_PGM_FCS_CODE ) )                AS                                 PRVDR_PGM_FCS_CODE 
		, OPPF_CRT_DTTM                                      AS                                      OPPF_CRT_DTTM 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, upper( TRIM( CRT_USER_CODE ) )                     AS                                      CRT_USER_CODE 
		, upper( TRIM( CRT_PRGRM_NAME ) )                    AS                                     CRT_PRGRM_NAME 
		, upper( TRIM( DCTVT_USER_CODE ) )                   AS                                    DCTVT_USER_CODE 
		, upper( TRIM( DCTVT_PRGRM_NAME ) )                  AS                                   DCTVT_PRGRM_NAME 
		from SRC_OPPF
            ),
LOGIC_OPPT as ( SELECT 
		  upper( TRIM( PRVDR_PRGRM_CODE ) )                  AS                                   PRVDR_PRGRM_CODE 
		, upper( TRIM( PRVDR_PRGRM_NAME ) )                  AS                                   PRVDR_PRGRM_NAME 
		, upper( TRIM( PRVDR_PRGRM_DESCR ) )                 AS                                  PRVDR_PRGRM_DESCR 
		, CRT_DTTM                                           AS                                           CRT_DTTM 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, upper( TRIM( CRT_USER_CODE ) )                     AS                                      CRT_USER_CODE 
		, upper( TRIM( CRT_PRGRM_NAME ) )                    AS                                     CRT_PRGRM_NAME 
		, upper( TRIM( DCTVT_USER_CODE ) )                   AS                                    DCTVT_USER_CODE 
		, upper( TRIM( DCTVT_PRGRM_NAME ) )                  AS                                   DCTVT_PRGRM_NAME 
		from SRC_OPPT
            ),
LOGIC_PPFT as ( SELECT 
		  upper( TRIM( PRVDR_PGM_FCS_CODE ) )                AS                                 PRVDR_PGM_FCS_CODE 
		, upper( TRIM( PRVDR_PGM_FCS_NAME ) )                AS                                 PRVDR_PGM_FCS_NAME 
		, CRT_DTTM                                           AS                                           CRT_DTTM 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, upper( TRIM( CRT_USER_CODE ) )                     AS                                      CRT_USER_CODE 
		, upper( TRIM( CRT_PRGRM_NAME ) )                    AS                                     CRT_PRGRM_NAME 
		, upper( TRIM( DCTVT_USER_CODE ) )                   AS                                    DCTVT_USER_CODE 
		, upper( TRIM( DCTVT_PRGRM_NAME ) )                  AS                                   DCTVT_PRGRM_NAME 
		from SRC_PPFT
            )

---- RENAME LAYER ----
,

RENAME_OPPF as ( SELECT 
		  PRVDR_BASE_NMBR                                    as                                    PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                     PRVDR_SFX_NMBR
		, PRVDR_PRGRM_CODE                                   as                                   PRVDR_PRGRM_CODE
		, PRGRM_PTP_PRD_NMBR                                 as                       PROGRAM_PARTICIPATION_PERIOD
		, PRVDR_PGM_FCS_CODE                                 as                                 PRVDR_PGM_FCS_CODE
		, OPPF_CRT_DTTM                                      as                                           CRT_DTTM
		, EFCTV_DATE                                         as                                         EFCTV_DATE
		, ENDNG_DATE                                         as                                         ENDNG_DATE
		, DCTVT_DTTM                                         as                                         DCTVT_DTTM
		, CRT_USER_CODE                                      as                                      CRT_USER_CODE
		, CRT_PRGRM_NAME                                     as                                     CRT_PRGRM_NAME
		, DCTVT_USER_CODE                                    as                                    DCTVT_USER_CODE
		, DCTVT_PRGRM_NAME                                   as                                   DCTVT_PRGRM_NAME 
				FROM     LOGIC_OPPF   ), 
RENAME_OPPT as ( SELECT 
		  PRVDR_PRGRM_CODE                                   as                              PROVIDER_PROGRAM_CODE
		, PRVDR_PRGRM_NAME                                   as                              PROVIDER_PROGRAM_DESC
		, PRVDR_PRGRM_DESCR                                  as                                  PRVDR_PRGRM_DESCR
		, CRT_DTTM                                           as                                      OPPT_CRT_DTTM
		, EFCTV_DATE                                         as                                    OPPT_EFCTV_DATE
		, ENDNG_DATE                                         as                                    OPPT_ENDNG_DATE
		, DCTVT_DTTM                                         as                                    OPPT_DCTVT_DTTM
		, CRT_USER_CODE                                      as                                 OPPT_CRT_USER_CODE
		, CRT_PRGRM_NAME                                     as                                OPPT_CRT_PRGRM_NAME
		, DCTVT_USER_CODE                                    as                               OPPT_DCTVT_USER_CODE
		, DCTVT_PRGRM_NAME                                   as                              OPPT_DCTVT_PRGRM_NAME 
				FROM     LOGIC_OPPT   ), 
RENAME_PPFT as ( SELECT 
		  PRVDR_PGM_FCS_CODE                                 as                            PPFT_PRVDR_PGM_FCS_CODE
		, PRVDR_PGM_FCS_NAME                                 as                        PROVIDER_PROGRAM_FOCUS_TEXT
		, CRT_DTTM                                           as                                      PPFT_CRT_DTTM
		, EFCTV_DATE                                         as                                    PPFT_EFCTV_DATE
		, ENDNG_DATE                                         as                                    PPFT_ENDNG_DATE
		, DCTVT_DTTM                                         as                                    PPFT_DCTVT_DTTM
		, CRT_USER_CODE                                      as                                 PPFT_CRT_USER_CODE
		, CRT_PRGRM_NAME                                     as                                PPFT_CRT_PRGRM_NAME
		, DCTVT_USER_CODE                                    as                               PPFT_DCTVT_USER_CODE
		, DCTVT_PRGRM_NAME                                   as                              PPFT_DCTVT_PRGRM_NAME 
				FROM     LOGIC_PPFT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_OPPF                           as ( SELECT * from    RENAME_OPPF   ),
FILTER_OPPT                           as ( SELECT * from    RENAME_OPPT   ),
FILTER_PPFT                           as ( SELECT * from    RENAME_PPFT   ),

---- JOIN LAYER ----

OPPF as ( SELECT * 
				FROM  FILTER_OPPF
				LEFT JOIN FILTER_OPPT ON  FILTER_OPPF.PRVDR_PRGRM_CODE =  FILTER_OPPT.PROVIDER_PROGRAM_CODE 
								LEFT JOIN FILTER_PPFT ON  FILTER_OPPF.PRVDR_PGM_FCS_CODE =  FILTER_PPFT.PPFT_PRVDR_PGM_FCS_CODE  )
SELECT * 
from OPPF
      );
    