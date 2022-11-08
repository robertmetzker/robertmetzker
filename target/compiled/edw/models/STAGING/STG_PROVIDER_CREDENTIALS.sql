---- SRC LAYER ----
WITH
SRC_PCRD as ( SELECT *     from     DEV_VIEWS.DBMOBP00.TMPPCRD ),
SRC_CRDD as ( SELECT *     from     DEV_VIEWS.DBMOBP00.TMPCRDD ),
SRC_CRDC as ( SELECT *     from     DEV_VIEWS.BWC_PEACH.TMPCRDC ),
SRC_CRDT as ( SELECT *     from     DEV_VIEWS.BWC_PEACH.TMPCRDT ),
SRC_MSPT as ( SELECT *     from     DEV_VIEWS.BWC_PEACH.TMPMSPT ),
SRC_MSPL as ( SELECT *     from     DEV_VIEWS.BWC_PEACH.TMPMSPL ),
SRC_CRTT as ( SELECT *     from     DEV_VIEWS.BWC_PEACH.TMPCRTT ),
//SRC_PCRD as ( SELECT *     from     TMPPCRD) ,
//SRC_CRDD as ( SELECT *     from     TMPCRDD) ,
//SRC_CRDC as ( SELECT *     from     TMPCRDC) ,
//SRC_CRDT as ( SELECT *     from     TMPCRDT) ,
//SRC_MSPT as ( SELECT *     from     TMPMSPT) ,
//SRC_MSPL as ( SELECT *     from     TMPMSPL) ,
//SRC_CRTT as ( SELECT *     from     TMPCRTT) ,

---- LOGIC LAYER ----

LOGIC_PCRD as ( SELECT 
		  cast( PRVDR_BASE_NMBR as TEXT )                    AS                                    PRVDR_BASE_NMBR 
		, LPAD(cast(PRVDR_SFX_NMBR as TEXT), 4, '0')         AS                                     PRVDR_SFX_NMBR 
		, PCRD_CRT_DTTM                                      AS                                      PCRD_CRT_DTTM 
		, upper( TRIM( CRT_USER_CODE ) )                     AS                                      CRT_USER_CODE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, upper( TRIM( DCTVT_USER_CODE ) )                   AS                                    DCTVT_USER_CODE 
		, CRDN_ID                                            AS                                            CRDN_ID 
		from SRC_PCRD
            ),
LOGIC_CRDD as ( SELECT 
		  upper( TRIM( CRDN_CTGRY_CODE ) )                   AS                                    CRDN_CTGRY_CODE 
		, upper( TRIM( CRDN_TYPE_CODE ) )                    AS                                     CRDN_TYPE_CODE 
		, upper( TRIM( CRDN_ID_NMBR ) )                      AS                                       CRDN_ID_NMBR 
		, upper( TRIM( CRDN_CNFRM_IND ) )                    AS                                     CRDN_CNFRM_IND 
		, NULLIF(upper( TRIM( STATE_CODE ) ) , '')           AS                                         STATE_CODE 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, EXPRT_DATE                                         AS                                         EXPRT_DATE 
		, upper( TRIM( MDCL_SPC_TYPE_CODE ) )                AS                                 MDCL_SPC_TYPE_CODE 
		, upper( TRIM( MDCL_SPC_LVL_CODE ) )                 AS                                  MDCL_SPC_LVL_CODE 
		, TRMNT_DATE                                         AS                                         TRMNT_DATE 
		, upper( TRIM( CRDN_TRM_TYPE_CODE ) )                AS                                 CRDN_TRM_TYPE_CODE 
		, CRDD_CRT_DTTM                                      AS                                      CRDD_CRT_DTTM 
		, CRDN_ID                                            AS                                            CRDN_ID 
		, upper( TRIM( CRT_USER_CODE ) )                     AS                                      CRT_USER_CODE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, upper( TRIM( DCTVT_USER_CODE ) )                   AS                                    DCTVT_USER_CODE 
		, upper( TRIM( DCTVT_MTHD_CODE ) )                   AS                                    DCTVT_MTHD_CODE 
		from SRC_CRDD
            ),
LOGIC_CRDC as ( SELECT 
		  upper( TRIM( CRDN_CTGRY_NAME ) )                   AS                                    CRDN_CTGRY_NAME 
		, upper( TRIM( CRDN_CTGRY_CODE ) )                   AS                                    CRDN_CTGRY_CODE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		from SRC_CRDC
            ),
LOGIC_CRDT as ( SELECT 
		  upper( TRIM( CRDN_TYPE_NAME ) )                    AS                                     CRDN_TYPE_NAME 
		, upper( TRIM( CRDN_TYPE_CODE ) )                    AS                                     CRDN_TYPE_CODE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		from SRC_CRDT
            ),
LOGIC_MSPT as ( SELECT 
		  upper( TRIM( MDCL_SPC_TYPE_NAME ) )                AS                                 MDCL_SPC_TYPE_NAME 
		, upper( TRIM( MDCL_SPC_TYPE_CODE ) )                AS                                 MDCL_SPC_TYPE_CODE 
		, CRT_DTTM                                           AS                                           CRT_DTTM 
		, upper( TRIM( CRT_USER_CODE ) )                     AS                                      CRT_USER_CODE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, upper( TRIM( DCTVT_USER_CODE ) )                   AS                                    DCTVT_USER_CODE 
		from SRC_MSPT
            ),
LOGIC_MSPL as ( SELECT 
		  upper( TRIM( MDCL_SPC_LVL_NAME ) )                 AS                                  MDCL_SPC_LVL_NAME 
		, upper( TRIM( MDCL_SPC_LVL_CODE ) )                 AS                                  MDCL_SPC_LVL_CODE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		from SRC_MSPL
            ),
LOGIC_CRTT as ( SELECT 
		  upper( TRIM( CRDN_TRM_DESCR ) )                    AS                                     CRDN_TRM_DESCR 
		, upper( TRIM( CRDN_TRM_TYPE_CODE ) )                AS                                 CRDN_TRM_TYPE_CODE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		from SRC_CRTT
            )

---- RENAME LAYER ----
,

RENAME_PCRD as ( SELECT 
		  PRVDR_BASE_NMBR                                    as                                    PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                     PRVDR_SFX_NMBR
		, PCRD_CRT_DTTM                                      as                                      PCRD_CRT_DTTM
		, CRT_USER_CODE                                      as                                 PCRD_CRT_USER_CODE
		, DCTVT_DTTM                                         as                                    PCRD_DCTVT_DTTM
		, DCTVT_USER_CODE                                    as                               PCRD_DCTVT_USER_CODE
		, CRDN_ID                                            as                                       PCRD_CRDN_ID 
				FROM     LOGIC_PCRD   ), 
RENAME_CRDD as ( SELECT 
		  CRDN_CTGRY_CODE                                    as                                    CRDN_CTGRY_CODE
		, CRDN_TYPE_CODE                                     as                                     CRDN_TYPE_CODE
		, CRDN_ID_NMBR                                       as                                       CRDN_ID_NMBR
		, CRDN_CNFRM_IND                                     as                                     CRDN_CNFRM_IND
		, STATE_CODE                                         as                                         STATE_CODE
		, EFCTV_DATE                                         as                                         EFCTV_DATE
		, EXPRT_DATE                                         as                                         EXPRT_DATE
		, MDCL_SPC_TYPE_CODE                                 as                                 MDCL_SPC_TYPE_CODE
		, MDCL_SPC_LVL_CODE                                  as                                  MDCL_SPC_LVL_CODE
		, TRMNT_DATE                                         as                                         TRMNT_DATE
		, CRDN_TRM_TYPE_CODE                                 as                                 CRDN_TRM_TYPE_CODE
		, CRDD_CRT_DTTM                                      as                                      CRDD_CRT_DTTM
		, CRDN_ID                                            as                                       CRDD_CRDN_ID
		, CRT_USER_CODE                                      as                                 CRDD_CRT_USER_CODE
		, DCTVT_DTTM                                         as                                    CRDD_DCTVT_DTTM
		, DCTVT_USER_CODE                                    as                               CRDD_DCTVT_USER_CODE
		, DCTVT_MTHD_CODE                                    as                                    DCTVT_MTHD_CODE 
				FROM     LOGIC_CRDD   ), 
RENAME_CRDC as ( SELECT 
		  CRDN_CTGRY_NAME                                    as                                    CRDN_CTGRY_NAME
		, CRDN_CTGRY_CODE                                    as                               CRDC_CRDN_CTGRY_CODE
		, DCTVT_DTTM                                         as                                    CRDC_DCTVT_DTTM 
				FROM     LOGIC_CRDC   ), 
RENAME_CRDT as ( SELECT 
		  CRDN_TYPE_NAME                                     as                                     CRDN_TYPE_NAME
		, CRDN_TYPE_CODE                                     as                                CRDT_CRDN_TYPE_CODE
		, DCTVT_DTTM                                         as                                    CRDT_DCTVT_DTTM 
				FROM     LOGIC_CRDT   ), 
RENAME_MSPT as ( SELECT 
		  MDCL_SPC_TYPE_NAME                                 as                                 MDCL_SPC_TYPE_NAME
		, MDCL_SPC_TYPE_CODE                                 as                            MSPT_MDCL_SPC_TYPE_CODE
		, CRT_DTTM                                           as                                      MSPT_CRT_DTTM
		, CRT_USER_CODE                                      as                                 MSPT_CRT_USER_CODE
		, DCTVT_DTTM                                         as                                    MSPT_DCTVT_DTTM
		, DCTVT_USER_CODE                                    as                               MSPT_DCTVT_USER_CODE 
				FROM     LOGIC_MSPT   ), 
RENAME_MSPL as ( SELECT 
		  MDCL_SPC_LVL_NAME                                  as                                  MDCL_SPC_LVL_NAME
		, MDCL_SPC_LVL_CODE                                  as                             MSPL_MDCL_SPC_LVL_CODE
		, DCTVT_DTTM                                         as                                    MSPL_DCTVT_DTTM 
				FROM     LOGIC_MSPL   ), 
RENAME_CRTT as ( SELECT 
		  CRDN_TRM_DESCR                                     as                                     CRDN_TRM_DESCR
		, CRDN_TRM_TYPE_CODE                                 as                            CRTT_CRDN_TRM_TYPE_CODE
		, DCTVT_DTTM                                         as                                    CRTT_DCTVT_DTTM 
				FROM     LOGIC_CRTT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PCRD                           as ( SELECT * from    RENAME_PCRD   ),
FILTER_CRDD                           as ( SELECT * from    RENAME_CRDD   ),
FILTER_CRDT                           as ( SELECT * from    RENAME_CRDT 
				WHERE CRDT_DCTVT_DTTM > current_date  ),
FILTER_CRTT                           as ( SELECT * from    RENAME_CRTT 
				WHERE CRTT_DCTVT_DTTM > current_date  ),
FILTER_CRDC                           as ( SELECT * from    RENAME_CRDC 
				WHERE CRDC_DCTVT_DTTM > current_date  ),
FILTER_MSPT                           as ( SELECT * from    RENAME_MSPT 
				WHERE MSPT_DCTVT_DTTM > current_date  ),
FILTER_MSPL                           as ( SELECT * from    RENAME_MSPL 
				WHERE MSPL_DCTVT_DTTM > current_date  ),

---- JOIN LAYER ----

CRDD as ( SELECT * 
				FROM  FILTER_CRDD
				LEFT JOIN FILTER_CRDT ON  FILTER_CRDD.CRDN_TYPE_CODE =  FILTER_CRDT.CRDT_CRDN_TYPE_CODE 
								LEFT JOIN FILTER_CRTT ON  FILTER_CRDD.CRDN_TRM_TYPE_CODE =  FILTER_CRTT.CRTT_CRDN_TRM_TYPE_CODE 
								LEFT JOIN FILTER_CRDC ON  FILTER_CRDD.CRDN_CTGRY_CODE =  FILTER_CRDC.CRDC_CRDN_CTGRY_CODE 
								LEFT JOIN FILTER_MSPT ON  FILTER_CRDD.MDCL_SPC_TYPE_CODE =  FILTER_MSPT.MSPT_MDCL_SPC_TYPE_CODE 
								LEFT JOIN FILTER_MSPL ON  FILTER_CRDD.MDCL_SPC_LVL_CODE =  FILTER_MSPL.MSPL_MDCL_SPC_LVL_CODE  ),
PCRD as ( SELECT * 
				FROM  FILTER_PCRD
				INNER JOIN CRDD  ON FILTER_PCRD.PCRD_CRDN_ID = CRDD.CRDD_CRDN_ID   )
SELECT * 
from PCRD