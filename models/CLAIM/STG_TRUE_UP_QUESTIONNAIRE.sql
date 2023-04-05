

---- SRC LAYER ----
WITH
SRC_A              as ( SELECT *     FROM     {{ ref( 'EMPLR_ANSWER' ) }} ),
SRC_Q              as ( SELECT *     FROM     {{ ref( 'EMPLR_QUESTION' ) }} ),
SRC_SO             as ( SELECT *     FROM     {{ ref( 'TEGSVOC' ) }} ),
//SRC_A              as ( SELECT *     FROM     EMPLR_ANSWER) ,
//SRC_Q              as ( SELECT *     FROM     EMPLR_QUESTION) ,
//SRC_SO             as ( SELECT *     FROM     TEGSVOC) ,

---- LOGIC LAYER ----


LOGIC_A as ( SELECT 
		  PLCY_NMBR                                          as                                          PLCY_NMBR 
		, BUS_SEQ_NMBR                                       as                                       BUS_SEQ_NMBR 
		, PRD_BGNG_DATE                                      as                                      PRD_BGNG_DATE 
		, PRD_ENDNG_DATE                                     as                                     PRD_ENDNG_DATE 
		, ANSWR_CRT_DTTM                                     as                                     ANSWR_CRT_DTTM 
		, upper( NULLIF( TRIM( QSTN_CODE ),'') )             as                                          QSTN_CODE 
		, upper( NULLIF( TRIM( ANSWR_TEXT ),'') )            as                                         ANSWR_TEXT 
		, upper( NULLIF( TRIM( SRVC_OFRNG_CODE ),'') )       as                                    SRVC_OFRNG_CODE 
		, upper( NULLIF( TRIM( CRT_PRGRM_NAME ),'') )        as                                     CRT_PRGRM_NAME 
		, upper( NULLIF( TRIM( CRT_USER_CODE ),'') )         as                                      CRT_USER_CODE 
		, upper( NULLIF( TRIM( DCTVT_PRGRM_NAME ),'') )      as                                   DCTVT_PRGRM_NAME 
		, upper( NULLIF( TRIM( DCTVT_USER_CODE ),'') )       as                                    DCTVT_USER_CODE 
		, DCTVT_DTTM                                         as                                         DCTVT_DTTM 
		FROM SRC_A
            ),

LOGIC_Q as ( SELECT 
		  upper( NULLIF( TRIM( QSTN_TEXT ),'') )             as                                          QSTN_TEXT 
		, upper( NULLIF( TRIM( QSTN_EXPLN_TEXT ),'') )       as                                    QSTN_EXPLN_TEXT 
		FROM SRC_Q
            ),

LOGIC_SO as ( SELECT 
		  upper( NULLIF( TRIM( CODE_DESC ),'') )             as                                          CODE_DESC 
		FROM SRC_SO
            )

---- RENAME LAYER ----
,

RENAME_A          as ( SELECT 
		  PLCY_NMBR                                          as                                          PLCY_NMBR
		, BUS_SEQ_NMBR                                       as                                       BUS_SEQ_NMBR
		, PRD_BGNG_DATE                                      as                                      PRD_BGNG_DATE
		, PRD_ENDNG_DATE                                     as                                     PRD_ENDNG_DATE
		, ANSWR_CRT_DTTM                                     as                                     ANSWR_CRT_DTTM
		, QSTN_CODE                                          as                                          QSTN_CODE
		, ANSWR_TEXT                                         as                                         ANSWR_TEXT
		, SRVC_OFRNG_CODE                                    as                                    SRVC_OFRNG_CODE
		, CRT_PRGRM_NAME                                     as                                     CRT_PRGRM_NAME
		, CRT_USER_CODE                                      as                                      CRT_USER_CODE
		, DCTVT_PRGRM_NAME                                   as                                   DCTVT_PRGRM_NAME
		, DCTVT_USER_CODE                                    as                                    DCTVT_USER_CODE
		, DCTVT_DTTM                                         as                                         DCTVT_DTTM 
				FROM     LOGIC_A   ), 
RENAME_Q          as ( SELECT 
		  QSTN_TEXT                                          as                                          QSTN_TEXT
		, QSTN_EXPLN_TEXT                                    as                                    QSTN_EXPLN_TEXT 
				FROM     LOGIC_Q   ), 
RENAME_SO         as ( SELECT 
		  CODE_DESC                                          as                                    SRVC_OFRNG_DESC 
				FROM     LOGIC_SO   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_A                              as ( SELECT * FROM    RENAME_A   ),
FILTER_Q                              as ( SELECT * FROM    RENAME_Q 
                                            WHERE DCTVT_DTTM IS NULL  ),
FILTER_SO                             as ( SELECT * FROM    RENAME_SO 
                                            WHERE DCTVT_DTTM IS NULL  ),

---- JOIN LAYER ----

A as ( SELECT * 
				FROM  FILTER_A
				INNER JOIN INNER JOIN  FILTER_Q  using( QSTN_CODE )  
								LEFT JOIN FILTER_SO ON  FILTER_A.SRVC_OFRNG_CODE =  FILTER_SO.SRVC_OFRNG_CODE  )
SELECT * 
FROM A