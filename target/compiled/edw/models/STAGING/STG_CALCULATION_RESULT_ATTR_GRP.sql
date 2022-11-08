---- SRC LAYER ----
WITH
SRC_CRAG           as ( SELECT *     FROM     DEV_VIEWS.PCMP.CALCULATION_RESULT_ATTR_GRP ),
SRC_CRT            as ( SELECT *     FROM      DEV_VIEWS.PCMP.CALCULATION_RESULT_TYPE ),
SRC_CRAGT          as ( SELECT *     FROM     DEV_VIEWS.PCMP.CALCULATION_RSLT_ATTR_GRP_TYP ),
SRC_CRA            as ( SELECT *     FROM      DEV_VIEWS.PCMP.CALCULATION_RESULT_ATTRIBUTE ),
SRC_CRAT           as ( SELECT *     FROM     DEV_VIEWS.PCMP.CALCULATION_RESULT_ATTR_TYP ),
//SRC_CRAG           as ( SELECT *     FROM     CALCULATION_RESULT_ATTR_GRP) ,
//SRC_CRT            as ( SELECT *     FROM     CALCULATION_RESULT_TYPE) ,
//SRC_CRAGT          as ( SELECT *     FROM     CALCULATION_RSLT_ATTR_GRP_TYP) ,
//SRC_CRA            as ( SELECT *     FROM     CALCULATION_RESULT_ATTRIBUTE) ,
//SRC_CRAT           as ( SELECT *     FROM     CALCULATION_RESULT_ATTR_TYP) ,

---- LOGIC LAYER ----


LOGIC_CRAG as ( SELECT 
		  CALC_RSLT_ATTR_GRP_ID                              as                              CALC_RSLT_ATTR_GRP_ID 
		, CALC_RSLT_ATTR_GRP_SEQ_NO                          as                          CALC_RSLT_ATTR_GRP_SEQ_NO 
		, CALC_RSLT_ID                                       as                                       CALC_RSLT_ID 
		, upper( TRIM( CALC_RSLT_ATTR_GRP_TYP_CD ) )         as                          CALC_RSLT_ATTR_GRP_TYP_CD 
		FROM SRC_CRAG
            ),

LOGIC_CRT as ( SELECT 
		  upper( TRIM( CALC_RSLT_TYP_CD ) )                  as                                   CALC_RSLT_TYP_CD 
		, upper( TRIM( CALC_RSLT_TYP_NM ) )                  as                                   CALC_RSLT_TYP_NM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		FROM SRC_CRT
            ),

LOGIC_CRAGT as ( SELECT 
		  upper( TRIM( CALC_RSLT_ATTR_GRP_TYP_NM ) )         as                          CALC_RSLT_ATTR_GRP_TYP_NM 
		, upper( TRIM( CALC_RSLT_ATTR_GRP_TYP_CD ) )         as                          CALC_RSLT_ATTR_GRP_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		, upper( TRIM( CALC_RSLT_TYP_CD ) )                  as                                   CALC_RSLT_TYP_CD 
		FROM SRC_CRAGT
            ),

LOGIC_CRA as ( SELECT 
		  upper( TRIM( CALC_RSLT_ATTR_TEXT_VAL ) )           as                            CALC_RSLT_ATTR_TEXT_VAL 
		, upper( TRIM( CALC_RSLT_ATTR_TYP_CD ) )             as                              CALC_RSLT_ATTR_TYP_CD 
		, CALC_RSLT_ATTR_GRP_ID                              as                              CALC_RSLT_ATTR_GRP_ID 
		, CALC_RSLT_ATTR_GRP_SEQ_NO                          as                          CALC_RSLT_ATTR_GRP_SEQ_NO 
		FROM SRC_CRA
            ),

LOGIC_CRAT as ( SELECT 
		  upper( TRIM( CALC_RSLT_ATTR_TYP_NM ) )             as                              CALC_RSLT_ATTR_TYP_NM 
		, upper( TRIM( CALC_RSLT_ATTR_TYP_CD ) )             as                              CALC_RSLT_ATTR_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		FROM SRC_CRAT
            )

---- RENAME LAYER ----
,

RENAME_CRAG       as ( SELECT 
		  CALC_RSLT_ATTR_GRP_ID                              as                              CALC_RSLT_ATTR_GRP_ID
		, CALC_RSLT_ATTR_GRP_SEQ_NO                          as                          CALC_RSLT_ATTR_GRP_SEQ_NO
		, CALC_RSLT_ID                                       as                                       CALC_RSLT_ID
		, CALC_RSLT_ATTR_GRP_TYP_CD                          as                          CALC_RSLT_ATTR_GRP_TYP_CD 
				FROM     LOGIC_CRAG   ), 
RENAME_CRT        as ( SELECT 
		  CALC_RSLT_TYP_CD                                   as                                   CALC_RSLT_TYP_CD
		, CALC_RSLT_TYP_NM                                   as                                   CALC_RSLT_TYP_NM
		, VOID_IND                                           as                                       CRT_VOID_IND 
				FROM     LOGIC_CRT   ), 
RENAME_CRAGT      as ( SELECT 
		  CALC_RSLT_ATTR_GRP_TYP_NM                          as                          CALC_RSLT_ATTR_GRP_TYP_NM
		, CALC_RSLT_ATTR_GRP_TYP_CD                          as                    CRAGT_CALC_RSLT_ATTR_GRP_TYP_CD
		, VOID_IND                                           as                                     CRAGT_VOID_IND
		, CALC_RSLT_TYP_CD                                   as                             CRAGT_CALC_RSLT_TYP_CD 
				FROM     LOGIC_CRAGT   ), 
RENAME_CRA        as ( SELECT 
		  CALC_RSLT_ATTR_TEXT_VAL                            as                            CALC_RSLT_ATTR_TEXT_VAL
		, CALC_RSLT_ATTR_TYP_CD                              as                              CALC_RSLT_ATTR_TYP_CD
		, CALC_RSLT_ATTR_GRP_ID                              as                          CRA_CALC_RSLT_ATTR_GRP_ID
		, CALC_RSLT_ATTR_GRP_SEQ_NO                          as                      CRA_CALC_RSLT_ATTR_GRP_SEQ_NO 
				FROM     LOGIC_CRA   ), 
RENAME_CRAT       as ( SELECT 
		  CALC_RSLT_ATTR_TYP_NM                              as                              CALC_RSLT_ATTR_TYP_NM
		, CALC_RSLT_ATTR_TYP_CD                              as                         CRAT_CALC_RSLT_ATTR_TYP_CD
		, VOID_IND                                           as                                      CRAT_VOID_IND 
				FROM     LOGIC_CRAT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CRAG                           as ( SELECT * FROM    RENAME_CRAG   ),
FILTER_CRAGT                          as ( SELECT * FROM    RENAME_CRAGT 
                                            WHERE CRAGT_VOID_IND = 'N'  ),
FILTER_CRA                            as ( SELECT * FROM    RENAME_CRA   ),
FILTER_CRAT                           as ( SELECT * FROM    RENAME_CRAT 
                                            WHERE CRAT_VOID_IND = 'N'  ),
FILTER_CRT                            as ( SELECT * FROM    RENAME_CRT 
                                            WHERE CRT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CRAGT as ( SELECT * 
				FROM  FILTER_CRAGT
				LEFT JOIN FILTER_CRT ON  FILTER_CRAGT.CRAGT_CALC_RSLT_TYP_CD =  FILTER_CRT.CALC_RSLT_TYP_CD  ),
CRA as ( SELECT * 
				FROM  FILTER_CRA
				LEFT JOIN FILTER_CRAT ON  FILTER_CRA.CALC_RSLT_ATTR_TYP_CD =  FILTER_CRAT.CRAT_CALC_RSLT_ATTR_TYP_CD  ),
CRAG as ( SELECT * 
				FROM  FILTER_CRAG
				LEFT JOIN CRAGT ON  FILTER_CRAG.CALC_RSLT_ATTR_GRP_TYP_CD = CRAGT.CRAGT_CALC_RSLT_ATTR_GRP_TYP_CD 
						LEFT JOIN CRA ON  FILTER_CRAG.CALC_RSLT_ATTR_GRP_ID = CRA.CRA_CALC_RSLT_ATTR_GRP_ID AND CALC_RSLT_ATTR_GRP_SEQ_NO = CRA_CALC_RSLT_ATTR_GRP_SEQ_NO  )
SELECT * 
FROM CRAG