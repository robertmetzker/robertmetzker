

---- SRC LAYER ----
WITH

SRC_CNEA           as ( SELECT *     FROM     {{ ref( 'TMPCNEA' ) }} ),
SRC_ADRS           as ( SELECT *     FROM     {{ ref( 'TMPADRS' ) }} ),
SRC_ST             as ( SELECT *     FROM     {{ ref( 'STATE' ) }} ),
SRC_ADRT           as ( SELECT *     FROM     {{ ref( 'TMPADRT' ) }} ),

/*
SRC_CNEA           as ( SELECT *     FROM     BWC_PEACH.TMPCNEA ),
SRC_ADRS           as ( SELECT *     FROM     BWC_PEACH.TMPADRS ),
SRC_ST             as ( SELECT *     FROM     PCMP.STATE ),
SRC_ADRT           as ( SELECT *     FROM     BWC_PEACH.TMPADRT ),

*/

---- LOGIC LAYER ----


, LOGIC_CNEA as ( 
	SELECT 	  
		 cast( PRVDR_BASE_NMBR as TEXT )                    as                                    PRVDR_BASE_NMBR	, 
		 cast( PRVDR_SFX_NMBR as TEXT )                     as                                     PRVDR_SFX_NMBR	, 
		 NULLIF( TRIM( ADRS_TYPE_CODE ),'' )                as                                     ADRS_TYPE_CODE	, 
		 ADRS_TYPE_LCT_NMBR                                 as                                 ADRS_TYPE_LCT_NMBR	, 
		 CNEA_CRT_DTTM                                      as                                      CNEA_CRT_DTTM	, 
		 ADRS_ID                                            as                                            ADRS_ID	, 
		 NULLIF( TRIM( PRMRY_LCTN_IND ),'' )                as                                     PRMRY_LCTN_IND	, 
		 EFCTV_DATE                                         as                                         EFCTV_DATE	, 
		 ENDNG_DATE                                         as                                         ENDNG_DATE	, 
		 NULLIF( TRIM( CRT_USER_CODE ),'' )                 as                                      CRT_USER_CODE	, 
		 DCTVT_DTTM                                         as                                         DCTVT_DTTM	, 
		 NULLIF( TRIM( DCTVT_USER_CODE ),'' )               as                                    DCTVT_USER_CODE	, 
		 NULLIF( TRIM( UPDT_PRGRM_NAME ),'' )               as                                    UPDT_PRGRM_NAME
	 from SRC_CNEA )

, LOGIC_ADRS as ( 
	SELECT 	  
		 ADRS_CRT_DTTM                                      as                                      ADRS_CRT_DTTM	, 
		 NULLIF( TRIM( LINE_1_ADRS ),'' )                   as                                        LINE_1_ADRS	, 
		 NULLIF( TRIM( LINE_2_ADRS ),'' )                   as                                        LINE_2_ADRS	, 
		 NULLIF( TRIM( P_O_BOX_NMBR ),'' )                  as                                       P_O_BOX_NMBR	, 
		 NULLIF( TRIM( CITY_NAME ),'' )                     as                                          CITY_NAME	, 
		 NULLIF( TRIM( STATE_CODE ),'' )                    as                                         STATE_CODE	, 
		 NULLIF( TRIM( ZIP_CODE_NMBR ),'' )                 as                                      ZIP_CODE_NMBR	, 
		 NULLIF( TRIM( ZIP_CODE_PLS4_NMBR ),'' )            as                                 ZIP_CODE_PLS4_NMBR	, 
		 NULLIF( TRIM( CNTY_CODE ),'' )                     as                                          CNTY_CODE	, 
		 NULLIF( TRIM( CNTY_NAME ),'' )                     as                                          CNTY_NAME	, 
		 NULLIF( TRIM( FRGN_ADRS_IND ),'' )                 as                                      FRGN_ADRS_IND	, 
		 NULLIF( TRIM( FRGN_TRTRY_NAME ),'' )               as                                    FRGN_TRTRY_NAME	, 
		 NULLIF( TRIM( FRGN_PSTL_CODE ),'' )                as                                     FRGN_PSTL_CODE	, 
		 NULLIF( TRIM( FRGN_CNTRY_NAME ),'' )               as                                    FRGN_CNTRY_NAME	, 
		 NULLIF( TRIM( FNLST_VLDTN_IND ),'' )               as                                    FNLST_VLDTN_IND	, 
		 NULLIF( TRIM( FNLST_OVRD_IND ),'' )                as                                     FNLST_OVRD_IND	, 
		 NULLIF( TRIM( CRT_USER_CODE ),'' )                 as                                      CRT_USER_CODE	, 
		 DCTVT_DTTM                                         as                                         DCTVT_DTTM	, 
		 NULLIF( TRIM( DCTVT_USER_CODE ),'' )               as                                    DCTVT_USER_CODE	, 
		 NULLIF( TRIM( UPDT_PRGRM_NAME ),'' )               as                                    UPDT_PRGRM_NAME	, 
		 ADRS_ID                                            as                                            ADRS_ID
	 from SRC_ADRS )

, LOGIC_ST as ( 
	SELECT 	  
		 NULLIF( TRIM( STT_NM ),'' )                        as                                             STT_NM	, 
		 NULLIF( TRIM( STT_ABRV ),'' )                      as                                           STT_ABRV
	 from SRC_ST )

, LOGIC_ADRT as ( 
	SELECT 	  
		 NULLIF( TRIM( ADRS_TYPE_DESCR ),'' )               as                                    ADRS_TYPE_DESCR	, 
		 CRT_DTTM                                           as                                           CRT_DTTM	, 
		 EFCTV_DATE                                         as                                         EFCTV_DATE	, 
		 ENDNG_DATE                                         as                                         ENDNG_DATE	, 
		 NULLIF( TRIM( CRT_USER_CODE ),'' )                 as                                      CRT_USER_CODE	, 
		 DCTVT_DTTM                                         as                                         DCTVT_DTTM	, 
		 NULLIF( TRIM( DCTVT_USER_CODE ),'' )               as                                    DCTVT_USER_CODE	, 
		 NULLIF( TRIM( UPDT_PRGRM_NAME ),'' )               as                                    UPDT_PRGRM_NAME	, 
		 NULLIF( TRIM( ADRS_TYPE_CODE ),'' )                as                                     ADRS_TYPE_CODE
	 from SRC_ADRT )


---- RENAME LAYER ----


, RENAME_CNEA as ( SELECT  
		 PRVDR_BASE_NMBR                                    as                                    PRVDR_BASE_NMBR , 
		 PRVDR_SFX_NMBR                                     as                                     PRVDR_SFX_NMBR , 
		 ADRS_TYPE_CODE                                     as                                     ADRS_TYPE_CODE , 
		 ADRS_TYPE_LCT_NMBR                                 as                                 ADRS_TYPE_LCT_NMBR , 
		 CNEA_CRT_DTTM                                      as                                      CNEA_CRT_DTTM , 
		 ADRS_ID                                            as                                            ADRS_ID , 
		 PRMRY_LCTN_IND                                     as                                     PRMRY_LCTN_IND , 
		 EFCTV_DATE                                         as                                         EFCTV_DATE , 
		 ENDNG_DATE                                         as                                         ENDNG_DATE , 
		 CRT_USER_CODE                                      as                                      CRT_USER_CODE , 
		 DCTVT_DTTM                                         as                                         DCTVT_DTTM , 
		 DCTVT_USER_CODE                                    as                                    DCTVT_USER_CODE , 
		 UPDT_PRGRM_NAME                                    as                                    UPDT_PRGRM_NAME 
		FROM LOGIC_CNEA
            )

, RENAME_ADRS as ( SELECT  
		 ADRS_CRT_DTTM                                      as                                      ADRS_CRT_DTTM , 
		 LINE_1_ADRS                                        as                                        LINE_1_ADRS , 
		 LINE_2_ADRS                                        as                                        LINE_2_ADRS , 
		 P_O_BOX_NMBR                                       as                                       P_O_BOX_NMBR , 
		 CITY_NAME                                          as                                          CITY_NAME , 
		 STATE_CODE                                         as                                         STATE_CODE , 
		 ZIP_CODE_NMBR                                      as                                      ZIP_CODE_NMBR , 
		 ZIP_CODE_PLS4_NMBR                                 as                                 ZIP_CODE_PLS4_NMBR , 
		 CNTY_CODE                                          as                                          CNTY_CODE , 
		 CNTY_NAME                                          as                                          CNTY_NAME , 
		 FRGN_ADRS_IND                                      as                                      FRGN_ADRS_IND , 
		 FRGN_TRTRY_NAME                                    as                                    FRGN_TRTRY_NAME , 
		 FRGN_PSTL_CODE                                     as                                     FRGN_PSTL_CODE , 
		 FRGN_CNTRY_NAME                                    as                                    FRGN_CNTRY_NAME , 
		 FNLST_VLDTN_IND                                    as                                    FNLST_VLDTN_IND , 
		 FNLST_OVRD_IND                                     as                                     FNLST_OVRD_IND , 
		 CRT_USER_CODE                                      as                                 ADRS_CRT_USER_CODE , 
		 DCTVT_DTTM                                         as                                    ADRS_DCTVT_DTTM , 
		 DCTVT_USER_CODE                                    as                               ADRS_DCTVT_USER_CODE , 
		 UPDT_PRGRM_NAME                                    as                               ADRS_UPDT_PRGRM_NAME , 
		 ADRS_ID                                            as                                       ADRS_ADRS_ID 
		FROM LOGIC_ADRS
            )

, RENAME_ST as ( SELECT  
		 STT_NM                                             as                                         STATE_NAME , 
		 STT_ABRV                                           as                                           STT_ABRV 
		FROM LOGIC_ST
            )

, RENAME_ADRT as ( SELECT  
		 ADRS_TYPE_DESCR                                    as                                    ADRS_TYPE_DESCR , 
		 CRT_DTTM                                           as                                           CRT_DTTM , 
		 EFCTV_DATE                                         as                                    ADRT_EFCTV_DATE , 
		 ENDNG_DATE                                         as                                    ADRT_ENDNG_DATE , 
		 CRT_USER_CODE                                      as                                 ADRT_CRT_USER_CODE , 
		 DCTVT_DTTM                                         as                                    ADRT_DCTVT_DTTM , 
		 DCTVT_USER_CODE                                    as                               ADRT_DCTVT_USER_CODE , 
		 UPDT_PRGRM_NAME                                    as                               ADRT_UPDT_PRGRM_NAME , 
		 ADRS_TYPE_CODE                                     as                                ADRT_ADRS_TYPE_CODE 
		FROM LOGIC_ADRT
            )

---- FILTER LAYER ----

FILTER_CNEA                           as ( SELECT * FROM    RENAME_CNEA   ),
FILTER_ADRS                           as ( SELECT * FROM    RENAME_ADRS   ),
FILTER_ST                             as ( SELECT * FROM    RENAME_ST   ),
FILTER_ADRT                           as ( SELECT * FROM    RENAME_ADRT   )

---- JOIN LAYER ----

STATE_CODE as ( SELECT * 
				FROM  FILTER_STATE_CODE
				LEFT JOIN FILTER_ST ON  FILTER_STATE_CODE =  FILTER_STT_ABRV  ),
CNEA as ( SELECT * 
				FROM  FILTER_CNEA
				INNER JOIN FILTER_ADRS ON  FILTER_CNEA.ADRS_ID =  FILTER_ADRS.ADRS_ADRS_ID 
						INNER JOIN FILTER_ADRT ON  FILTER_CNEA.ADRS_TYPE_CODE =  FILTER_ADRT.ADRT_ADRS_TYPE_CODE  )
SELECT 
		  PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR
		, ADRS_TYPE_CODE
		, ADRS_TYPE_LCT_NMBR
		, CNEA_CRT_DTTM
		, ADRS_ID
		, PRMRY_LCTN_IND
		, EFCTV_DATE
		, ENDNG_DATE
		, CRT_USER_CODE
		, DCTVT_DTTM
		, DCTVT_USER_CODE
		, UPDT_PRGRM_NAME
		, ADRS_CRT_DTTM
		, LINE_1_ADRS
		, LINE_2_ADRS
		, P_O_BOX_NMBR
		, CITY_NAME
		, STATE_CODE
		, STATE_NAME
		, ZIP_CODE_NMBR
		, ZIP_CODE_PLS4_NMBR
		, CNTY_CODE
		, CNTY_NAME
		, FRGN_ADRS_IND
		, FRGN_TRTRY_NAME
		, FRGN_PSTL_CODE
		, FRGN_CNTRY_NAME
		, FNLST_VLDTN_IND
		, FNLST_OVRD_IND
		, ADRS_CRT_USER_CODE
		, ADRS_DCTVT_DTTM
		, ADRS_DCTVT_USER_CODE
		, ADRS_UPDT_PRGRM_NAME
		, ADRS_TYPE_DESCR
		, CRT_DTTM
		, ADRT_EFCTV_DATE
		, ADRT_ENDNG_DATE
		, ADRT_CRT_USER_CODE
		, ADRT_DCTVT_DTTM
		, ADRT_DCTVT_USER_CODE
		, ADRT_UPDT_PRGRM_NAME
		, ADRS_ADRS_ID
		, ADRT_ADRS_TYPE_CODE 
FROM CNEA