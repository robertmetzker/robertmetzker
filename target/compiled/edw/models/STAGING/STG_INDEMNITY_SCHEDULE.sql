---- SRC LAYER ----
WITH
SRC_ISCH as ( SELECT *     from     DEV_VIEWS.PCMP.INDEMNITY_SCHEDULE ),
SRC_IFT as ( SELECT *     from     DEV_VIEWS.PCMP.INDEMNITY_FREQUENCY_TYPE ),
//SRC_ISCH as ( SELECT *     from     INDEMNITY_SCHEDULE) ,
//SRC_IFT as ( SELECT *     from     INDEMNITY_FREQUENCY_TYPE) ,

---- LOGIC LAYER ----

LOGIC_ISCH as ( SELECT 
		  INDM_SCH_ID                                        AS                                        INDM_SCH_ID 
		, INDM_PAY_ID                                        AS                                        INDM_PAY_ID 
		, upper( TRIM( INDM_FREQ_TYP_CD ) )                  AS                                   INDM_FREQ_TYP_CD 
		, cast( INDM_SCH_FST_PRCS_DT as DATE )               AS                               INDM_SCH_FST_PRCS_DT 
		, upper( INDM_SCH_AUTO_PAY_IND )                     AS                              INDM_SCH_AUTO_PAY_IND 
		, upper( INDM_SCH_OFST_CHG_IND )                     AS                              INDM_SCH_OFST_CHG_IND 
		, INDM_SCH_NO_OF_PAY                                 AS                                 INDM_SCH_NO_OF_PAY 
		, AUTH_ID                                            AS                                            AUTH_ID 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_ISCH
            ),
LOGIC_IFT as ( SELECT 
		  upper( TRIM( INDM_FREQ_TYP_NM ) )                  AS                                   INDM_FREQ_TYP_NM 
		, upper( TRIM( INDM_FREQ_TYP_CD ) )                  AS                                   INDM_FREQ_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_IFT
            )

---- RENAME LAYER ----
,

RENAME_ISCH as ( SELECT 
		  INDM_SCH_ID                                        as                                        INDM_SCH_ID
		, INDM_PAY_ID                                        as                                        INDM_PAY_ID
		, INDM_FREQ_TYP_CD                                   as                                   INDM_FREQ_TYP_CD
		, INDM_SCH_FST_PRCS_DT                               as                               INDM_SCH_FST_PRCS_DT
		, INDM_SCH_AUTO_PAY_IND                              as                              INDM_SCH_AUTO_PAY_IND
		, INDM_SCH_OFST_CHG_IND                              as                              INDM_SCH_OFST_CHG_IND
		, INDM_SCH_NO_OF_PAY                                 as                                 INDM_SCH_NO_OF_PAY
		, AUTH_ID                                            as                                            AUTH_ID
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_ISCH   ), 
RENAME_IFT as ( SELECT 
		  INDM_FREQ_TYP_NM                                   as                                   INDM_FREQ_TYP_NM
		, INDM_FREQ_TYP_CD                                   as                               IFT_INDM_FREQ_TYP_CD
		, VOID_IND                                           as                                       IFT_VOID_IND 
				FROM     LOGIC_IFT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ISCH                           as ( SELECT * from    RENAME_ISCH   ),
FILTER_IFT                            as ( SELECT * from    RENAME_IFT 
				WHERE IFT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

ISCH as ( SELECT * 
				FROM  FILTER_ISCH
				LEFT JOIN FILTER_IFT ON  FILTER_ISCH.INDM_FREQ_TYP_CD =  FILTER_IFT.IFT_INDM_FREQ_TYP_CD  )
SELECT * 
from ISCH