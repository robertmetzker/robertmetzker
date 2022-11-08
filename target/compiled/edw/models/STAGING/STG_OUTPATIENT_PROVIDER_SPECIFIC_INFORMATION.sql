---- SRC LAYER ----
WITH
SRC_A as ( SELECT *     from     DEV_VIEWS.BASE.PRO_OUT_PSF ),
SRC_B as ( SELECT *     from     DEV_VIEWS.BASE.PRO ),
SRC_C as ( SELECT *     from     DEV_VIEWS.BASE.REF ),
SRC_W as ( SELECT *     from     DEV_VIEWS.BASE.WAGE_INDEX ),

---- LOGIC LAYER ----

LOGIC_A as ( SELECT 
		  PRO_RID                                            AS                                            PRO_RID 
		, cast( EFFECTIVE_DATE as DATE )                     AS                                     EFFECTIVE_DATE 
		, cast( EXPIRATION_DATE as DATE )                    AS                                    EXPIRATION_DATE 
		, upper( TRIM( ENTRY_USER_ID ) )                     AS                                      ENTRY_USER_ID 
		, cast( ENTRY_DATE as DATE )                         AS                                         ENTRY_DATE 
		, upper( TRIM( ULM ) )                               AS                                                ULM 
		, DLM                                                AS                                                DLM 
		, upper(NULLIF( TRIM( QUALITY_IND ), ''))         AS                                        QUALITY_IND 
		, upper( TRIM( CRITICAL_ACCESS ) )                   AS                                    CRITICAL_ACCESS 
		, upper( TRIM( PRO_TYPE ) )                          AS                                           PRO_TYPE 
		, PAY_TO_COST                                        AS                                        PAY_TO_COST 
		, OUT_CCR                                            AS                                            OUT_CCR 
		, upper(NULLIF( TRIM( CBSA ), ''))                   AS                                               CBSA 
		, DEV_CCR                                            AS                                            DEV_CCR 
		, PUB_DEV_CCR                                        AS                                        PUB_DEV_CCR 
		from SRC_A
            ),
LOGIC_B as ( SELECT 
		  upper( TRIM( PRO_NUM ) )                           AS                                            PRO_NUM 
		, upper( TRIM( PRO_NAME ) )                          AS                                           PRO_NAME 
		, upper( TRIM( DBA_NAME ) )                          AS                                           DBA_NAME 
		, cast( DATE_OF_DEATH as DATE )                      AS                                      DATE_OF_DEATH 
		, cast( LAST_TRANS_DATE as DATE )                    AS                                    LAST_TRANS_DATE 
		, upper( TRIM( DEP_IND ) )                           AS                                            DEP_IND 
		, EDI_ID                                             AS                                             EDI_ID 
		, upper( TRIM( PRO_TYPE ) )                          AS                                           PRO_TYPE 
		, cast( PRO_DLM as DATE )                            AS                                            PRO_DLM 
		, upper( TRIM( PRO_ULM ) )                           AS                                            PRO_ULM 
		, PRO_ID                                             AS                                             PRO_ID 
		from SRC_B
            ),
LOGIC_C as ( SELECT 
		  upper( TRIM( REF_DGN ) )                           AS                                            REF_DGN 
		, upper( TRIM( REF_IDN ) )                           AS                                            REF_IDN 
		, upper( TRIM( REF_DSC ) )                           AS                                            REF_DSC 
		, REF_RID                                            AS                                            REF_RID 
		from SRC_C
            ),
LOGIC_W as ( SELECT 
		  upper( TRIM( CBSA ) )                              AS                                               CBSA 
		, cast( EFFECTIVE_DATE as DATE )                     AS                                     EFFECTIVE_DATE 
		, cast( EXPIRATION_DATE as DATE )                    AS                                    EXPIRATION_DATE 
		, upper( TRIM( ENTRY_USER_ID ) )                     AS                                      ENTRY_USER_ID 
		, cast( ENTRY_DATE as DATE )                         AS                                         ENTRY_DATE 
		, upper( TRIM( ULM ) )                               AS                                                ULM 
		, cast( DLM as DATE )                                AS                                                DLM 
		, WAGE_INDEX                                         AS                                         WAGE_INDEX 
		from SRC_W
            )

---- RENAME LAYER ----
,

RENAME_A as ( SELECT 
		  PRO_RID                                            as                                            PRO_RID
		, EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                                    EXPIRATION_DATE
		, ENTRY_USER_ID                                      as                                      ENTRY_USER_ID
		, ENTRY_DATE                                         as                                         ENTRY_DATE
		, ULM                                                as                                                ULM
		, DLM                                                as                                                DLM
		, QUALITY_IND                                        as                                   OPPS_QUALITY_IND
		, CRITICAL_ACCESS                                    as                                CRITICAL_ACCESS_IND
		, PRO_TYPE                                           as                        MEDICARE_PROVIDER_TYPE_CODE
		, PAY_TO_COST                                        as                                        PAY_TO_COST
		, OUT_CCR                                            as                                            OUT_CCR
		, CBSA                                               as                                 PROVIDER_CBSA_CODE
		, DEV_CCR                                            as                                            DEV_CCR
		, PUB_DEV_CCR                                        as                                        PUB_DEV_CCR 
				FROM     LOGIC_A   ), 
RENAME_B as ( SELECT 
		  PRO_NUM                                            as                                            PRO_NUM
		, PRO_NAME                                           as                                           PRO_NAME
		, DBA_NAME                                           as                                           DBA_NAME
		, DATE_OF_DEATH                                      as                                      DATE_OF_DEATH
		, LAST_TRANS_DATE                                    as                                    LAST_TRANS_DATE
		, DEP_IND                                            as                                            DEP_IND
		, EDI_ID                                             as                                             EDI_ID
		, PRO_TYPE                                           as                                         B_PRO_TYPE
		, PRO_DLM                                            as                                          B_PRO_DLM
		, PRO_ULM                                            as                                          B_PRO_ULM
		, PRO_ID                                             as                                           B_PRO_ID 
				FROM     LOGIC_B   ), 
RENAME_C as ( SELECT 
		  REF_DGN                                            as                                            REF_DGN
		, REF_IDN                                            as                                            REF_IDN
		, REF_DSC                                            as                        MEDICARE_PROVIDER_TYPE_DESC
		, REF_RID                                            as                                            REF_RID 
				FROM     LOGIC_C   ), 
RENAME_W as ( SELECT 
		  CBSA                                               as                                             W_CBSA
		, EFFECTIVE_DATE                                     as                                CBSA_EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                               CBSA_EXPIRATION_DATE
		, ENTRY_USER_ID                                      as                                 CBSA_ENTRY_USER_ID
		, ENTRY_DATE                                         as                                    CBSA_ENTRY_DATE
		, ULM                                                as                                           CBSA_ULM
		, DLM                                                as                                           CBSA_DLM
		, WAGE_INDEX                                         as                                         WAGE_INDEX 
				FROM     LOGIC_W   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_A                              as ( SELECT * from    RENAME_A   ),
FILTER_B                              as ( SELECT * from    RENAME_B   ),
FILTER_C                              as ( SELECT * from    RENAME_C 
				WHERE REF_DGN='MPT'  ),
FILTER_W                              as ( SELECT * from    RENAME_W   ),

---- JOIN LAYER ----

A as ( SELECT * 
				FROM  FILTER_A
				LEFT JOIN FILTER_B ON  FILTER_A.PRO_RID =  FILTER_B.B_PRO_ID 
								LEFT JOIN FILTER_C ON  FILTER_A.MEDICARE_PROVIDER_TYPE_CODE =  FILTER_C.REF_IDN 
								LEFT JOIN FILTER_W ON  FILTER_A.PROVIDER_CBSA_CODE =  FILTER_W.W_CBSA AND YEAR(FILTER_A.EFFECTIVE_DATE) =YEAR(FILTER_W.CBSA_EFFECTIVE_DATE) )
SELECT * 
from A