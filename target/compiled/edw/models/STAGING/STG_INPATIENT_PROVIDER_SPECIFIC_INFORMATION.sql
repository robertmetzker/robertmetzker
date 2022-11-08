---- SRC LAYER ----
WITH
SRC_A as ( SELECT *     from     DEV_VIEWS.BASE.PRO_IN_PSF ),
SRC_B as ( SELECT *     from     DEV_VIEWS.BASE.PRO ),
//SRC_A as ( SELECT *     from     PRO_IN_PSF) ,
//SRC_B as ( SELECT *     from     PRO) ,

---- LOGIC LAYER ----

LOGIC_A as ( SELECT 
		  PRO_ID                                             AS                                             PRO_ID 
		, EFFECTIVE_DATE                                     AS                                     EFFECTIVE_DATE 
		, EXPIRATION_DATE                                    AS                                    EXPIRATION_DATE 
		, TRIM( ENTRY_USER_ID )                              AS                                      ENTRY_USER_ID 
		, ENTRY_DATE                                         AS                                         ENTRY_DATE 
		, TRIM( ULM )                                        AS                                                ULM 
		, DLM                                                AS                                                DLM 
		, IN_CCR                                             AS                                             IN_CCR 
		, DGME                                               AS                                               DGME 
		, TRIM( PRO_TYPE )                                   AS                                           PRO_TYPE 
		from SRC_A
            ),
LOGIC_B as ( SELECT 
		  PRO_ID                                             AS                                             PRO_ID 
		, TRIM( PRO_NUM )                                    AS                                            PRO_NUM 
		, TRIM( PRO_NAME )                                   AS                                           PRO_NAME 
		, TRIM( DBA_NAME )                                   AS                                           DBA_NAME 
		, DATE_OF_DEATH                                      AS                                      DATE_OF_DEATH 
		, TRIM( PRO_TYPE )                                   AS                                           PRO_TYPE 
		, LAST_TRANS_DATE                                    AS                                    LAST_TRANS_DATE 
		, TRIM( DEP_IND )                                    AS                                            DEP_IND 
		, PRO_DLM                                            AS                                            PRO_DLM 
		, TRIM( PRO_ULM )                                    AS                                            PRO_ULM 
		, EDI_ID                                             AS                                             EDI_ID 
		from SRC_B
            )

---- RENAME LAYER ----
,

RENAME_A as ( SELECT 
		  PRO_ID                                             as                                             PRO_ID
		, EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                                    EXPIRATION_DATE
		, ENTRY_USER_ID                                      as                                      ENTRY_USER_ID
		, ENTRY_DATE                                         as                                         ENTRY_DATE
		, ULM                                                as                                                ULM
		, DLM                                                as                                                DLM
		, IN_CCR                                             as                                             IN_CCR
		, DGME                                               as                                               DGME
		, PRO_TYPE                                           as                                           PRO_TYPE 
				FROM     LOGIC_A   ), 
RENAME_B as ( SELECT 
		  PRO_ID                                             as                                           B_PRO_ID
		, PRO_NUM                                            as                                            PRO_NUM
		, PRO_NAME                                           as                                           PRO_NAME
		, DBA_NAME                                           as                                           DBA_NAME
		, DATE_OF_DEATH                                      as                                      DATE_OF_DEATH
		, PRO_TYPE                                           as                                         B_PRO_TYPE
		, LAST_TRANS_DATE                                    as                                    LAST_TRANS_DATE
		, DEP_IND                                            as                                            DEP_IND
		, PRO_DLM                                            as                                            PRO_DLM
		, PRO_ULM                                            as                                            PRO_ULM
		, EDI_ID                                             as                                             EDI_ID 
				FROM     LOGIC_B   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_A                              as ( SELECT * from    RENAME_A   ),
FILTER_B                              as ( SELECT * from    RENAME_B   ),

---- JOIN LAYER ----

A as ( SELECT * 
				FROM  FILTER_A
				LEFT JOIN FILTER_B ON  FILTER_A.PRO_ID =  FILTER_B.B_PRO_ID  )
SELECT * 
from A