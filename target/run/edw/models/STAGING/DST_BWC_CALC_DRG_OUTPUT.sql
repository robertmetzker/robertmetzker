

      create or replace  table DEV_EDW.STAGING.DST_BWC_CALC_DRG_OUTPUT  as
      (----SRC LAYER----
WITH
SRC_INV as ( SELECT *     from      STAGING.STG_INVOICE_DRG ),
SRC_G1 as ( SELECT *     from      STAGING.STG_TDDICDN ),
SRC_G2 as ( SELECT *     from      STAGING.STG_TDDICDN),
SRC_G3 as ( SELECT *     from      STAGING.STG_TDDICDN),
SRC_G4 as ( SELECT *     from      STAGING.STG_TDDICDN ),
SRC_G5 as ( SELECT *     from      STAGING.STG_TDDICDN),
SRC_G6 as ( SELECT *     from      STAGING.STG_ICD_PROCEDURE),
SRC_G7 as ( SELECT *     from      STAGING.STG_ICD_PROCEDURE),
SRC_G8 as ( SELECT *     from      STAGING.STG_ICD_PROCEDURE),
SRC_G9 as ( SELECT *     from      STAGING.STG_ICD_PROCEDURE),
SRC_G10 as ( SELECT *     from      STAGING.STG_ICD_PROCEDURE ),
SRC_G11 as ( SELECT *     from      STAGING.STG_ICD_PROCEDURE),
SRC_G12 as ( SELECT *     from      STAGING.STG_ICD_PROCEDURE),
SRC_G13 as ( SELECT *     from      STAGING.STG_ICD_PROCEDURE),
SRC_G14 as ( SELECT *     from      STAGING.STG_ICD_PROCEDURE),
SRC_G15 as ( SELECT *     from      STAGING.STG_ICD_PROCEDURE)
----LOGIC LAYER----
,
LOGIC_INV as ( SELECT 
		--- NEW UNIQUE_ID_KEY AS [DERIVED],
		md5(cast(
    
    coalesce(cast(INVOICE_NUMBER as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY,
		TRIM(INVOICE_NUMBER) AS INVOICE_NUMBER,
		DATE_SENT AS DATE_SENT,
		TRIM(DRG_NUMBER) AS DRG_NUMBER,
		TRIM(DRG_TITLE) AS DRG_TITLE,
		TRIM(MAJOR_TITLE) AS MAJOR_TITLE,
		TRIM(MAJOR_CATEGORY) AS MAJOR_CATEGORY,
		TRIM(DIAG_1) AS DIAG_1,
		TRIM(DIAG_2) AS DIAG_2,
		TRIM(DIAG_3) AS DIAG_3,
		TRIM(DIAG_4) AS DIAG_4,
		TRIM(DIAG_5) AS DIAG_5,
		TRIM(OR_PROC_1) AS OR_PROC_1,
		TRIM(OR_PROC_2) AS OR_PROC_2,
		TRIM(OR_PROC_3) AS OR_PROC_3,
		TRIM(OR_PROC_4) AS OR_PROC_4,
		TRIM(OR_PROC_5) AS OR_PROC_5,
		TRIM(NOR_PROC_1) AS NOR_PROC_1,
		TRIM(NOR_PROC_2) AS NOR_PROC_2,
		TRIM(NOR_PROC_3) AS NOR_PROC_3,
		TRIM(NOR_PROC_4) AS NOR_PROC_4,
		TRIM(NOR_PROC_5) AS NOR_PROC_5,
		TRIM(RETURN_CODE) AS RETURN_CODE,
		TRIM(RETURN_MSG) AS RETURN_MSG,
		TRIM(RETURN_EXT) AS RETURN_EXT,
		TRIM(GRP_VERSION) AS GRP_VERSION,
		TRIM(GRP_METHOD) AS GRP_METHOD,
		TRIM(PRC_VERSION) AS PRC_VERSION 
				from SRC_INV
            ),
LOGIC_G1 as ( SELECT 
		TRIM(ICD_CODE_LONG_DESC) AS ICD_CODE_LONG_DESC,
		ICDN_VRSN_END_DATE AS ICDN_VRSN_END_DATE,
		TRIM(ICDN_CRNT_MAX_FLAG) AS ICDN_CRNT_MAX_FLAG,
		ICDN_ENDNG_DATE AS ICDN_ENDNG_DATE,
		TRIM(ICD_CODE) AS ICD_CODE,
		TRIM(ICDV_CODE) AS ICDV_CODE 
				from SRC_G1
            ),
LOGIC_G2 as ( SELECT 
		TRIM(ICD_CODE_LONG_DESC) AS ICD_CODE_LONG_DESC,
		ICDN_VRSN_END_DATE AS ICDN_VRSN_END_DATE,
		TRIM(ICDN_CRNT_MAX_FLAG) AS ICDN_CRNT_MAX_FLAG,
		ICDN_ENDNG_DATE AS ICDN_ENDNG_DATE,
		TRIM(ICD_CODE) AS ICD_CODE,
		TRIM(ICDV_CODE) AS ICDV_CODE 
				from SRC_G2
            ),
LOGIC_G3 as ( SELECT 
		TRIM(ICD_CODE_LONG_DESC) AS ICD_CODE_LONG_DESC,
		ICDN_VRSN_END_DATE AS ICDN_VRSN_END_DATE,
		TRIM(ICDN_CRNT_MAX_FLAG) AS ICDN_CRNT_MAX_FLAG,
		ICDN_ENDNG_DATE AS ICDN_ENDNG_DATE,
		TRIM(ICD_CODE) AS ICD_CODE,
		TRIM(ICDV_CODE) AS ICDV_CODE 
				from SRC_G3
            ),
LOGIC_G4 as ( SELECT 
		TRIM(ICD_CODE_LONG_DESC) AS ICD_CODE_LONG_DESC,
		ICDN_VRSN_END_DATE AS ICDN_VRSN_END_DATE,
		TRIM(ICDN_CRNT_MAX_FLAG) AS ICDN_CRNT_MAX_FLAG,
		ICDN_ENDNG_DATE AS ICDN_ENDNG_DATE,
		TRIM(ICD_CODE) AS ICD_CODE,
		TRIM(ICDV_CODE) AS ICDV_CODE 
				from SRC_G4
            ),
LOGIC_G5 as ( SELECT 
		TRIM(ICD_CODE_LONG_DESC) AS ICD_CODE_LONG_DESC,
        ICDN_VRSN_END_DATE AS ICDN_VRSN_END_DATE,
		TRIM(ICDN_CRNT_MAX_FLAG) AS ICDN_CRNT_MAX_FLAG,
		ICDN_ENDNG_DATE AS ICDN_ENDNG_DATE,
		TRIM(ICD_CODE) AS ICD_CODE,
		TRIM(ICDV_CODE) AS ICDV_CODE 
				from SRC_G5
            ),
LOGIC_G6 as ( SELECT 
		TRIM(DESCRIPTION) AS DESCRIPTION,
		EFFECTIVE_DATE AS EFFECTIVE_DATE,
		EXPIRATION_DATE AS EXPIRATION_DATE,
		TRIM(CODE) AS CODE,
		TRIM(VERSION) AS VERSION 
				from SRC_G6
            ),
LOGIC_G7 as ( SELECT 
		TRIM(DESCRIPTION) AS DESCRIPTION,
		EFFECTIVE_DATE AS EFFECTIVE_DATE,
		EXPIRATION_DATE AS EXPIRATION_DATE,
		TRIM(CODE) AS CODE,
		TRIM(VERSION) AS VERSION 
				from SRC_G7
            ),
LOGIC_G8 as ( SELECT 
		TRIM(DESCRIPTION) AS DESCRIPTION,
		EFFECTIVE_DATE AS EFFECTIVE_DATE,
		EXPIRATION_DATE AS EXPIRATION_DATE,
		TRIM(CODE) AS CODE,
		TRIM(VERSION) AS VERSION 
				from SRC_G8
            ),
LOGIC_G9 as ( SELECT 
		TRIM(DESCRIPTION) AS DESCRIPTION,
		EFFECTIVE_DATE AS EFFECTIVE_DATE,
		EXPIRATION_DATE AS EXPIRATION_DATE,
		TRIM(CODE) AS CODE,
		TRIM(VERSION) AS VERSION 
				from SRC_G9
            ),
LOGIC_G10 as ( SELECT 
		TRIM(DESCRIPTION) AS DESCRIPTION,
		EFFECTIVE_DATE AS EFFECTIVE_DATE,
		EXPIRATION_DATE AS EXPIRATION_DATE,
		TRIM(CODE) AS CODE,
		TRIM(VERSION) AS VERSION 
				from SRC_G10
            ),
LOGIC_G11 as ( SELECT 
		TRIM(DESCRIPTION) AS DESCRIPTION,
		EFFECTIVE_DATE AS EFFECTIVE_DATE,
		EXPIRATION_DATE AS EXPIRATION_DATE,
		TRIM(CODE) AS CODE,
		TRIM(VERSION) AS VERSION 
				from SRC_G11
            ),
LOGIC_G12 as ( SELECT 
		TRIM(DESCRIPTION) AS DESCRIPTION,
		EFFECTIVE_DATE AS EFFECTIVE_DATE,
		EXPIRATION_DATE AS EXPIRATION_DATE,
		TRIM(CODE) AS CODE,
		TRIM(VERSION) AS VERSION 
				from SRC_G12
            ),
LOGIC_G13 as ( SELECT 
		TRIM(DESCRIPTION) AS DESCRIPTION,
		EFFECTIVE_DATE AS EFFECTIVE_DATE,
		EXPIRATION_DATE AS EXPIRATION_DATE,
		TRIM(CODE) AS CODE,
		TRIM(VERSION) AS VERSION 
				from SRC_G13
            ),
LOGIC_G14 as ( SELECT 
		TRIM(DESCRIPTION) AS DESCRIPTION,
		EFFECTIVE_DATE AS EFFECTIVE_DATE,
		EXPIRATION_DATE AS EXPIRATION_DATE,
		TRIM(CODE) AS CODE,
		TRIM(VERSION) AS VERSION 
				from SRC_G14
            ),
LOGIC_G15 as ( SELECT 
		TRIM(DESCRIPTION) AS DESCRIPTION,
		EFFECTIVE_DATE AS EFFECTIVE_DATE,
		EXPIRATION_DATE AS EXPIRATION_DATE,
		TRIM(CODE) AS CODE,
		TRIM(VERSION) AS VERSION 
				from SRC_G15
            )
----RENAME LAYER ----
,
RENAME_INV as ( SELECT 
			
			UNIQUE_ID_KEY AS UNIQUE_ID_KEY,INVOICE_NUMBER AS INVOICE_NUMBER,DATE_SENT AS DATE_SENT,DRG_NUMBER AS DRG_NUMBER,DRG_TITLE AS DRG_TITLE
			,MAJOR_TITLE AS MAJOR_TITLE,MAJOR_CATEGORY AS MAJOR_CATEGORY,DIAG_1 AS DIAG_1,DIAG_2 AS DIAG_2,DIAG_3 AS DIAG_3,DIAG_4 AS DIAG_4,DIAG_5 AS DIAG_5
			,OR_PROC_1 AS OR_PROC_1,OR_PROC_2 AS OR_PROC_2,OR_PROC_3 AS OR_PROC_3,OR_PROC_4 AS OR_PROC_4,OR_PROC_5 AS OR_PROC_5,NOR_PROC_1 AS NOR_PROC_1
			,NOR_PROC_2 AS NOR_PROC_2,NOR_PROC_3 AS NOR_PROC_3,NOR_PROC_4 AS NOR_PROC_4,NOR_PROC_5 AS NOR_PROC_5,RETURN_CODE AS RETURN_CODE,RETURN_MSG AS RETURN_MSG
			,RETURN_EXT AS RETURN_EXT,GRP_VERSION AS GRP_VERSION,GRP_METHOD AS GRP_METHOD,PRC_VERSION AS PRC_VERSION 
			from      LOGIC_INV
        ),
RENAME_G1 as ( SELECT ICD_CODE_LONG_DESC AS DIAG_1_DESC,
			
			ICDN_VRSN_END_DATE AS G1_ICDN_VRSN_END_DATE,
			
			ICDN_CRNT_MAX_FLAG AS G1_ICDN_CRNT_MAX_FLAG,
			
			ICDN_ENDNG_DATE AS G1_ICDN_ENDNG_DATE,
			
			ICD_CODE AS G1_ICDN_ICDC_CODE,
			
			ICDV_CODE AS G1_ICDV_CODE 
			from      LOGIC_G1
        ),
RENAME_G2 as ( SELECT ICD_CODE_LONG_DESC AS DIAG_2_DESC,
			
			ICDN_VRSN_END_DATE AS G2_ICDN_VRSN_END_DATE,
			
			ICDN_CRNT_MAX_FLAG AS G2_ICDN_CRNT_MAX_FLAG,
			
			ICDN_ENDNG_DATE AS G2_ICDN_ENDNG_DATE,
			
			ICD_CODE AS G2_ICDN_ICDC_CODE,
			
			ICDV_CODE AS G2_ICDV_CODE 
			from      LOGIC_G2
        ),
RENAME_G3 as ( SELECT ICD_CODE_LONG_DESC AS DIAG_3_DESC,
			
			ICDN_VRSN_END_DATE AS G3_ICDN_VRSN_END_DATE,
			
			ICDN_CRNT_MAX_FLAG AS G3_ICDN_CRNT_MAX_FLAG,
			
			ICDN_ENDNG_DATE AS G3_ICDN_ENDNG_DATE,
			
			ICD_CODE AS G3_ICDN_ICDC_CODE,
			
			ICDV_CODE AS G3_ICDV_CODE 
			from      LOGIC_G3
        ),
RENAME_G4 as ( SELECT ICD_CODE_LONG_DESC AS DIAG_4_DESC,
			
			ICDN_VRSN_END_DATE AS G4_ICDN_VRSN_END_DATE,
			
			ICDN_CRNT_MAX_FLAG AS G4_ICDN_CRNT_MAX_FLAG,
			
			ICDN_ENDNG_DATE AS G4_ICDN_ENDNG_DATE,
			
			ICD_CODE AS G4_ICDN_ICDC_CODE,
			
			ICDV_CODE AS G4_ICDV_CODE 
			from      LOGIC_G4
        ),
RENAME_G5 as ( SELECT ICD_CODE_LONG_DESC AS DIAG_5_DESC,
                    ICDN_VRSN_END_DATE AS G5_ICDN_VRSN_END_DATE,
			
			ICDN_CRNT_MAX_FLAG AS G5_ICDN_CRNT_MAX_FLAG,
			
			ICDN_ENDNG_DATE AS G5_ICDN_ENDNG_DATE,
			
			ICD_CODE AS G5_ICDN_ICDC_CODE,
			
			ICDV_CODE AS G5_ICDV_CODE 
			from      LOGIC_G5
        ),
RENAME_G6 as ( SELECT DESCRIPTION AS OR_PROC_1_DESC,
			
			EFFECTIVE_DATE AS G6_EFFECTIVE_DATE,
			
			EXPIRATION_DATE AS G6_EXPIRATION_DATE,
			
			CODE AS G6_CODE,
			
			VERSION AS G6_VERSION 
			from      LOGIC_G6
        ),
RENAME_G7 as ( SELECT DESCRIPTION AS OR_PROC_2_DESC,
			
			EFFECTIVE_DATE AS G7_EFFECTIVE_DATE,
			
			EXPIRATION_DATE AS G7_EXPIRATION_DATE,
			
			CODE AS G7_CODE,
			
			VERSION AS G7_VERSION 
			from      LOGIC_G7
        ),
RENAME_G8 as ( SELECT DESCRIPTION AS OR_PROC_3_DESC,
			
			EFFECTIVE_DATE AS G8_EFFECTIVE_DATE,
			
			EXPIRATION_DATE AS G8_EXPIRATION_DATE,
			
			CODE AS G8_CODE,
			
			VERSION AS G8_VERSION 
			from      LOGIC_G8
        ),
RENAME_G9 as ( SELECT DESCRIPTION AS OR_PROC_4_DESC,
			
			EFFECTIVE_DATE AS G9_EFFECTIVE_DATE,
			
			EXPIRATION_DATE AS G9_EXPIRATION_DATE,
			
			CODE AS G9_CODE,
			
			VERSION AS G9_VERSION 
			from      LOGIC_G9
        ),
RENAME_G10 as ( SELECT DESCRIPTION AS OR_PROC_5_DESC,
			
			EFFECTIVE_DATE AS G10_EFFECTIVE_DATE,
			
			EXPIRATION_DATE AS G10_EXPIRATION_DATE,
			
			CODE AS G10_CODE,
			
			VERSION AS G10_VERSION 
			from      LOGIC_G10
        ),
RENAME_G11 as ( SELECT DESCRIPTION AS NOR_PROC_1_DESC,
			
			EFFECTIVE_DATE AS G11_EFFECTIVE_DATE,
			
			EXPIRATION_DATE AS G11_EXPIRATION_DATE,
			
			CODE AS G11_CODE,
			
			VERSION AS G11_VERSION 
			from      LOGIC_G11
        ),
RENAME_G12 as ( SELECT DESCRIPTION AS NOR_PROC_2_DESC,
			
			EFFECTIVE_DATE AS G12_EFFECTIVE_DATE,
			
			EXPIRATION_DATE AS G12_EXPIRATION_DATE,
			
			CODE AS G12_CODE,
			
			VERSION AS G12_VERSION 
			from      LOGIC_G12
        ),
RENAME_G13 as ( SELECT DESCRIPTION AS NOR_PROC_3_DESC,
			
			EFFECTIVE_DATE AS G13_EFFECTIVE_DATE,
			
			EXPIRATION_DATE AS G13_EXPIRATION_DATE,
			
			CODE AS G13_CODE,
			
			VERSION AS G13_VERSION 
			from      LOGIC_G13
        ),
RENAME_G14 as ( SELECT DESCRIPTION AS NOR_PROC_4_DESC,
			
			EFFECTIVE_DATE AS G14_EFFECTIVE_DATE,
			
			EXPIRATION_DATE AS G14_EXPIRATION_DATE,
			
			CODE AS G14_CODE,
			
			VERSION AS G14_VERSION 
			from      LOGIC_G14
        ),
RENAME_G15 as ( SELECT DESCRIPTION AS NOR_PROC_5_DESC,
			
			EFFECTIVE_DATE AS G15_EFFECTIVE_DATE,
			
			EXPIRATION_DATE AS G15_EXPIRATION_DATE,
			
			CODE AS G15_CODE,
			
			VERSION AS G15_VERSION 
			from      LOGIC_G15
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_INV as ( SELECT  * 
			from     RENAME_INV 
            
        ),

        FILTER_G1 as ( SELECT  * 
			from     RENAME_G1 
            WHERE G1_ICDN_VRSN_END_DATE > CURRENT_DATE AND (trim(G1_ICDN_CRNT_MAX_FLAG) = 'Y' OR G1_ICDN_ENDNG_DATE > CURRENT_DATE) 			
			QUALIFY (ROW_NUMBER() OVER (PARTITION BY G1_ICDN_ICDC_CODE ORDER BY G1_ICDV_CODE)) =1
        ),

        FILTER_G2 as ( SELECT  * 
			from     RENAME_G2 
            WHERE G2_ICDN_VRSN_END_DATE > CURRENT_DATE AND (trim(G2_ICDN_CRNT_MAX_FLAG) = 'Y' OR G2_ICDN_ENDNG_DATE > CURRENT_DATE) 			
			QUALIFY (ROW_NUMBER() OVER (PARTITION BY G2_ICDN_ICDC_CODE ORDER BY G2_ICDV_CODE)) =1
        ),

        FILTER_G3 as ( SELECT  * 
			from     RENAME_G3 
            WHERE G3_ICDN_VRSN_END_DATE > CURRENT_DATE AND (trim(G3_ICDN_CRNT_MAX_FLAG) = 'Y' OR G3_ICDN_ENDNG_DATE > CURRENT_DATE) 			
			QUALIFY (ROW_NUMBER() OVER (PARTITION BY G3_ICDN_ICDC_CODE ORDER BY G3_ICDV_CODE)) =1
        ),

        FILTER_G4 as ( SELECT  * 
			from     RENAME_G4 
            WHERE G4_ICDN_VRSN_END_DATE > CURRENT_DATE AND (trim(G4_ICDN_CRNT_MAX_FLAG) = 'Y' OR G4_ICDN_ENDNG_DATE > CURRENT_DATE) 			
			QUALIFY (ROW_NUMBER() OVER (PARTITION BY G4_ICDN_ICDC_CODE ORDER BY G4_ICDV_CODE)) =1
        ),

        FILTER_G5 as ( SELECT  * 
			from     RENAME_G5 
            WHERE G5_ICDN_VRSN_END_DATE > CURRENT_DATE AND (trim(G5_ICDN_CRNT_MAX_FLAG) = 'Y' OR G5_ICDN_ENDNG_DATE > CURRENT_DATE) 			
			QUALIFY (ROW_NUMBER() OVER (PARTITION BY G5_ICDN_ICDC_CODE ORDER BY G5_ICDV_CODE)) =1
        ),

        FILTER_G6 as ( SELECT  * 
			from     RENAME_G6 
            WHERE G6_EXPIRATION_DATE > CURRENT_DATE 
        ),

        FILTER_G7 as ( SELECT  * 
			from     RENAME_G7 
            WHERE G7_EXPIRATION_DATE > CURRENT_DATE 
        ),

        FILTER_G8 as ( SELECT  * 
			from     RENAME_G8 
            WHERE G8_EXPIRATION_DATE > CURRENT_DATE
        ),

        FILTER_G9 as ( SELECT  * 
			from     RENAME_G9 
            WHERE G9_EXPIRATION_DATE > CURRENT_DATE
        ),

        FILTER_G10 as ( SELECT  * 
			from     RENAME_G10 
            WHERE G10_EXPIRATION_DATE > CURRENT_DATE
        ),

        FILTER_G11 as ( SELECT  * 
			from     RENAME_G11 
            WHERE G11_EXPIRATION_DATE > CURRENT_DATE
        ),

        FILTER_G12 as ( SELECT  * 
			from     RENAME_G12 
            WHERE G12_EXPIRATION_DATE > CURRENT_DATE
        ),

        FILTER_G13 as ( SELECT  * 
			from     RENAME_G13 
            WHERE G13_EXPIRATION_DATE > CURRENT_DATE
        ),

        FILTER_G14 as ( SELECT  * 
			from     RENAME_G14 
            WHERE G14_EXPIRATION_DATE > CURRENT_DATE
        ),

        FILTER_G15 as ( SELECT  * 
			from     RENAME_G15 
            WHERE G15_EXPIRATION_DATE > CURRENT_DATE
        )
----JOIN LAYER----
,
INV as ( SELECT * 
			from  FILTER_INV
				LEFT JOIN FILTER_G1 ON FILTER_INV.DIAG_1 = FILTER_g1.g1_ICDN_ICDC_CODE 
				LEFT JOIN FILTER_G2 ON FILTER_INV.DIAG_2 = FILTER_g2.g2_ICDN_ICDC_CODE 
				LEFT JOIN FILTER_G3 ON FILTER_INV.DIAG_3 = FILTER_g3.g3_ICDN_ICDC_CODE 
				LEFT JOIN FILTER_G4 ON FILTER_INV.DIAG_4 = FILTER_g4.g4_ICDN_ICDC_CODE 
				LEFT JOIN FILTER_G5 ON FILTER_INV.DIAG_5 = FILTER_g5.g5_ICDN_ICDC_CODE 
				LEFT JOIN FILTER_G6 ON FILTER_INV.OR_PROC_1 = FILTER_g6.g6_CODE 
				LEFT JOIN FILTER_G7 ON FILTER_INV.OR_PROC_2 = FILTER_g7.g7_CODE 
				LEFT JOIN FILTER_G8 ON FILTER_INV.OR_PROC_3 = FILTER_g8.g8_CODE 
				LEFT JOIN FILTER_G9 ON FILTER_INV.OR_PROC_4 = FILTER_g9.g9_CODE 
				LEFT JOIN FILTER_G10 ON FILTER_INV.OR_PROC_5 = FILTER_g10.g10_CODE 
				LEFT JOIN FILTER_G11 ON FILTER_INV.NOR_PROC_1 = FILTER_g11.g11_CODE 
				LEFT JOIN FILTER_G12 ON FILTER_INV.NOR_PROC_2 = FILTER_g12.g12_CODE 
				LEFT JOIN FILTER_G13 ON FILTER_INV.NOR_PROC_3 = FILTER_g13.g13_CODE 
				LEFT JOIN FILTER_G14 ON FILTER_INV.NOR_PROC_4 = FILTER_g14.g14_CODE 
				LEFT JOIN FILTER_G15 ON FILTER_INV.NOR_PROC_5 = FILTER_g15.g15_CODE  )
select * from INV
      );
    