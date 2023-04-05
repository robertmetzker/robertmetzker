{{ config( 
materialized = 'view' 
) }}

---- SRC LAYER ----
WITH

SRC_CE             as ( SELECT *     FROM     {{ ref( 'DST_CLAIM_ELEMENT' ) }} ),

/*
SRC_CE             as ( SELECT *     FROM     STAGING.DST_CLAIM_ELEMENT ),

*/

---- LOGIC LAYER ----


, LOGIC_CE as ( 
	SELECT 	  
		 UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY	, 
		 OCCR_SRC_TYP_CD                                    as                                    OCCR_SRC_TYP_CD	, 
		 OCCR_SRC_TYP_NM                                    as                                    OCCR_SRC_TYP_NM	, 
		 OCCR_MEDA_TYP_CD                                   as                                   OCCR_MEDA_TYP_CD	, 
		 OCCR_MEDA_TYP_NM                                   as                                   OCCR_MEDA_TYP_NM	, 
		 COMBINED_CLAIM_IND                                 as                                 COMBINED_CLAIM_IND	, 
		 FIREFIGHTER_CANCER_IND                             as                             FIREFIGHTER_CANCER_IND	, 
		 SB229_IND                                          as                                          SB229_IND	, 
		 EMPLOYER_PREMISES_IND                              as                              EMPLOYER_PREMISES_IND	, 
		 EMPLOYER_PAID_PROGRAM_ENROLLMENT_DESC              as              EMPLOYER_PAID_PROGRAM_ENROLLMENT_DESC	, 
		 EMPLOYER_PAID_PROGRAM_TYPE_DESC                    as                    EMPLOYER_PAID_PROGRAM_TYPE_DESC	, 
		 EMPLOYER_PAID_PROGRAM_REASON_CODE                  as                  EMPLOYER_PAID_PROGRAM_REASON_CODE	, 
		 EMPLOYER_PAID_PROGRAM_REASON_DESC                  as                  EMPLOYER_PAID_PROGRAM_REASON_DESC
	 from SRC_CE )


---- RENAME LAYER ----


, RENAME_CE as ( SELECT  
		 UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY , 
		 OCCR_SRC_TYP_CD                                    as                                 FILING_SOURCE_CODE , 
		 OCCR_SRC_TYP_NM                                    as                                 FILING_SOURCE_DESC , 
		 OCCR_MEDA_TYP_CD                                   as                                  FILING_MEDIA_CODE , 
		 OCCR_MEDA_TYP_NM                                   as                                  FILING_MEDIA_DESC , 
		 COMBINED_CLAIM_IND                                 as                                 COMBINED_CLAIM_IND , 
		 FIREFIGHTER_CANCER_IND                             as                             FIREFIGHTER_CANCER_IND , 
		 SB229_IND                                          as                                          SB229_IND , 
		 EMPLOYER_PREMISES_IND                              as                              EMPLOYER_PREMISES_IND , 
		 EMPLOYER_PAID_PROGRAM_ENROLLMENT_DESC              as              EMPLOYER_PAID_PROGRAM_ENROLLMENT_DESC , 
		 EMPLOYER_PAID_PROGRAM_TYPE_DESC                    as                    EMPLOYER_PAID_PROGRAM_TYPE_DESC , 
		 EMPLOYER_PAID_PROGRAM_REASON_CODE                  as                  EMPLOYER_PAID_PROGRAM_REASON_CODE , 
		 EMPLOYER_PAID_PROGRAM_REASON_DESC                  as                  EMPLOYER_PAID_PROGRAM_REASON_DESC 
		FROM LOGIC_CE
            )

---- FILTER LAYER ----

FILTER_CE                             as ( SELECT * FROM    RENAME_CE    )

---- JOIN LAYER ----

 JOIN_CE          as  ( SELECT * 
				FROM  FILTER_CE )
 SELECT * FROM  JOIN_CE