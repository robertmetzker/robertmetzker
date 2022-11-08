----SRC LAYER----
WITH
SRC_M as ( SELECT *     from      DEV_VIEWS.SQL_SERVER_DEP.MEMBERS )
//SRC_M as ( SELECT *     from      MEMBERS)
----LOGIC LAYER----
,
LOGIC_M as ( SELECT 
		cast(PEACH_NUMBER as TEXT) AS PEACH_NUMBER,
		cast(lpad(PEACH_EXTENSION,4,0) as TEXT) AS PEACH_EXTENSION,
		UPPER(TRIM(LAST_NAME)) AS LAST_NAME,
		UPPER(TRIM(FIRST_NAME)) AS FIRST_NAME,
		UPPER(TRIM(MI)) AS MI,
		UPPER(TRIM(TITLE)) AS TITLE,
		UPPER(TRIM(PROFESSIONAL_TITLE)) AS PROFESSIONAL_TITLE,
		UPPER(TRIM(CERTIFIED_HPP_PROVIDER)) AS CERTIFIED_HPP_PROVIDER,
		UPPER(TRIM(NAME_OF_PRACTICE)) AS NAME_OF_PRACTICE,
		UPPER(TRIM(PRACTICE_PHONE)) AS PRACTICE_PHONE,
		UPPER(TRIM(PRACTICE_FAX_NUMBER)) AS PRACTICE_FAX_NUMBER,
		UPPER(NULLIF(TRIM(ADMINISTRATIVE_AGENT), '')) AS ADMINISTRATIVE_AGENT,
		cast(ADMINISTRATIVE_PHONE as TEXT) AS ADMINISTRATIVE_PHONE,
		cast(ADMINISTRATIVE_FAX_NUMBER as TEXT) AS ADMINISTRATIVE_FAX_NUMBER,
		UPPER(TRIM(CORRESPONDENCE_INFORMATION)) AS CORRESPONDENCE_INFORMATION,
		UPPER(TRIM(CORRESPONDENCE_PHONE)) AS CORRESPONDENCE_PHONE,
		UPPER(TRIM(CORRESPONDENCE_FAX_NUMBER)) AS CORRESPONDENCE_FAX_NUMBER,
		DATE_ENTERED::DATE AS DATE_ENTERED,
		UPPER(TRIM(CERTIFICATION_OR_DIPLOMATE_1)) AS CERTIFICATION_OR_DIPLOMATE_1,
        CASE WHEN CERTIFICATION_DATE_1 IS NULL THEN try_to_TIMESTAMP('Invalid') ELSE CERTIFICATION_DATE_1 END::DATE AS CERTIFICATION_DATE_1,
		UPPER(TRIM(CERTIFICATION_OR_DIPLOMATE_2)) AS CERTIFICATION_OR_DIPLOMATE_2,
		CASE WHEN CERTIFICATION_DATE_2 IS NULL OR TRIM(CERTIFICATION_DATE_2)=''  THEN try_to_TIMESTAMP('Invalid') ELSE CERTIFICATION_DATE_2 END::DATE AS CERTIFICATION_DATE_2,
		UPPER(TRIM(DISPUTE_RESOLUTION_REVIEWS)) AS DISPUTE_RESOLUTION_REVIEWS,
		UPPER(TRIM(DISPUTE_RESOLUTION_EXAM)) AS DISPUTE_RESOLUTION_EXAM,
		UPPER(TRIM(_90_DAY_EXAM)) AS _90_DAY_EXAM,
		UPPER(TRIM(C_92_REVIEW)) AS C_92_REVIEW,
		UPPER(TRIM(PERMANENT_EXAM_C_92)) AS PERMANENT_EXAM_C_92,
		UPPER(TRIM(INDEPENDENT_IME)) AS INDEPENDENT_IME,
		UPPER(TRIM(MEDICAL_REVIEW_NON_C_92_A)) AS MEDICAL_REVIEW_NON_C_92_A,
		UPPER(TRIM(DEA_1)) AS DEA_1,
		UPPER(TRIM(DEA_1_EXPLAIN)) AS DEA_1_EXPLAIN,
		UPPER(TRIM(DEA_2)) AS DEA_2,
		UPPER(TRIM(DEA_2_EXPLAIN)) AS DEA_2_EXPLAIN,
		UPPER(TRIM(DEA_3)) AS DEA_3,
		UPPER(TRIM(DEA_3_EXPLAIN)) AS DEA_3_EXPLAIN,
		UPPER(TRIM(DEA_4)) AS DEA_4,
		UPPER(TRIM(DEA_4_EXPLAIN)) AS DEA_4_EXPLAIN,
		UPPER(TRIM(DEA_5)) AS DEA_5,
		UPPER(TRIM(DEA_5_EXPLAIN)) AS DEA_5_EXPLAIN,
		UPPER(TRIM(DEA_6)) AS DEA_6,
		UPPER(TRIM(DEA_6_EXPLAIN)) AS DEA_6_EXPLAIN,
		UPPER(TRIM(DEA_7)) AS DEA_7,
		UPPER(TRIM(DEA_7_EXPLAIN)) AS DEA_7_EXPLAIN,
		DEA_8 AS DEA_8,
		DEA_9 AS DEA_9,
		DEA_10_A AS DEA_10_A,
		DEA_10_B AS DEA_10_B,
		DEA_11 AS DEA_11,
		DEA_12_A AS DEA_12_A,
		DEA_12_B AS DEA_12_B,
		DEA_12_C AS DEA_12_C,
		DEA_12_D AS DEA_12_D,
		DEA_12_E AS DEA_12_E,
		DOC_1 AS DOC_1,
		DOC_2 AS DOC_2,
		DOC_3 AS DOC_3,
		DOC_4 AS DOC_4,
		PRIMARY AS PRIMARY,
		ADMIN AS ADMIN,
		SHIPPING AS SHIPPING,
		MAIL AS MAIL,
		DO_NOT_DISPLAY AS DO_NOT_DISPLAY,
		UPPER(TRIM(EMAIL_ADDRESS)) AS EMAIL_ADDRESS,
		cast(TERMINATION_DATE as DATE) AS TERMINATION_DATE,
		cast(ACTIVE_DATE as DATE) AS ACTIVE_DATE,
		cast(PROCESSING_DATE as DATE) AS PROCESSING_DATE,
		PROCESSING_DAYS AS PROCESSING_DAYS,
		INACTIVE AS INACTIVE,
		ADMIN_HOLD AS ADMIN_HOLD,
		EXAM_PACKET AS EXAM_PACKET,
		SERVICE_OFFICE_NAME AS SERVICE_OFFICE_NAME,
		TRIM(SOCIAL_SECURITY_NUMBER) AS SOCIAL_SECURITY_NUMBER,
		TRIM(TAX_IDENTIFICATION_NUMBER) AS TAX_IDENTIFICATION_NUMBER,
		TRIM(BWC_PROVIDER_NUMBER) AS BWC_PROVIDER_NUMBER 
				from SRC_M
            )
----RENAME LAYER ----
,
RENAME_M as ( SELECT 
			
			PEACH_NUMBER AS PEACH_BASE_NUMBER,
			
			PEACH_EXTENSION AS PEACH_SUFFIX_NUMBER,LAST_NAME AS LAST_NAME,FIRST_NAME AS FIRST_NAME,MI AS MI,TITLE AS TITLE,PROFESSIONAL_TITLE AS PROFESSIONAL_TITLE,CERTIFIED_HPP_PROVIDER AS CERTIFIED_HPP_PROVIDER,NAME_OF_PRACTICE AS NAME_OF_PRACTICE,PRACTICE_PHONE AS PRACTICE_PHONE,PRACTICE_FAX_NUMBER AS PRACTICE_FAX_NUMBER,ADMINISTRATIVE_AGENT AS ADMINISTRATIVE_AGENT,ADMINISTRATIVE_PHONE AS ADMINISTRATIVE_PHONE,ADMINISTRATIVE_FAX_NUMBER AS ADMINISTRATIVE_FAX_NUMBER,CORRESPONDENCE_INFORMATION AS CORRESPONDENCE_INFORMATION,CORRESPONDENCE_PHONE AS CORRESPONDENCE_PHONE,CORRESPONDENCE_FAX_NUMBER AS CORRESPONDENCE_FAX_NUMBER,DATE_ENTERED AS DATE_ENTERED,CERTIFICATION_OR_DIPLOMATE_1 AS CERTIFICATION_OR_DIPLOMATE_1,CERTIFICATION_DATE_1 AS CERTIFICATION_DATE_1,CERTIFICATION_OR_DIPLOMATE_2 AS CERTIFICATION_OR_DIPLOMATE_2,CERTIFICATION_DATE_2 AS CERTIFICATION_DATE_2,DISPUTE_RESOLUTION_REVIEWS AS DISPUTE_RESOLUTION_REVIEWS,DISPUTE_RESOLUTION_EXAM AS DISPUTE_RESOLUTION_EXAM,_90_DAY_EXAM AS _90_DAY_EXAM,C_92_REVIEW AS C_92_REVIEW,PERMANENT_EXAM_C_92 AS PERMANENT_EXAM_C_92,INDEPENDENT_IME AS INDEPENDENT_IME,MEDICAL_REVIEW_NON_C_92_A AS MEDICAL_REVIEW_NON_C_92_A,DEA_1 AS DEA_1,DEA_1_EXPLAIN AS DEA_1_EXPLAIN,DEA_2 AS DEA_2,DEA_2_EXPLAIN AS DEA_2_EXPLAIN,DEA_3 AS DEA_3,DEA_3_EXPLAIN AS DEA_3_EXPLAIN,DEA_4 AS DEA_4,DEA_4_EXPLAIN AS DEA_4_EXPLAIN,DEA_5 AS DEA_5,DEA_5_EXPLAIN AS DEA_5_EXPLAIN,DEA_6 AS DEA_6,DEA_6_EXPLAIN AS DEA_6_EXPLAIN,DEA_7 AS DEA_7,DEA_7_EXPLAIN AS DEA_7_EXPLAIN,DEA_8 AS DEA_8,DEA_9 AS DEA_9,DEA_10_A AS DEA_10_A,DEA_10_B AS DEA_10_B,DEA_11 AS DEA_11,DEA_12_A AS DEA_12_A,DEA_12_B AS DEA_12_B,DEA_12_C AS DEA_12_C,DEA_12_D AS DEA_12_D,DEA_12_E AS DEA_12_E,DOC_1 AS DOC_1,DOC_2 AS DOC_2,DOC_3 AS DOC_3,DOC_4 AS DOC_4,PRIMARY AS PRIMARY,ADMIN AS ADMIN,SHIPPING AS SHIPPING,MAIL AS MAIL,DO_NOT_DISPLAY AS DO_NOT_DISPLAY,EMAIL_ADDRESS AS EMAIL_ADDRESS,TERMINATION_DATE AS TERMINATION_DATE,ACTIVE_DATE AS ACTIVE_DATE,PROCESSING_DATE AS PROCESSING_DATE,PROCESSING_DAYS AS PROCESSING_DAYS,INACTIVE AS INACTIVE,ADMIN_HOLD AS ADMIN_HOLD,EXAM_PACKET AS EXAM_PACKET,SERVICE_OFFICE_NAME AS SERVICE_OFFICE_NAME,SOCIAL_SECURITY_NUMBER AS SOCIAL_SECURITY_NUMBER,TAX_IDENTIFICATION_NUMBER AS TAX_IDENTIFICATION_NUMBER,BWC_PROVIDER_NUMBER AS BWC_PROVIDER_NUMBER 
			from      LOGIC_M
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_M as ( SELECT  * 
			from     RENAME_M 
            
        )
----JOIN LAYER----
,
 JOIN_M as ( SELECT * 
			from  FILTER_M )
 SELECT * FROM JOIN_M