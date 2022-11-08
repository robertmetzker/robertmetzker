----SRC LAYER----
WITH
SRC_SOS as ( SELECT *     from      DEV_VIEWS.SQL_SERVER_DEP.SIGN_OFF_SHEET )
//SRC_SOS as ( SELECT *     from      SIGN_OFF_SHEET)
----LOGIC LAYER----
,
LOGIC_SOS as ( SELECT 
		cast(PEACHNUMBER as TEXT) AS PEACHNUMBER,
		cast(lpad(PEACHEXTENSION,4,0) as TEXT) AS PEACHEXTENSION,
		UPPER(ENROLLMENT) AS ENROLLMENT,
		UPPER(HPP_APPLICATION) AS HPP_APPLICATION,
		UPPER(PAYMENT_HISTORY) AS PAYMENT_HISTORY,
		UPPER(CURRENT_LICENSE) AS CURRENT_LICENSE,
		UPPER(C92_TRAINING) AS C92_TRAINING,
		UPPER(FILE_REVIEW_TRAINING) AS FILE_REVIEW_TRAINING,
		UPPER(POC_1) AS POC_1,
		UPPER(POC_2) AS POC_2,
		UPPER(POC_3) AS POC_3,
		UPPER(POC_4) AS POC_4,
		UPPER(LIST_SPECIALITY) AS LIST_SPECIALITY,
		UPPER(COMPREHENSIVE_LIABILITY) AS COMPREHENSIVE_LIABILITY,
		UPPER(PROFESSIONAL_LIABILITY) AS PROFESSIONAL_LIABILITY,
		UPPER(BWC_CERTIFICATE) AS BWC_CERTIFICATE,
		UPPER(APPLICATION_SIGNED) AS APPLICATION_SIGNED,
		UPPER(CURRENT_CV) AS CURRENT_CV,
		UPPER(_90_DAY_EXAMS) AS _90_DAY_EXAMS,
		UPPER(IMES) AS IMES,
		UPPER(C_92_EXAMS) AS C_92_EXAMS,
		UPPER(C_92_FILE_REVIEW) AS C_92_FILE_REVIEW,
		UPPER(MEDICAL_FILE_REVIEW) AS MEDICAL_FILE_REVIEW,
		UPPER(DISPUTE_IMES) AS DISPUTE_IMES,
		UPPER(DISPUTE_FILE_REVIEW) AS DISPUTE_FILE_REVIEW,
		UPPER(DMIME) AS DMIME,
		UPPER(ADDITIONAL_LANGUAGE) AS ADDITIONAL_LANGUAGE,
		UPPER(COMMENTS_BY_BONNIE) AS COMMENTS_BY_BONNIE,
		UPPER(COMMENTS_BY_NURSE) AS COMMENTS_BY_NURSE,
		UPPER(COMMENTS_BY_DOCTOR) AS COMMENTS_BY_DOCTOR,
		UPPER(VERIFIED_BY) AS VERIFIED_BY,
		UPPER(DEP_NURSE) AS DEP_NURSE,
		UPPER(MEDICAL_ADVISOR) AS MEDICAL_ADVISOR,
		UPPER(ADDITIONAL_INFORMATION) AS ADDITIONAL_INFORMATION,
		UPPER(ON_LINE_FILE_REVIEWS) AS ON_LINE_FILE_REVIEWS,
		UPPER(LANGUAGE) AS LANGUAGE,
		UPPER(PULMONARY_EXAM) AS PULMONARY_EXAM,
		UPPER(PULMONARY_REVIEW) AS PULMONARY_REVIEW,
		UPPER(B_READER) AS B_READER,
		UPPER(ASBESTOSIS_EXAM) AS ASBESTOSIS_EXAM,
		UPPER(MCO_MEDICAL_DIRECTOR) AS MCO_MEDICAL_DIRECTOR,
		UPPER(MCO) AS MCO,
		UPPER(DUR) AS DUR,
		UPPER(PRIORAUTH) AS PRIORAUTH,
		UPPER(PACKETPWD) AS PACKETPWD,
		UPPER(BWC_PROVIDER_NUMBER) AS BWC_PROVIDER_NUMBER 
				from SRC_SOS
            )
----RENAME LAYER ----
,
RENAME_SOS as ( SELECT 
			
			PEACHNUMBER AS PEACH_BASE_NUMBER,
			
			PEACHEXTENSION AS PEACH_SUFFIX_NUMBER,ENROLLMENT AS ENROLLMENT,HPP_APPLICATION AS HPP_APPLICATION,PAYMENT_HISTORY AS PAYMENT_HISTORY,CURRENT_LICENSE AS CURRENT_LICENSE,C92_TRAINING AS C92_TRAINING,FILE_REVIEW_TRAINING AS FILE_REVIEW_TRAINING,POC_1 AS POC_1,POC_2 AS POC_2,POC_3 AS POC_3,POC_4 AS POC_4,LIST_SPECIALITY AS LIST_SPECIALITY,COMPREHENSIVE_LIABILITY AS COMPREHENSIVE_LIABILITY,PROFESSIONAL_LIABILITY AS PROFESSIONAL_LIABILITY,BWC_CERTIFICATE AS BWC_CERTIFICATE,APPLICATION_SIGNED AS APPLICATION_SIGNED,CURRENT_CV AS CURRENT_CV,_90_DAY_EXAMS AS _90_DAY_EXAMS,IMES AS IMES,C_92_EXAMS AS C_92_EXAMS,C_92_FILE_REVIEW AS C_92_FILE_REVIEW,MEDICAL_FILE_REVIEW AS MEDICAL_FILE_REVIEW,DISPUTE_IMES AS DISPUTE_IMES,DISPUTE_FILE_REVIEW AS DISPUTE_FILE_REVIEW,DMIME AS DMIME,ADDITIONAL_LANGUAGE AS ADDITIONAL_LANGUAGE,COMMENTS_BY_BONNIE AS COMMENTS_BY_BONNIE,COMMENTS_BY_NURSE AS COMMENTS_BY_NURSE,COMMENTS_BY_DOCTOR AS COMMENTS_BY_DOCTOR,VERIFIED_BY AS VERIFIED_BY,DEP_NURSE AS DEP_NURSE,MEDICAL_ADVISOR AS MEDICAL_ADVISOR,ADDITIONAL_INFORMATION AS ADDITIONAL_INFORMATION,ON_LINE_FILE_REVIEWS AS ON_LINE_FILE_REVIEWS,LANGUAGE AS LANGUAGE,PULMONARY_EXAM AS PULMONARY_EXAM,PULMONARY_REVIEW AS PULMONARY_REVIEW,B_READER AS B_READER,ASBESTOSIS_EXAM AS ASBESTOSIS_EXAM,MCO_MEDICAL_DIRECTOR AS MCO_MEDICAL_DIRECTOR,MCO AS MCO,DUR AS DUR,PRIORAUTH AS PRIORAUTH,PACKETPWD AS PACKETPWD,BWC_PROVIDER_NUMBER AS BWC_PROVIDER_NUMBER 
			from      LOGIC_SOS
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_SOS as ( SELECT  * 
			from     RENAME_SOS 
            
        )
----JOIN LAYER----
,
 JOIN_SOS as ( SELECT * 
			from  FILTER_SOS )
 SELECT * FROM JOIN_SOS