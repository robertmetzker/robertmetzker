----SRC LAYER----
WITH
FINAL as ( SELECT DISTINCT
 md5(cast(
    
    coalesce(cast(FILING_SOURCE_DESC as 
    varchar
), '') || '-' || coalesce(cast(FILING_MEDIA_DESC as 
    varchar
), '') || '-' || coalesce(cast(NATURE_OF_INJURY_CATEGORY as 
    varchar
), '') || '-' || coalesce(cast(NATURE_OF_INJURY_TYPE as 
    varchar
), '') || '-' || coalesce(cast(FIREFIGHTER_CANCER_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_EXPOSURE_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_EMERGENCY_WORKER_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_HEALTH_CARE_WORKER_IND as 
    varchar
), '') || '-' || coalesce(cast(COMBINED_IND as 
    varchar
), '') || '-' || coalesce(cast(SB223_IND as 
    varchar
), '') || '-' || coalesce(cast(EMPLOYER_PREMISES_IND as 
    varchar
), '') || '-' || coalesce(cast(CATASTROPHIC_IND as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_ENROLLMENT_DESC as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_REASON_DESC as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY 
, FILING_SOURCE_DESC
, FILING_MEDIA_DESC
, NATURE_OF_INJURY_CATEGORY
, NATURE_OF_INJURY_TYPE
, FIREFIGHTER_CANCER_IND
, COVID_EXPOSURE_IND
, COVID_EMERGENCY_WORKER_IND
, COVID_HEALTH_CARE_WORKER_IND
, COMBINED_IND
, SB223_IND
, EMPLOYER_PREMISES_IND
, CATASTROPHIC_IND
, K_PROGRAM_ENROLLMENT_DESC
, K_PROGRAM_TYPE_DESC
, K_PROGRAM_REASON_DESC
   	from      STAGING.DSV_CLAIM
 union
	 SELECT DISTINCT
	  md5(cast(
    
    coalesce(cast(FILING_SOURCE_DESC as 
    varchar
), '') || '-' || coalesce(cast(FILING_MEDIA_DESC as 
    varchar
), '') || '-' || coalesce(cast(NATURE_OF_INJURY_CATEGORY as 
    varchar
), '') || '-' || coalesce(cast(NATURE_OF_INJURY_TYPE as 
    varchar
), '') || '-' || coalesce(cast(FIREFIGHTER_CANCER_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_EXPOSURE_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_EMERGENCY_WORKER_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_HEALTH_CARE_WORKER_IND as 
    varchar
), '') || '-' || coalesce(cast(COMBINED_IND as 
    varchar
), '') || '-' || coalesce(cast(SB223_IND as 
    varchar
), '') || '-' || coalesce(cast(EMPLOYER_PREMISES_IND as 
    varchar
), '') || '-' || coalesce(cast(CATASTROPHIC_IND as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_ENROLLMENT_DESC as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_REASON_DESC as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY 
, FILING_SOURCE_DESC
, FILING_MEDIA_DESC
, NATURE_OF_INJURY_CATEGORY
, NATURE_OF_INJURY_TYPE
, FIREFIGHTER_CANCER_IND
, COVID_EXPOSURE_IND
, COVID_EMERGENCY_WORKER_IND
, COVID_HEALTH_CARE_WORKER_IND
, COMBINED_IND
, SB223_IND
, EMPLOYER_PREMISES_IND
, CATASTROPHIC_IND
, K_PROGRAM_ENROLLMENT_DESC
, K_PROGRAM_TYPE_DESC
, K_PROGRAM_REASON_DESC
   	from      STAGING.DSV_CLAIM_ACTIVITY)
select * from FINAL