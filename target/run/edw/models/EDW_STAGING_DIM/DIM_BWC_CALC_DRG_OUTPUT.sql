

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_BWC_CALC_DRG_OUTPUT  as
      ( 


 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
          last_value(MEDICAL_INVOICE_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MEDICAL_INVOICE_NUMBER,
     last_value(DATE_SENT) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DATE_SENT,
     last_value(DIAGNOSIS_RELATED_GROUP_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIAGNOSIS_RELATED_GROUP_CODE,
     last_value(DIAGNOSIS_RELATED_GROUP_TITLE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIAGNOSIS_RELATED_GROUP_TITLE,
     last_value(MAJOR_TITLE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAJOR_TITLE,
     last_value(MAJOR_CATEGORY) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MAJOR_CATEGORY,
     last_value(DIAGNOSIS_CODE_1) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIAGNOSIS_CODE_1,
     last_value(DIAGNOSIS_DESC_1) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIAGNOSIS_DESC_1,
     last_value(DIAGNOSIS_CODE_2) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIAGNOSIS_CODE_2,
     last_value(DIAGNOSIS_DESC_2) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIAGNOSIS_DESC_2,
     last_value(DIAGNOSIS_CODE_3) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIAGNOSIS_CODE_3,
     last_value(DIAGNOSIS_DESC_3) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIAGNOSIS_DESC_3,
     last_value(DIAGNOSIS_CODE_4) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIAGNOSIS_CODE_4,
     last_value(DIAGNOSIS_DESC_4) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIAGNOSIS_DESC_4,
     last_value(DIAGNOSIS_CODE_5) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIAGNOSIS_CODE_5,
     last_value(DIAGNOSIS_DESC_5) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DIAGNOSIS_DESC_5,
     last_value(OR_PROCEDURE_CODE_1) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OR_PROCEDURE_CODE_1,
     last_value(OR_PROCEDURE_DESC_1) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OR_PROCEDURE_DESC_1,
     last_value(OR_PROCEDURE_CODE_2) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OR_PROCEDURE_CODE_2,
     last_value(OR_PROCEDURE_DESC_2) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OR_PROCEDURE_DESC_2,
     last_value(OR_PROCEDURE_CODE_3) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OR_PROCEDURE_CODE_3,
     last_value(OR_PROCEDURE_DESC_3) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OR_PROCEDURE_DESC_3,
     last_value(OR_PROCEDURE_CODE_4) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OR_PROCEDURE_CODE_4,
     last_value(OR_PROCEDURE_DESC_4) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OR_PROCEDURE_DESC_4,
     last_value(OR_PROCEDURE_CODE_5) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OR_PROCEDURE_CODE_5,
     last_value(OR_PROCEDURE_DESC_5) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OR_PROCEDURE_DESC_5,
     last_value(NON_OR_PROCEDURE_CODE_1) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NON_OR_PROCEDURE_CODE_1,
     last_value(NON_OR_PROCEDURE_DESC_1) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NON_OR_PROCEDURE_DESC_1,
     last_value(NON_OR_PROCEDURE_CODE_2) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NON_OR_PROCEDURE_CODE_2,
     last_value(NON_OR_PROCEDURE_DESC_2) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NON_OR_PROCEDURE_DESC_2,
     last_value(NON_OR_PROCEDURE_CODE_3) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NON_OR_PROCEDURE_CODE_3,
     last_value(NON_OR_PROCEDURE_DESC_3) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NON_OR_PROCEDURE_DESC_3,
     last_value(NON_OR_PROCEDURE_CODE_4) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NON_OR_PROCEDURE_CODE_4,
     last_value(NON_OR_PROCEDURE_DESC_4) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NON_OR_PROCEDURE_DESC_4,
     last_value(NON_OR_PROCEDURE_CODE_5) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NON_OR_PROCEDURE_CODE_5,
     last_value(NON_OR_PROCEDURE_DESC_5) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NON_OR_PROCEDURE_DESC_5,
     last_value(RETURN_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as RETURN_CODE,
     last_value(RETURN_MESSAGE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as RETURN_MESSAGE,
     last_value(RETURN_EXTENSION) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as RETURN_EXTENSION,
     last_value(GROUPER_VERSION) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as GROUPER_VERSION,
     last_value(GROUPER_METHOD) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as GROUPER_METHOD,
     last_value(PRICER_VERSION) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRICER_VERSION

     FROM EDW_STAGING.DIM_BWC_CALC_DRG_OUTPUT_SCDALL_STEP2)

select md5(cast(
    
    coalesce(cast(MEDICAL_INVOICE_NUMBER as 
    varchar
), '')

 as 
    varchar
)) AS BWC_CALC_DRG_OUTPUT_HKEY
,UNIQUE_ID_KEY
,MEDICAL_INVOICE_NUMBER
,DATE_SENT::date as DATE_SENT
,DIAGNOSIS_RELATED_GROUP_CODE
,DIAGNOSIS_RELATED_GROUP_TITLE
,MAJOR_TITLE
,MAJOR_CATEGORY
,DIAGNOSIS_CODE_1
,DIAGNOSIS_DESC_1
,DIAGNOSIS_CODE_2
,DIAGNOSIS_DESC_2
,DIAGNOSIS_CODE_3
,DIAGNOSIS_DESC_3
,DIAGNOSIS_CODE_4
,DIAGNOSIS_DESC_4
,DIAGNOSIS_CODE_5
,DIAGNOSIS_DESC_5
,OR_PROCEDURE_CODE_1
,OR_PROCEDURE_DESC_1
,OR_PROCEDURE_CODE_2
,OR_PROCEDURE_DESC_2
,OR_PROCEDURE_CODE_3
,OR_PROCEDURE_DESC_3
,OR_PROCEDURE_CODE_4
,OR_PROCEDURE_DESC_4
,OR_PROCEDURE_CODE_5
,OR_PROCEDURE_DESC_5
,NON_OR_PROCEDURE_CODE_1
,NON_OR_PROCEDURE_DESC_1
,NON_OR_PROCEDURE_CODE_2
,NON_OR_PROCEDURE_DESC_2
,NON_OR_PROCEDURE_CODE_3
,NON_OR_PROCEDURE_DESC_3
,NON_OR_PROCEDURE_CODE_4
,NON_OR_PROCEDURE_DESC_4
,NON_OR_PROCEDURE_CODE_5
,NON_OR_PROCEDURE_DESC_5
,RETURN_CODE
,RETURN_MESSAGE
,RETURN_EXTENSION
,GROUPER_VERSION
,GROUPER_METHOD
,PRICER_VERSION
,CURRENT_TIMESTAMP AS LOAD_DATETIME
,'CAM' AS PRIMARY_SOURCE_SYSTEM
 from SCD
      );
    