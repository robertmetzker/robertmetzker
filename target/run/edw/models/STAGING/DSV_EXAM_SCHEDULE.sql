
  create or replace  view DEV_EDW.STAGING.DSV_EXAM_SCHEDULE  as (
    

---- SRC LAYER ----
WITH
SRC_EXAM           as ( SELECT *     FROM     STAGING.DST_CASE_EXAM_SCHEDULE ),
//SRC_EXAM           as ( SELECT *     FROM     DST_CASE_EXAM_SCHEDULE) ,

---- LOGIC LAYER ----


LOGIC_EXAM as ( SELECT 
		  EXAM_SCHEDULE_ID                                   as                                   EXAM_SCHEDULE_ID 
		, CDES_CLMT_AVL_MON_IND                              as                              CDES_CLMT_AVL_MON_IND 
		, CDES_CLMT_AVL_TUE_IND                              as                              CDES_CLMT_AVL_TUE_IND 
		, CDES_CLMT_AVL_WED_IND                              as                              CDES_CLMT_AVL_WED_IND 
		, CDES_CLMT_AVL_THU_IND                              as                              CDES_CLMT_AVL_THU_IND 
		, CDES_CLMT_AVL_FRI_IND                              as                              CDES_CLMT_AVL_FRI_IND 
		, CDES_CLMT_AVL_SAT_IND                              as                              CDES_CLMT_AVL_SAT_IND 
		, CDES_CLMT_AVL_SUN_IND                              as                              CDES_CLMT_AVL_SUN_IND 
		, CDES_ITPRT_NEED_IND                                as                                CDES_ITPRT_NEED_IND 
		, CDES_GRTT_45_IND                                   as                                   CDES_GRTT_45_IND 
		, CDES_TRVL_REMB_IND                                 as                                 CDES_TRVL_REMB_IND 
		, CDES_ADDTNL_TST_IND                                as                                CDES_ADDTNL_TST_IND 
		, CDES_ADNDM_REQS_IND                                as                                CDES_ADNDM_REQS_IND 
		, CDES_RSLT_SUSPD_IND                                as                                CDES_RSLT_SUSPD_IND 
		FROM SRC_EXAM
            )

---- RENAME LAYER ----
,

RENAME_EXAM       as ( SELECT 
		  EXAM_SCHEDULE_ID                                   as                                      UNIQUE_ID_KEY
		, CDES_CLMT_AVL_MON_IND                              as                            MONDAY_AVAILABILITY_IND
		, CDES_CLMT_AVL_TUE_IND                              as                           TUESDAY_AVAILABILITY_IND
		, CDES_CLMT_AVL_WED_IND                              as                         WEDNESDAY_AVAILABILITY_IND
		, CDES_CLMT_AVL_THU_IND                              as                          THURSDAY_AVAILABILITY_IND
		, CDES_CLMT_AVL_FRI_IND                              as                            FRIDAY_AVAILABILITY_IND
		, CDES_CLMT_AVL_SAT_IND                              as                          SATURDAY_AVAILABILITY_IND
		, CDES_CLMT_AVL_SUN_IND                              as                            SUNDAY_AVAILABILITY_IND
		, CDES_ITPRT_NEED_IND                                as                             INTERPRETER_NEEDED_IND
		, CDES_GRTT_45_IND                                   as                          GREATER_THAN_45_MILES_IND
		, CDES_TRVL_REMB_IND                                 as                           TRAVEL_REIMBURSEMENT_IND
		, CDES_ADDTNL_TST_IND                                as                             ADDITIONAL_TESTING_IND
		, CDES_ADNDM_REQS_IND                                as                             ADDENDUM_REQUESTED_IND
		, CDES_RSLT_SUSPD_IND                                as                               RESULT_SUSPENDED_IND 
				FROM     LOGIC_EXAM   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_EXAM                           as ( SELECT * FROM    RENAME_EXAM   ),

---- JOIN LAYER ----

 JOIN_EXAM        as  ( SELECT * 
				FROM  FILTER_EXAM )
------ETL LAYER------------
,ETL AS(SELECT DISTINCT 
 UNIQUE_ID_KEY
,MONDAY_AVAILABILITY_IND
,TUESDAY_AVAILABILITY_IND
,WEDNESDAY_AVAILABILITY_IND
,THURSDAY_AVAILABILITY_IND
,FRIDAY_AVAILABILITY_IND
,SATURDAY_AVAILABILITY_IND
,SUNDAY_AVAILABILITY_IND
,INTERPRETER_NEEDED_IND
,GREATER_THAN_45_MILES_IND
,TRAVEL_REIMBURSEMENT_IND
,ADDITIONAL_TESTING_IND
,ADDENDUM_REQUESTED_IND
,RESULT_SUSPENDED_IND
FROM JOIN_EXAM)

SELECT * FROM ETL
  );
