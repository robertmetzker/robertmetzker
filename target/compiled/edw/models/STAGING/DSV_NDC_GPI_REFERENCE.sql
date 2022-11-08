

---- SRC LAYER ----
WITH
SRC_NDC as ( SELECT *     from     STAGING.DST_NDC_GPI_REFERENCE ),
//SRC_NDC as ( SELECT *     from     DST_NDC_GPI_REFERENCE) ,

---- LOGIC LAYER ----

LOGIC_NDC as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, NDC_11_CODE                                        AS                                        NDC_11_CODE 
		, NDC_11_FORMATTED_CODE                              AS                              NDC_11_FORMATTED_CODE 
		, NDC_5_LABELER_CODE                                 AS                                 NDC_5_LABELER_CODE 
		, NDC_5_LABELER_NAME                                 AS                                 NDC_5_LABELER_NAME 
		, NDC_4_PRODUCT_CODE                                 AS                                 NDC_4_PRODUCT_CODE 
		, NDC_4_PRODUCT_DESC                                 AS                                 NDC_4_PRODUCT_DESC 
		, NDC_2_PACKAGE_CODE                                 AS                                 NDC_2_PACKAGE_CODE 
		, GPI_14_CODE                                        AS                                        GPI_14_CODE 
		, GPI_14_DESC                                        AS                                        GPI_14_DESC 
		, GPI_2_GROUP_CODE                                   AS                                   GPI_2_GROUP_CODE 
		, GPI_2_GROUP_DESC                                   AS                                   GPI_2_GROUP_DESC 
		, GPI_4_CLASS_CODE                                   AS                                   GPI_4_CLASS_CODE 
		, GPI_4_CLASS_DESC                                   AS                                   GPI_4_CLASS_DESC 
		, GPI_6_SUBCLASS_CODE                                AS                                GPI_6_SUBCLASS_CODE 
		, GPI_6_SUBCLASS_DESC                                AS                                GPI_6_SUBCLASS_DESC 
		, GPI_8_DRUG_NAME_CODE                               AS                               GPI_8_DRUG_NAME_CODE 
		, GPI_8_DRUG_NAME_DESC                               AS                               GPI_8_DRUG_NAME_DESC 
		, GPI_10_DRUG_NAME_EXT_CODE                          AS                          GPI_10_DRUG_NAME_EXT_CODE 
		, GPI_10_DRUG_NAME_EXT_DESC                          AS                          GPI_10_DRUG_NAME_EXT_DESC 
		, GPI_12_DRUG_DOSAGE_FORM_CODE                       AS                       GPI_12_DRUG_DOSAGE_FORM_CODE 
		, GPI_12_DRUG_DOSAGE_FORM_DESC                       AS                       GPI_12_DRUG_DOSAGE_FORM_DESC 
		, NDC_BEGINNING_DATE                                 AS                                 NDC_BEGINNING_DATE 
		, NDC_ENDING_DATE                                    AS                                    NDC_ENDING_DATE 
		, GPI_14_EFFECTIVE_DATE                              AS                              GPI_14_EFFECTIVE_DATE 
		, GPI_14_ENDING_DATE                                 AS                                 GPI_14_ENDING_DATE 
		, BWC_FORMULARY_PRIOR_AUTHORIZATION_IND              AS              BWC_FORMULARY_PRIOR_AUTHORIZATION_IND 
		, BWC_FORMULARY_COVERAGE_CODE                        AS                        BWC_FORMULARY_COVERAGE_CODE 
		, BWC_FORMULARY_COVERAGE_DESC                        AS                        BWC_FORMULARY_COVERAGE_DESC 
		, BWC_FORMULARY_COVERAGE_LIMITATION_DESC             AS             BWC_FORMULARY_COVERAGE_LIMITATION_DESC 
		, OVER_THE_COUNTER_CODE                              AS                              OVER_THE_COUNTER_CODE 
		, OVER_THE_COUNTER_DESC                              AS                              OVER_THE_COUNTER_DESC 
		, DEA_DRUG_SCHEDULE_CODE                             AS                             DEA_DRUG_SCHEDULE_CODE 
		, DEA_DRUG_SCHEDULE_DESC                             AS                             DEA_DRUG_SCHEDULE_DESC 
		, DRUG_STRENGTH_CODE                                 AS                                 DRUG_STRENGTH_CODE 
		, STRENGTH_MEASURE_CODE                              AS                              STRENGTH_MEASURE_CODE 
		, ROUTE_OF_ADMINISTRATION_CODE                       AS                       ROUTE_OF_ADMINISTRATION_CODE 
		, ROUTE_OF_ADMINISTRATION_DESC                       AS                       ROUTE_OF_ADMINISTRATION_DESC 
		, DOSAGE_FORM_CODE                                   AS                                   DOSAGE_FORM_CODE 
		, DOSAGE_FORM_DESC                                   AS                                   DOSAGE_FORM_DESC 
		, PACKAGE_SIZE_UNIT_OF_MEASURE_CODE                  AS                  PACKAGE_SIZE_UNIT_OF_MEASURE_CODE 
		, PACKAGE_SIZE_UNIT_OF_MEASURE_DESC                  AS                  PACKAGE_SIZE_UNIT_OF_MEASURE_DESC 
		, PACKAGE_TYPE_CODE                                  AS                                  PACKAGE_TYPE_CODE 
		, PACKAGE_TYPE_DESC                                  AS                                  PACKAGE_TYPE_DESC 
		, PACKAGE_DOSE_CODE                                  AS                                  PACKAGE_DOSE_CODE 
		, PACKAGE_DOSE_DESC                                  AS                                  PACKAGE_DOSE_DESC 
		, PACKAGE_SIZE_QTY                                   AS                                   PACKAGE_SIZE_QTY 
		, PACKAGE_QTY                                        AS                                        PACKAGE_QTY 
		, MILLIGRAM_EQUIVALENCE_PER_UNIT                     AS                     MILLIGRAM_EQUIVALENCE_PER_UNIT 
		, ACTIVE_DRUG_STRENGTH_QUANTITY                      AS                      ACTIVE_DRUG_STRENGTH_QUANTITY 
		, CURRENT_AWP_DRUG_AMOUNT                            AS                            CURRENT_AWP_DRUG_AMOUNT 
		, MED_CONVERSION_FACTOR                              AS                              MED_CONVERSION_FACTOR 
		, AED_CONVERSION_FACTOR                              AS                              AED_CONVERSION_FACTOR 
		, GPI_PAYMENT_CATEGORY_DESC                          AS                          GPI_PAYMENT_CATEGORY_DESC 
		, EQUIVALENT_DRUG_NAME                               AS                               EQUIVALENT_DRUG_NAME 
		, DRUG_DESCRIPTOR_IDENTIFIER                         AS                         DRUG_DESCRIPTOR_IDENTIFIER 
		, NDC_VERSION_NUMBER                                 AS                                 NDC_VERSION_NUMBER 
		, NDC_ACTIVE_IND                                     AS                                     NDC_ACTIVE_IND 
		from SRC_NDC
            )

---- RENAME LAYER ----
,

RENAME_NDC as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, NDC_11_CODE                                        as                                        NDC_11_CODE
		, NDC_11_FORMATTED_CODE                              as                              NDC_11_FORMATTED_CODE
		, NDC_5_LABELER_CODE                                 as                                 NDC_5_LABELER_CODE
		, NDC_5_LABELER_NAME                                 as                                 NDC_5_LABELER_NAME
		, NDC_4_PRODUCT_CODE                                 as                                 NDC_4_PRODUCT_CODE
		, NDC_4_PRODUCT_DESC                                 as                                 NDC_4_PRODUCT_DESC
		, NDC_2_PACKAGE_CODE                                 as                                 NDC_2_PACKAGE_CODE
		, GPI_14_CODE                                        as                                        GPI_14_CODE
		, GPI_14_DESC                                        as                                        GPI_14_DESC
		, GPI_2_GROUP_CODE                                   as                                   GPI_2_GROUP_CODE
		, GPI_2_GROUP_DESC                                   as                                   GPI_2_GROUP_DESC
		, GPI_4_CLASS_CODE                                   as                                   GPI_4_CLASS_CODE
		, GPI_4_CLASS_DESC                                   as                                   GPI_4_CLASS_DESC
		, GPI_6_SUBCLASS_CODE                                as                                GPI_6_SUBCLASS_CODE
		, GPI_6_SUBCLASS_DESC                                as                                GPI_6_SUBCLASS_DESC
		, GPI_8_DRUG_NAME_CODE                               as                               GPI_8_DRUG_NAME_CODE
		, GPI_8_DRUG_NAME_DESC                               as                               GPI_8_DRUG_NAME_DESC
		, GPI_10_DRUG_NAME_EXT_CODE                          as                          GPI_10_DRUG_NAME_EXT_CODE
		, GPI_10_DRUG_NAME_EXT_DESC                          as                          GPI_10_DRUG_NAME_EXT_DESC
		, GPI_12_DRUG_DOSAGE_FORM_CODE                       as                       GPI_12_DRUG_DOSAGE_FORM_CODE
		, GPI_12_DRUG_DOSAGE_FORM_DESC                       as                       GPI_12_DRUG_DOSAGE_FORM_DESC
		, NDC_BEGINNING_DATE                                 as                                 NDC_EFFECTIVE_DATE
		, NDC_ENDING_DATE                                    as                                       NDC_END_DATE
		, GPI_14_EFFECTIVE_DATE                              as                                 GPI_EFFECTIVE_DATE
		, GPI_14_ENDING_DATE                                 as                                       GPI_END_DATE
		, BWC_FORMULARY_PRIOR_AUTHORIZATION_IND              as              BWC_FORMULARY_PRIOR_AUTHORIZATION_IND
		, BWC_FORMULARY_COVERAGE_CODE                        as                        BWC_FORMULARY_COVERAGE_CODE
		, BWC_FORMULARY_COVERAGE_DESC                        as                        BWC_FORMULARY_COVERAGE_DESC
		, BWC_FORMULARY_COVERAGE_LIMITATION_DESC             as             BWC_FORMULARY_COVERAGE_LIMITATION_DESC
		, OVER_THE_COUNTER_CODE                              as                              OVER_THE_COUNTER_CODE
		, OVER_THE_COUNTER_DESC                              as                              OVER_THE_COUNTER_DESC
		, DEA_DRUG_SCHEDULE_CODE                             as                             DEA_DRUG_SCHEDULE_CODE
		, DEA_DRUG_SCHEDULE_DESC                             as                             DEA_DRUG_SCHEDULE_DESC
		, DRUG_STRENGTH_CODE                                 as                                 DRUG_STRENGTH_CODE
		, STRENGTH_MEASURE_CODE                              as                              STRENGTH_MEASURE_CODE
		, ROUTE_OF_ADMINISTRATION_CODE                       as                       ROUTE_OF_ADMINISTRATION_CODE
		, ROUTE_OF_ADMINISTRATION_DESC                       as                       ROUTE_OF_ADMINISTRATION_DESC
		, DOSAGE_FORM_CODE                                   as                                   DOSAGE_FORM_CODE
		, DOSAGE_FORM_DESC                                   as                                   DOSAGE_FORM_DESC
		, PACKAGE_SIZE_UNIT_OF_MEASURE_CODE                  as                  PACKAGE_SIZE_UNIT_OF_MEASURE_CODE
		, PACKAGE_SIZE_UNIT_OF_MEASURE_DESC                  as                  PACKAGE_SIZE_UNIT_OF_MEASURE_DESC
		, PACKAGE_TYPE_CODE                                  as                                  PACKAGE_TYPE_CODE
		, PACKAGE_TYPE_DESC                                  as                                  PACKAGE_TYPE_DESC
		, PACKAGE_DOSE_CODE                                  as                                  PACKAGE_DOSE_CODE
		, PACKAGE_DOSE_DESC                                  as                                  PACKAGE_DOSE_DESC
		, PACKAGE_SIZE_QTY                                   as                                   PACKAGE_SIZE_QTY
		, PACKAGE_QTY                                        as                                        PACKAGE_QTY
		, MILLIGRAM_EQUIVALENCE_PER_UNIT                     as                    MILLIGRAMS_EQUIVALENCE_PER_UNIT
		, ACTIVE_DRUG_STRENGTH_QUANTITY                      as                      ACTIVE_DRUG_STRENGTH_QUANTITY
		, CURRENT_AWP_DRUG_AMOUNT                            as                            CURRENT_AWP_DRUG_AMOUNT
		, MED_CONVERSION_FACTOR                              as                              MED_CONVERSION_FACTOR
		, AED_CONVERSION_FACTOR                              as                              AED_CONVERSION_FACTOR
		, GPI_PAYMENT_CATEGORY_DESC                          as                          GPI_PAYMENT_CATEGORY_DESC
		, EQUIVALENT_DRUG_NAME                               as                               EQUIVALENT_DRUG_NAME
		, DRUG_DESCRIPTOR_IDENTIFIER                         as                         DRUG_DESCRIPTOR_IDENTIFIER
		, NDC_VERSION_NUMBER                                 as                                 NDC_VERSION_NUMBER
		, NDC_ACTIVE_IND                                     as                                     NDC_ACTIVE_IND 
				FROM     LOGIC_NDC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_NDC                            as ( SELECT * from    RENAME_NDC   ),

---- JOIN LAYER ----

 JOIN_NDC  as  ( SELECT * 
				FROM  FILTER_NDC )
 SELECT * FROM  JOIN_NDC