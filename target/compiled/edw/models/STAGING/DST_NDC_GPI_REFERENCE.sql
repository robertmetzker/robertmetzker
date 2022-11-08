---- SRC LAYER ----
WITH
SRC_NDC as ( SELECT *     from     STAGING.STG_NDC_VRSN_DURATION ),
SRC_MNMN as ( SELECT *     from     STAGING.STG_DRUG_MANUFACTURERS ),
SRC_DSCRPTR as ( SELECT *     from     STAGING.STG_MEDISPAN_NM_DRUG_DSCRPTR ),
SRC_MGMD as ( SELECT *     from     STAGING.STG_MEDISPAN_GPI_GNRC_NM_DRUG_DSCRPTR ),
SRC_DGPI14 as ( SELECT *     from     STAGING.STG_DRUG_GENERIC_PRODUCT_INDEX ),
SRC_DGPI2 as ( SELECT *     from     STAGING.STG_DRUG_GENERIC_PRODUCT_INDEX ),
SRC_DGPI4 as ( SELECT *     from     STAGING.STG_DRUG_GENERIC_PRODUCT_INDEX ),
SRC_NDC_VRSN as ( SELECT *     from     STAGING.STG_MEDISPAN_DRUG_DSCRPTR_IDNTFR_NTNL_DRUG_CD_VER ),
SRC_DGPI6 as ( SELECT *     from     STAGING.STG_DRUG_GENERIC_PRODUCT_INDEX ),
SRC_DGPI8 as ( SELECT *     from     STAGING.STG_DRUG_GENERIC_PRODUCT_INDEX ),
SRC_DGPI10 as ( SELECT *     from     STAGING.STG_DRUG_GENERIC_PRODUCT_INDEX ),
SRC_DGPI12 as ( SELECT *     from     STAGING.STG_DRUG_GENERIC_PRODUCT_INDEX ),
SRC_ONPA as ( SELECT *     from     STAGING.STG_NDC_PRIOR_AUTH_IND ),
SRC_FRMLRY as ( SELECT *     from     STAGING.STG_NDC_FRMLRY_CVRG ),
SRC_ONLC as ( SELECT *     from     STAGING.STG_NDC_FRMLRY_CVRG_DESC ),
SRC_OTC as ( SELECT *     from     STAGING.STG_NDC_OTC_OR_RX ),
SRC_MNDC as ( SELECT *     from     STAGING.STG_NDC_CODES ),
SRC_PCKG as ( SELECT *     from     STAGING.STG_DRUG_PACKAGE ),
SRC_DCR as ( SELECT *     from     STAGING.STG_BWC_DRUG_CATEGORY_REFERENCE ),
SRC_MEQ as ( SELECT *     from     STAGING.STG_BWC_DRUG_MILLIGRAM_EQUIVALENCE_REFERENCE ),
SRC_NDC_PRCNG as ( SELECT *     from     STAGING.STG_NDC_PRICING_TYPE ),
SRC_MINV as ( SELECT *     from     STAGING.STG_NDC_MANUFACTURERS ),
//SRC_NDC as ( SELECT *     from     STG_NDC_VRSN_DURATION) ,
//SRC_MNMN as ( SELECT *     from     STG_DRUG_MANUFACTURERS) ,
//SRC_DSCRPTR as ( SELECT *     from     STG_MEDISPAN_NM_DRUG_DSCRPTR) ,
//SRC_MGMD as ( SELECT *     from     STG_MEDISPAN_GPI_GNRC_NM_DRUG_DSCRPTR) ,
//SRC_DGPI14 as ( SELECT *     from     STG_DRUG_GENERIC_PRODUCT_INDEX) ,
//SRC_DGPI2 as ( SELECT *     from     STG_DRUG_GENERIC_PRODUCT_INDEX) ,
//SRC_DGPI4 as ( SELECT *     from     STG_DRUG_GENERIC_PRODUCT_INDEX) ,
//SRC_NDC_VRSN as ( SELECT *     from     STG_MEDISPAN_DRUG_DSCRPTR_IDNTFR_NTNL_DRUG_CD_VER) ,
//SRC_DGPI6 as ( SELECT *     from     STG_DRUG_GENERIC_PRODUCT_INDEX) ,
//SRC_DGPI8 as ( SELECT *     from     STG_DRUG_GENERIC_PRODUCT_INDEX) ,
//SRC_DGPI10 as ( SELECT *     from     STG_DRUG_GENERIC_PRODUCT_INDEX) ,
//SRC_DGPI12 as ( SELECT *     from     STG_DRUG_GENERIC_PRODUCT_INDEX) ,
//SRC_ONPA as ( SELECT *     from     STG_NDC_PRIOR_AUTH_IND) ,
//SRC_FRMLRY as ( SELECT *     from     STG_NDC_FRMLRY_CVRG) ,
//SRC_ONLC as ( SELECT *     from     STG_NDC_FRMLRY_CVRG_DESC) ,
//SRC_OTC as ( SELECT *     from     STG_NDC_OTC_OR_RX) ,
//SRC_MNDC as ( SELECT *     from     STG_NDC_CODES) ,
//SRC_PCKG as ( SELECT *     from     STG_DRUG_PACKAGE) ,
//SRC_DCR as ( SELECT *     from     STG_BWC_DRUG_CATEGORY_REFERENCE) ,
//SRC_MEQ as ( SELECT *     from     STG_BWC_DRUG_MILLIGRAM_EQUIVALENCE_REFERENCE) ,
//SRC_NDC_PRCNG as ( SELECT *     from     STG_NDC_PRICING_TYPE) ,
//SRC_MINV as ( SELECT *     from     STG_NDC_MANUFACTURERS) ,

---- LOGIC LAYER ----

LOGIC_NDC as ( SELECT 
		FK_NDCL_CODE||FK_DCPR_CODE||FK_NDCP_CODE 		     AS 									   NDC_11_CODE
		, FK_NDCL_CODE||' '||FK_DCPR_CODE||' '||FK_NDCP_CODE AS 							 NDC_11_FORMATTED_CODE
		, TRIM( FK_NDCL_CODE )                               AS                                       FK_NDCL_CODE 
		, TRIM( FK_DCPR_CODE )                               AS                                       FK_DCPR_CODE 
		, TRIM( FK_NDCP_CODE )                               AS                                       FK_NDCP_CODE 
		, FK_NDCV_NMBR                                       AS                                       FK_NDCV_NMBR 
		, BGNG_DATE                                          AS                                          BGNG_DATE 
		, NULLIF(ENDNG_DATE, '3000-01-01')                   AS                                         ENDNG_DATE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		from SRC_NDC
            ),
LOGIC_MNMN as ( SELECT 
		  TRIM( NAME )                                       AS                                               NAME 
		, TRIM( ABRVD_NAME )                                 AS                                         ABRVD_NAME 
		, TRIM( LBLR_TYPE_CODE )                             AS                                     LBLR_TYPE_CODE
		, CASE TRIM( LBLR_TYPE_CODE )  
				WHEN 'B' THEN 'BRAND'
		        WHEN 'G' THEN 'GENERIC'
				WHEN 'O' THEN 'EITHER/OR'     ELSE NULL END  AS                                     LBLR_TYPE_NAME 
		, FK_MLBI_NMBR                                       AS                                       FK_MLBI_NMBR 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		from SRC_MNMN
            ),
LOGIC_DSCRPTR as ( SELECT 
		  TRIM( MDPN_NAME )                                  AS                                          MDPN_NAME 
		, TRIM( STRTH_QTY )                                  AS                                          STRTH_QTY 
		, TRIM( FK_MNST_CODE )                               AS                                       FK_MNST_CODE 
		, TRIM( FK_MNRA_CODE )                               AS                                       FK_MNRA_CODE 
		, TRIM( MNRA_NAME )                                  AS                                          MNRA_NAME 
		, TRIM( FK_MNDT_CODE )                               AS                                       FK_MNDT_CODE 
		, TRIM( MNDT_NAME )                                  AS                                          MNDT_NAME 
		, FK_MDDI_NMBR                                       AS                                       FK_MDDI_NMBR 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, MDPN_EFCTV_DATE                                    AS                                    MDPN_EFCTV_DATE 
		, MDPN_ENDNG_DATE                                    AS                                    MDPN_ENDNG_DATE 
		from SRC_DSCRPTR
            ),
LOGIC_MGMD as ( SELECT 
		  FK_MTDG_CODE||FK_MTDC_CODE||FK_MTDS_CODE||FK_MTDN_CODE||FK_MDNE_CODE||FK_MDFT_CODE||FK_MGGN_CODE AS GPI_14_CODE
		, FK_MTDG_CODE||FK_MTDC_CODE AS GPI_4_CLASS_CODE
		, FK_MTDG_CODE||FK_MTDC_CODE||FK_MTDS_CODE AS GPI_6_SUBCLASS_CODE
		, FK_MTDG_CODE||FK_MTDC_CODE||FK_MTDS_CODE||FK_MTDN_CODE AS GPI_8_DRUG_NAME_CODE
		, FK_MTDG_CODE||FK_MTDC_CODE||FK_MTDS_CODE||FK_MTDN_CODE||FK_MDNE_CODE AS GPI_10_DRUG_NAME_EXT_CODE
		, FK_MTDG_CODE||FK_MTDC_CODE||FK_MTDS_CODE||FK_MTDN_CODE||FK_MDNE_CODE||FK_MDFT_CODE AS GPI_12_DRUG_DOSAGE_FORM_CODE
		, GPI_EFFECTIVE_DATE                                 AS                                 GPI_EFFECTIVE_DATE 
		, GPI_END_DATE                                       AS                                       GPI_END_DATE 
		, TRIM( FK_MTDG_CODE )                               AS                                       FK_MTDG_CODE 
		, FK_MDDI_NMBR                                       AS                                       FK_MDDI_NMBR 
		, TRIM( FK_MTDC_CODE )                               AS                                       FK_MTDC_CODE 
		, TRIM( FK_MTDS_CODE )                               AS                                       FK_MTDS_CODE 
		, TRIM( FK_MTDN_CODE )                               AS                                       FK_MTDN_CODE 
		, TRIM( FK_MDNE_CODE )                               AS                                       FK_MDNE_CODE 
		, TRIM( FK_MDFT_CODE )                               AS                                       FK_MDFT_CODE 
		, TRIM( FK_MGGN_CODE )                               AS                                       FK_MGGN_CODE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		from SRC_MGMD
            ),
LOGIC_DGPI14 as ( SELECT 
		  TRIM( DGPI_NM )                                    AS                                            DGPI_NM 
		, DGPI_EFF_DT                                        AS                                        DGPI_EFF_DT 
		, TRIM( DGPI_CD )                                    AS                                            DGPI_CD 
		from SRC_DGPI14
            ),
LOGIC_DGPI2 as ( SELECT 
		  TRIM( DGPI_NM )                                    AS                                            DGPI_NM 
		, DGPI_EFF_DT                                        AS                                        DGPI_EFF_DT 
		, TRIM( DGPI_CD )                                    AS                                            DGPI_CD 
		from SRC_DGPI2
            ),
LOGIC_DGPI4 as ( SELECT 
		  TRIM( DGPI_NM )                                    AS                                            DGPI_NM 
		, DGPI_EFF_DT                                        AS                                        DGPI_EFF_DT 
		, TRIM( DGPI_CD )                                    AS                                            DGPI_CD 
		from SRC_DGPI4
            ),
LOGIC_NDC_VRSN as ( SELECT 
		  FK_MDDI_NMBR                                       AS                                       FK_MDDI_NMBR 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		, TRIM( FK_NDCL_CODE )                               AS                                       FK_NDCL_CODE 
		, TRIM( FK_DCPR_CODE )                               AS                                       FK_DCPR_CODE 
		, TRIM( FK_NDCP_CODE )                               AS                                       FK_NDCP_CODE 
		, FK_NDCV_NMBR                                       AS                                       FK_NDCV_NMBR 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		from SRC_NDC_VRSN
            ),
LOGIC_DGPI6 as ( SELECT 
		  TRIM( DGPI_NM )                                    AS                                            DGPI_NM 
		, DGPI_EFF_DT                                        AS                                        DGPI_EFF_DT 
		, TRIM( DGPI_CD )                                    AS                                            DGPI_CD 
		from SRC_DGPI6
            ),
LOGIC_DGPI8 as ( SELECT 
		  TRIM( DGPI_NM )                                    AS                                            DGPI_NM 
		, DGPI_EFF_DT                                        AS                                        DGPI_EFF_DT 
		, TRIM( DGPI_CD )                                    AS                                            DGPI_CD 
		from SRC_DGPI8
            ),
LOGIC_DGPI10 as ( SELECT 
		  TRIM( DGPI_NM )                                    AS                                            DGPI_NM 
		, DGPI_EFF_DT                                        AS                                        DGPI_EFF_DT 
		, TRIM( DGPI_CD )                                    AS                                            DGPI_CD 
		from SRC_DGPI10
            ),
LOGIC_DGPI12 as ( SELECT 
		  TRIM( DGPI_NM )                                    AS                                            DGPI_NM 
		, DGPI_EFF_DT                                        AS                                        DGPI_EFF_DT 
		, TRIM( DGPI_CD )                                    AS                                            DGPI_CD 
		from SRC_DGPI12
            ),
LOGIC_ONPA as ( SELECT 
		  TRIM( IND )                                        AS                                                IND 
		, TRIM( FK_NDCL_CODE )                               AS                                       FK_NDCL_CODE 
		, TRIM( FK_DCPR_CODE )                               AS                                       FK_DCPR_CODE 
		, TRIM( FK_NDCP_CODE )                               AS                                       FK_NDCP_CODE 
		, FK_NDCV_NMBR                                       AS                                       FK_NDCV_NMBR 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		from SRC_ONPA
            ),
LOGIC_FRMLRY as ( SELECT 
		  TRIM( FK_NDAT_CODE )                               AS                                       FK_NDAT_CODE 
		, TRIM( NDAT_DESC )                                  AS                                          NDAT_DESC 
		, TRIM( FK_NDCL_CODE )                               AS                                       FK_NDCL_CODE 
		, TRIM( FK_DCPR_CODE )                               AS                                       FK_DCPR_CODE 
		, TRIM( FK_NDCP_CODE )                               AS                                       FK_NDCP_CODE 
		, FK_NDCV_NMBR                                       AS                                       FK_NDCV_NMBR 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		from SRC_FRMLRY
            ),
LOGIC_ONLC as ( SELECT 
		  TRIM( DESCR )                                      AS                                              DESCR 
		, TRIM( FK_NDCL_CODE )                               AS                                       FK_NDCL_CODE 
		, TRIM( FK_DCPR_CODE )                               AS                                       FK_DCPR_CODE 
		, TRIM( FK_NDCP_CODE )                               AS                                       FK_NDCP_CODE 
		, FK_NDCV_NMBR                                       AS                                       FK_NDCV_NMBR 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		from SRC_ONLC
            ),
LOGIC_OTC as ( SELECT 
		  TRIM( FK_MNOT_CODE )                               AS                                       FK_MNOT_CODE 
		, TRIM( MNOT_NAME )                                  AS                                          MNOT_NAME 
		, TRIM( FK_NDCL_CODE )                               AS                                       FK_NDCL_CODE 
		, TRIM( FK_DCPR_CODE )                               AS                                       FK_DCPR_CODE 
		, TRIM( FK_NDCP_CODE )                               AS                                       FK_NDCP_CODE 
		, FK_NDCV_NMBR                                       AS                                       FK_NDCV_NMBR 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		from SRC_OTC
            ),
LOGIC_MNDC as ( SELECT 
		  TRIM( FK_MDCT_CODE )                               AS                                       FK_MDCT_CODE 
		, TRIM( MDCT_NAME )                                  AS                                          MDCT_NAME 
		, TRIM( FK_NDCL_CODE )                               AS                                       FK_NDCL_CODE 
		, TRIM( FK_DCPR_CODE )                               AS                                       FK_DCPR_CODE 
		, TRIM( FK_NDCP_CODE )                               AS                                       FK_NDCP_CODE 
		, FK_NDCV_NMBR                                       AS                                       FK_NDCV_NMBR 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		from SRC_MNDC
            ),
LOGIC_PCKG as ( SELECT 
		  TRIM( FK_MPSM_CODE )                               AS                                       FK_MPSM_CODE 
		, TRIM( MPSM_DESCR )                                 AS                                         MPSM_DESCR 
		, TRIM( FK_MPDT_CODE )                               AS                                       FK_MPDT_CODE 
		, TRIM( MPDT_DESCR )                                 AS                                         MPDT_DESCR 
		, TRIM( FK_MDUP_CODE )                               AS                                       FK_MDUP_CODE 
		, TRIM( MDUP_DESCR )                                 AS                                         MDUP_DESCR 
		, PCKG_SIZE_QTY                                      AS                                      PCKG_SIZE_QTY 
		, PCKG_QTY                                           AS                                           PCKG_QTY 
		, TRIM( FK_NDCL_CODE )                               AS                                       FK_NDCL_CODE 
		, TRIM( FK_MGGN_CODE )                               AS                                       FK_MGGN_CODE 
		, TRIM( FK_DCPR_CODE )                               AS                                       FK_DCPR_CODE 
		, TRIM( FK_NDCP_CODE )                               AS                                       FK_NDCP_CODE 
		, FK_NDCV_NMBR                                       AS                                       FK_NDCV_NMBR 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		from SRC_PCKG
            ),
LOGIC_DCR as ( SELECT 
		  TRIM( BWC_DRUG_CATEGORY_NAME )                     AS                             BWC_DRUG_CATEGORY_NAME 
		, TRIM( MEDISPAN_4_GPI_CLASS_CODE )                  AS                          MEDISPAN_4_GPI_CLASS_CODE 
		from SRC_DCR
            ),
LOGIC_MEQ as ( SELECT 
		  CASE  EQUIVALENT_DRUG_NAME WHEN 'MORPHINE'
		   THEN CONVERSION_FACTOR_QUANTITY ELSE NULL END     AS                              MED_CONVERSION_FACTOR 
		, CASE  EQUIVALENT_DRUG_NAME WHEN 'DIAZEPAM'
		   THEN CONVERSION_FACTOR_QUANTITY ELSE NULL END     AS                              AED_CONVERSION_FACTOR 
		, CONVERSION_FACTOR_QUANTITY * ACTIVE_DRUG_STRENGTH_QUANTITY
												             AS                     MILLIGRAM_EQUIVALENCE_PER_UNIT           
		, TRIM( EQUIVALENT_DRUG_NAME )                       AS                               EQUIVALENT_DRUG_NAME 
		, ACTIVE_DRUG_STRENGTH_QUANTITY                      AS                      ACTIVE_DRUG_STRENGTH_QUANTITY 
		, TRIM( DRUG_STRENGTH_CODE )                         AS                                 DRUG_STRENGTH_CODE 
		, TRIM( MEDISPAN_14_GPI_CODE )                       AS                               MEDISPAN_14_GPI_CODE 
		, TRIM( DRUG_ROUTE_CODE )                            AS                                    DRUG_ROUTE_CODE 
		, TRIM( DRUG_STRENGTH_MEASURE_CODE )                 AS                         DRUG_STRENGTH_MEASURE_CODE 
		from SRC_MEQ
            ),
LOGIC_NDC_PRCNG as ( SELECT 
		  AMT                                                AS                                                AMT 
		, TRIM( FK_NDCL_CODE )                               AS                                       FK_NDCL_CODE 
		, TRIM( FK_DCPR_CODE )                               AS                                       FK_DCPR_CODE 
		, TRIM( FK_NDCP_CODE )                               AS                                       FK_NDCP_CODE 
		, FK_NDCV_NMBR                                       AS                                       FK_NDCV_NMBR 
		, TRIM( FK_NDPT_CODE )                               AS                                       FK_NDPT_CODE 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		from SRC_NDC_PRCNG
            ),
LOGIC_MINV as ( SELECT 
		  TRIM( FK_NDCL_CODE )                               AS                                       FK_NDCL_CODE 
		, FK_MLBI_NMBR                                       AS                                       FK_MLBI_NMBR 
		, TRIM( FK_DCPR_CODE )                               AS                                       FK_DCPR_CODE 
		, TRIM( FK_NDCP_CODE )                               AS                                       FK_NDCP_CODE 
		, FK_NDCV_NMBR                                       AS                                       FK_NDCV_NMBR 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		from SRC_MINV
            )

---- RENAME LAYER ----
,

RENAME_NDC as ( SELECT 
		  NDC_11_CODE                                        as                                        NDC_11_CODE
		, NDC_11_FORMATTED_CODE                              as                              NDC_11_FORMATTED_CODE
		, FK_NDCL_CODE                                       as                                 NDC_5_LABELER_CODE
		, FK_DCPR_CODE                                       as                                 NDC_4_PRODUCT_CODE
		, FK_NDCP_CODE                                       as                                 NDC_2_PACKAGE_CODE
		, FK_NDCV_NMBR                                       as                                 NDC_VERSION_NUMBER
		, BGNG_DATE                                          as                                 NDC_BEGINNING_DATE
		, ENDNG_DATE                                         as                                    NDC_ENDING_DATE
		, DCTVT_DTTM                                         as                                         DCTVT_DTTM 
				FROM     LOGIC_NDC   ), 
RENAME_MNMN as ( SELECT 
		  NAME                                               as                                 NDC_5_LABELER_NAME
		, ABRVD_NAME                                         as                  DRUG_MANUFACTURER_ABREVIATED_NAME
		, LBLR_TYPE_CODE                                     as                          MULTI_SOURCE_PRODUCT_CODE
		, LBLR_TYPE_NAME                                     as                          MULTI_SOURCE_PRODUCT_NAME
		, FK_MLBI_NMBR                                       as                                  MNMN_FK_MLBI_NMBR
		, DCTVT_DTTM                                         as                                    MNMN_DCTVT_DTTM
		, EFCTV_DATE                                         as                                    MNMN_EFCTV_DATE
		, ENDNG_DATE                                         as                                    MNMN_ENDNG_DATE 
				FROM     LOGIC_MNMN   ), 
RENAME_DSCRPTR as ( SELECT 
		  MDPN_NAME                                          as                                 NDC_4_PRODUCT_DESC
		, STRTH_QTY                                          as                                 DRUG_STRENGTH_CODE
		, FK_MNST_CODE                                       as                              STRENGTH_MEASURE_CODE
		, FK_MNRA_CODE                                       as                       ROUTE_OF_ADMINISTRATION_CODE
		, MNRA_NAME                                          as                       ROUTE_OF_ADMINISTRATION_DESC
		, FK_MNDT_CODE                                       as                                   DOSAGE_FORM_CODE
		, MNDT_NAME                                          as                                   DOSAGE_FORM_DESC
		, FK_MDDI_NMBR                                       as                               DSCRPTR_FK_MDDI_NMBR
		, DCTVT_DTTM                                         as                                 DSCRPTR_DCTVT_DTTM
		, MDPN_EFCTV_DATE                                    as                                 DSCRPTR_EFCTV_DATE
		, MDPN_ENDNG_DATE                                    as                                 DSCRPTR_ENDNG_DATE 
				FROM     LOGIC_DSCRPTR   ), 
RENAME_MGMD as ( SELECT 
		  GPI_14_CODE                                        as                                        GPI_14_CODE
		, GPI_EFFECTIVE_DATE                                 as                              GPI_14_EFFECTIVE_DATE
		, GPI_END_DATE                                       as                                 GPI_14_ENDING_DATE
		, FK_MTDG_CODE                                       as                                   GPI_2_GROUP_CODE
		, GPI_4_CLASS_CODE            						 as								      GPI_4_CLASS_CODE 		
		, GPI_6_SUBCLASS_CODE         						 as								   GPI_6_SUBCLASS_CODE 		
		, GPI_8_DRUG_NAME_CODE	      						 as								  GPI_8_DRUG_NAME_CODE     	
		, GPI_10_DRUG_NAME_EXT_CODE	  						 as							 GPI_10_DRUG_NAME_EXT_CODE              	
		, GPI_12_DRUG_DOSAGE_FORM_CODE						 as						  GPI_12_DRUG_DOSAGE_FORM_CODE                    		
		, FK_MDDI_NMBR                                       as                                  MGMD_FK_MDDI_NMBR
		, FK_MTDC_CODE                                       as                                  MGMD_FK_MTDC_CODE
		, FK_MTDS_CODE                                       as                                  MGMD_FK_MTDS_CODE
		, FK_MTDN_CODE                                       as                                  MGMD_FK_MTDN_CODE
		, FK_MDNE_CODE                                       as                                  MGMD_FK_MDNE_CODE
		, FK_MDFT_CODE                                       as                                  MGMD_FK_MDFT_CODE
		, FK_MGGN_CODE                                       as                                  MGMD_FK_MGGN_CODE
		, DCTVT_DTTM                                         as                                    MGMD_DCTVT_DTTM
				FROM     LOGIC_MGMD   ), 
RENAME_DGPI14 as ( SELECT 
		  DGPI_NM                                            as                                        GPI_14_DESC
		, DGPI_EFF_DT                                        as                                 DGPI14_DGPI_EFF_DT
		, DGPI_CD                                            as                                     DGPI14_DGPI_CD 
				FROM     LOGIC_DGPI14   ), 
RENAME_DGPI2 as ( SELECT 
		  DGPI_NM                                            as                                   GPI_2_GROUP_DESC
		, DGPI_EFF_DT                                        as                                  DGPI2_DGPI_EFF_DT
		, DGPI_CD                                            as                                      DGPI2_DGPI_CD 
				FROM     LOGIC_DGPI2   ), 
RENAME_DGPI4 as ( SELECT 
		  DGPI_NM                                            as                                   GPI_4_CLASS_DESC
		, DGPI_EFF_DT                                        as                                  DGPI4_DGPI_EFF_DT
		, DGPI_CD                                            as                                      DGPI4_DGPI_CD 
				FROM     LOGIC_DGPI4   ), 
RENAME_NDC_VRSN as ( SELECT 
		  FK_MDDI_NMBR                                       as                         DRUG_DESCRIPTOR_IDENTIFIER
		, EFCTV_DATE                                         as                                NDC_VRSN_EFCTV_DATE
		, ENDNG_DATE                                         as                                NDC_VRSN_ENDNG_DATE
		, FK_NDCL_CODE                                       as                              NDC_VRSN_FK_NDCL_CODE
		, FK_DCPR_CODE                                       as                              NDC_VRSN_FK_DCPR_CODE
		, FK_NDCP_CODE                                       as                              NDC_VRSN_FK_NDCP_CODE
		, FK_NDCV_NMBR                                       as                              NDC_VRSN_FK_NDCV_NMBR
		, DCTVT_DTTM                                         as                                NDC_VRSN_DCTVT_DTTM 
				FROM     LOGIC_NDC_VRSN   ), 
RENAME_DGPI6 as ( SELECT 
		  DGPI_NM                                            as                                GPI_6_SUBCLASS_DESC
		, DGPI_EFF_DT                                        as                                  DGPI6_DGPI_EFF_DT
		, DGPI_CD                                            as                                      DGPI6_DGPI_CD 
				FROM     LOGIC_DGPI6   ), 
RENAME_DGPI8 as ( SELECT 
		  DGPI_NM                                            as                               GPI_8_DRUG_NAME_DESC
		, DGPI_EFF_DT                                        as                                  DGPI8_DGPI_EFF_DT
		, DGPI_CD                                            as                                      DGPI8_DGPI_CD 
				FROM     LOGIC_DGPI8   ), 
RENAME_DGPI10 as ( SELECT 
		  DGPI_NM                                            as                          GPI_10_DRUG_NAME_EXT_DESC
		, DGPI_EFF_DT                                        as                                 DGPI10_DGPI_EFF_DT
		, DGPI_CD                                            as                                     DGPI10_DGPI_CD 
				FROM     LOGIC_DGPI10   ), 
RENAME_DGPI12 as ( SELECT 
		  DGPI_NM                                            as                       GPI_12_DRUG_DOSAGE_FORM_DESC
		, DGPI_EFF_DT                                        as                                 DGPI12_DGPI_EFF_DT
		, DGPI_CD                                            as                                     DGPI12_DGPI_CD 
				FROM     LOGIC_DGPI12   ), 
RENAME_ONPA as ( SELECT 
		  IND                                                as              BWC_FORMULARY_PRIOR_AUTHORIZATION_IND
		, FK_NDCL_CODE                                       as                                  ONPA_FK_NDCL_CODE
		, FK_DCPR_CODE                                       as                                  ONPA_FK_DCPR_CODE
		, FK_NDCP_CODE                                       as                                  ONPA_FK_NDCP_CODE
		, FK_NDCV_NMBR                                       as                                  ONPA_FK_NDCV_NMBR
		, DCTVT_DTTM                                         as                                    ONPA_DCTVT_DTTM
		, EFCTV_DATE                                         as                                    ONPA_EFCTV_DATE
		, ENDNG_DATE                                         as                                    ONPA_ENDNG_DATE 
				FROM     LOGIC_ONPA   ), 
RENAME_FRMLRY as ( SELECT 
		  FK_NDAT_CODE                                       as                        BWC_FORMULARY_COVERAGE_CODE
		, NDAT_DESC                                          as                        BWC_FORMULARY_COVERAGE_DESC
		, FK_NDCL_CODE                                       as                                FRMLRY_FK_NDCL_CODE
		, FK_DCPR_CODE                                       as                                FRMLRY_FK_DCPR_CODE
		, FK_NDCP_CODE                                       as                                FRMLRY_FK_NDCP_CODE
		, FK_NDCV_NMBR                                       as                                FRMLRY_FK_NDCV_NMBR
		, DCTVT_DTTM                                         as                                  FRMLRY_DCTVT_DTTM
		, EFCTV_DATE                                         as                                  FRMLRY_EFCTV_DATE
		, ENDNG_DATE                                         as                                  FRMLRY_ENDNG_DATE 
				FROM     LOGIC_FRMLRY   ), 
RENAME_ONLC as ( SELECT 
		  DESCR                                              as             BWC_FORMULARY_COVERAGE_LIMITATION_DESC
		, FK_NDCL_CODE                                       as                                  ONLC_FK_NDCL_CODE
		, FK_DCPR_CODE                                       as                                  ONLC_FK_DCPR_CODE
		, FK_NDCP_CODE                                       as                                  ONLC_FK_NDCP_CODE
		, FK_NDCV_NMBR                                       as                                  ONLC_FK_NDCV_NMBR
		, DCTVT_DTTM                                         as                                    ONLC_DCTVT_DTTM
		, EFCTV_DATE                                         as                                    ONLC_EFCTV_DATE
		, ENDNG_DATE                                         as                                    ONLC_ENDNG_DATE 
				FROM     LOGIC_ONLC   ), 
RENAME_OTC as ( SELECT 
		  FK_MNOT_CODE                                       as                              OVER_THE_COUNTER_CODE
		, MNOT_NAME                                          as                              OVER_THE_COUNTER_DESC
		, FK_NDCL_CODE                                       as                                   OTC_FK_NDCL_CODE
		, FK_DCPR_CODE                                       as                                   OTC_FK_DCPR_CODE
		, FK_NDCP_CODE                                       as                                   OTC_FK_NDCP_CODE
		, FK_NDCV_NMBR                                       as                                   OTC_FK_NDCV_NMBR
		, DCTVT_DTTM                                         as                                     OTC_DCTVT_DTTM
		, EFCTV_DATE                                         as                                     OTC_EFCTV_DATE
		, ENDNG_DATE                                         as                                     OTC_ENDNG_DATE 
				FROM     LOGIC_OTC   ), 
RENAME_MNDC as ( SELECT 
		  FK_MDCT_CODE                                       as                             DEA_DRUG_SCHEDULE_CODE
		, MDCT_NAME                                          as                             DEA_DRUG_SCHEDULE_DESC
		, FK_NDCL_CODE                                       as                                  MNDC_FK_NDCL_CODE
		, FK_DCPR_CODE                                       as                                  MNDC_FK_DCPR_CODE
		, FK_NDCP_CODE                                       as                                  MNDC_FK_NDCP_CODE
		, FK_NDCV_NMBR                                       as                                  MNDC_FK_NDCV_NMBR
		, DCTVT_DTTM                                         as                                    MNDC_DCTVT_DTTM
		, EFCTV_DATE                                         as                                    MNDC_EFCTV_DATE
		, ENDNG_DATE                                         as                                    MNDC_ENDNG_DATE 
				FROM     LOGIC_MNDC   ), 
RENAME_PCKG as ( SELECT 
		  FK_MPSM_CODE                                       as                  PACKAGE_SIZE_UNIT_OF_MEASURE_CODE
		, MPSM_DESCR                                         as                  PACKAGE_SIZE_UNIT_OF_MEASURE_DESC
		, FK_MPDT_CODE                                       as                                  PACKAGE_TYPE_CODE
		, MPDT_DESCR                                         as                                  PACKAGE_TYPE_DESC
		, FK_MDUP_CODE                                       as                                  PACKAGE_DOSE_CODE
		, MDUP_DESCR                                         as                                  PACKAGE_DOSE_DESC
		, PCKG_SIZE_QTY                                      as                                   PACKAGE_SIZE_QTY
		, PCKG_QTY                                           as                                        PACKAGE_QTY
		, FK_NDCL_CODE                                       as                                  PCKG_FK_NDCL_CODE
		, FK_MGGN_CODE                                       as                                  PCKG_FK_MGGN_CODE
		, FK_DCPR_CODE                                       as                                  PCKG_FK_DCPR_CODE
		, FK_NDCP_CODE                                       as                                  PCKG_FK_NDCP_CODE
		, FK_NDCV_NMBR                                       as                                  PCKG_FK_NDCV_NMBR
		, DCTVT_DTTM                                         as                                    PCKG_DCTVT_DTTM
		, EFCTV_DATE                                         as                                    PCKG_EFCTV_DATE
		, ENDNG_DATE                                         as                                    PCKG_ENDNG_DATE 
				FROM     LOGIC_PCKG   ), 
RENAME_DCR as ( SELECT 
		  BWC_DRUG_CATEGORY_NAME                             as                          GPI_PAYMENT_CATEGORY_DESC
		, MEDISPAN_4_GPI_CLASS_CODE                          as                          MEDISPAN_4_GPI_CLASS_CODE 
				FROM     LOGIC_DCR   ), 
RENAME_MEQ as ( SELECT 
		  MED_CONVERSION_FACTOR                              as                              MED_CONVERSION_FACTOR
		, AED_CONVERSION_FACTOR                              as                              AED_CONVERSION_FACTOR
		, MILLIGRAM_EQUIVALENCE_PER_UNIT                     as                     MILLIGRAM_EQUIVALENCE_PER_UNIT
		, EQUIVALENT_DRUG_NAME                               as                               EQUIVALENT_DRUG_NAME
		, ACTIVE_DRUG_STRENGTH_QUANTITY                      as                      ACTIVE_DRUG_STRENGTH_QUANTITY
		, DRUG_STRENGTH_CODE                                 as                             MEQ_DRUG_STRENGTH_CODE
		, MEDISPAN_14_GPI_CODE                               as                               MEDISPAN_14_GPI_CODE
		, DRUG_ROUTE_CODE                                    as                                    DRUG_ROUTE_CODE
		, DRUG_STRENGTH_MEASURE_CODE                         as                         DRUG_STRENGTH_MEASURE_CODE 
				FROM     LOGIC_MEQ   ), 
RENAME_NDC_PRCNG as ( SELECT 
		  AMT                                                as                            CURRENT_AWP_DRUG_AMOUNT
		, FK_NDCL_CODE                                       as                             NDC_PRCNG_FK_NDCL_CODE
		, FK_DCPR_CODE                                       as                             NDC_PRCNG_FK_DCPR_CODE
		, FK_NDCP_CODE                                       as                             NDC_PRCNG_FK_NDCP_CODE
		, FK_NDCV_NMBR                                       as                             NDC_PRCNG_FK_NDCV_NMBR
		, FK_NDPT_CODE                                       as                                       FK_NDPT_CODE
		, EFCTV_DATE                                         as                               NDC_PRCNG_EFCTV_DATE
		, ENDNG_DATE                                         as                               NDC_PRCNG_ENDNG_DATE 
				FROM     LOGIC_NDC_PRCNG   ), 
RENAME_MINV as ( SELECT 
		  FK_NDCL_CODE                                       as                                  MINV_FK_NDCL_CODE
		, FK_MLBI_NMBR                                       as                                  MINV_FK_MLBI_NMBR
		, FK_DCPR_CODE                                       as                                  MINV_FK_DCPR_CODE
		, FK_NDCP_CODE                                       as                                  MINV_FK_NDCP_CODE
		, FK_NDCV_NMBR                                       as                                  MINV_FK_NDCV_NMBR
		, DCTVT_DTTM                                         as                                    MINV_DCTVT_DTTM
		, EFCTV_DATE                                         as                                    MINV_EFCTV_DATE
		, ENDNG_DATE                                         as                                    MINV_ENDNG_DATE 
				FROM     LOGIC_MINV   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_NDC                            as ( SELECT * from    RENAME_NDC 
		QUALIFY (ROW_NUMBER() OVER (PARTITION BY NDC_5_LABELER_CODE,NDC_4_PRODUCT_CODE,NDC_2_PACKAGE_CODE ORDER BY NDC_VERSION_NUMBER DESC, DCTVT_DTTM DESC) ) = 1 
										),
FILTER_PCKG                           as ( SELECT * from    RENAME_PCKG 
				WHERE PCKG_EFCTV_DATE <= CURRENT_DATE AND PCKG_ENDNG_DATE >= CURRENT_DATE
		QUALIFY (ROW_NUMBER() OVER (PARTITION BY PCKG_FK_NDCL_CODE,PCKG_FK_DCPR_CODE,PCKG_FK_NDCP_CODE ORDER BY PCKG_DCTVT_DTTM DESC))=1
										),
FILTER_FRMLRY                         as ( SELECT * from    RENAME_FRMLRY 
				WHERE FRMLRY_EFCTV_DATE <= CURRENT_DATE AND FRMLRY_ENDNG_DATE >= CURRENT_DATE
		QUALIFY (ROW_NUMBER() OVER (PARTITION BY FRMLRY_FK_NDCL_CODE,FRMLRY_FK_DCPR_CODE,FRMLRY_FK_NDCP_CODE ORDER BY FRMLRY_DCTVT_DTTM DESC)) = 1
										),
FILTER_ONPA                           as ( SELECT * from    RENAME_ONPA 
				WHERE ONPA_EFCTV_DATE <= CURRENT_DATE AND ONPA_ENDNG_DATE >= CURRENT_DATE
		QUALIFY(ROW_NUMBER() OVER (PARTITION BY ONPA_FK_NDCL_CODE,ONPA_FK_DCPR_CODE,ONPA_FK_NDCP_CODE ORDER BY ONPA_DCTVT_DTTM DESC)) = 1
										),
FILTER_ONLC                           as ( SELECT * from    RENAME_ONLC 
				WHERE ONLC_EFCTV_DATE <= CURRENT_DATE AND ONLC_ENDNG_DATE >= CURRENT_DATE
		QUALIFY (ROW_NUMBER() OVER (PARTITION BY ONLC_FK_NDCL_CODE,ONLC_FK_DCPR_CODE,ONLC_FK_NDCP_CODE ORDER BY ONLC_DCTVT_DTTM DESC)) = 1
										),
FILTER_OTC                            as ( SELECT * from    RENAME_OTC 
				WHERE OTC_EFCTV_DATE <= CURRENT_DATE AND OTC_ENDNG_DATE >= CURRENT_DATE
		QUALIFY (ROW_NUMBER() OVER (PARTITION BY OTC_FK_NDCL_CODE,OTC_FK_DCPR_CODE,OTC_FK_NDCP_CODE ORDER BY OTC_DCTVT_DTTM DESC)) = 1
										),
FILTER_MNDC                           as ( SELECT * from    RENAME_MNDC 
				WHERE MNDC_EFCTV_DATE <= CURRENT_DATE AND MNDC_ENDNG_DATE >= CURRENT_DATE
		QUALIFY (ROW_NUMBER() OVER (PARTITION BY MNDC_FK_NDCL_CODE,MNDC_FK_DCPR_CODE,MNDC_FK_NDCP_CODE ORDER BY MNDC_DCTVT_DTTM DESC)) = 1
										),
FILTER_NDC_PRCNG                      as ( SELECT * from    RENAME_NDC_PRCNG 
				WHERE FK_NDPT_CODE = 'A' AND NDC_PRCNG_EFCTV_DATE <= CURRENT_DATE AND NDC_PRCNG_ENDNG_DATE >= CURRENT_DATE 
		QUALIFY (ROW_NUMBER () OVER (PARTITION BY NDC_PRCNG_FK_NDCL_CODE,NDC_PRCNG_FK_DCPR_CODE,NDC_PRCNG_FK_NDCP_CODE,NDC_PRCNG_FK_NDCV_NMBR ORDER BY NDC_PRCNG_EFCTV_DATE DESC,NDC_PRCNG_ENDNG_DATE DESC))=1
										),
FILTER_MINV                           as ( SELECT * from    RENAME_MINV 
				WHERE MINV_EFCTV_DATE <= CURRENT_DATE AND MINV_ENDNG_DATE >= CURRENT_DATE
		QUALIFY (ROW_NUMBER () OVER (PARTITION BY MINV_FK_NDCL_CODE,MINV_FK_DCPR_CODE,MINV_FK_NDCP_CODE ORDER BY MINV_FK_NDCV_NMBR DESC, MINV_DCTVT_DTTM DESC)) = 1
										),
FILTER_MNMN                           as ( SELECT * from    RENAME_MNMN 
				WHERE MNMN_EFCTV_DATE <= CURRENT_DATE AND MNMN_ENDNG_DATE >= CURRENT_DATE
		QUALIFY (ROW_NUMBER () OVER (PARTITION BY MNMN_FK_MLBI_NMBR ORDER BY MNMN_DCTVT_DTTM DESC)) = 1
										),
FILTER_NDC_VRSN                       as ( SELECT * from    RENAME_NDC_VRSN 
				WHERE NDC_VRSN_EFCTV_DATE <= CURRENT_DATE AND NDC_VRSN_ENDNG_DATE >= CURRENT_DATE
		QUALIFY (ROW_NUMBER () OVER (PARTITION BY NDC_VRSN_FK_NDCL_CODE,NDC_VRSN_FK_DCPR_CODE,NDC_VRSN_FK_NDCP_CODE,NDC_VRSN_FK_NDCV_NMBR ORDER BY NDC_VRSN_DCTVT_DTTM DESC))=1
										),
FILTER_DSCRPTR                        as ( SELECT * from    RENAME_DSCRPTR 
				WHERE DSCRPTR_EFCTV_DATE <= CURRENT_DATE AND DSCRPTR_ENDNG_DATE >= CURRENT_DATE 
		QUALIFY (ROW_NUMBER () OVER (PARTITION BY DSCRPTR_FK_MDDI_NMBR ORDER BY DSCRPTR_DCTVT_DTTM DESC))=1
										 ),
FILTER_MGMD                           as ( SELECT * from    RENAME_MGMD 
                                          
--				WHERE GPI_14_EFFECTIVE_DATE <= CURRENT_DATE AND NVL(GPI_14_ENDING_DATE, CURRENT_DATE) >= CURRENT_DATE
		QUALIFY(ROW_NUMBER() OVER (PARTITION BY MGMD_FK_MDDI_NMBR order by  MGMD_DCTVT_DTTM DESC,GPI_14_EFFECTIVE_DATE DESC, NVL(GPI_14_ENDING_DATE, CURRENT_DATE) DESC ))=1
										),
FILTER_DCR                            as ( SELECT * from    RENAME_DCR   ),
FILTER_MEQ                            as ( SELECT * from    RENAME_MEQ   ),
FILTER_DGPI2                          as ( SELECT * from    RENAME_DGPI2 
		QUALIFY(ROW_NUMBER () OVER (PARTITION BY DGPI2_DGPI_CD ORDER BY DGPI2_DGPI_EFF_DT DESC) )=1
										 ),
FILTER_DGPI4                          as ( SELECT * from    RENAME_DGPI4   
		QUALIFY(ROW_NUMBER () OVER (PARTITION BY DGPI4_DGPI_CD ORDER BY DGPI4_DGPI_EFF_DT DESC) )=1
										 ),
FILTER_DGPI6                          as ( SELECT * from    RENAME_DGPI6   
		QUALIFY(ROW_NUMBER () OVER (PARTITION BY DGPI6_DGPI_CD ORDER BY DGPI6_DGPI_EFF_DT DESC) )=1
										 ),
FILTER_DGPI8                          as ( SELECT * from    RENAME_DGPI8   
		QUALIFY(ROW_NUMBER () OVER (PARTITION BY DGPI8_DGPI_CD ORDER BY DGPI8_DGPI_EFF_DT DESC ))=1
										 ),
FILTER_DGPI10                         as ( SELECT * from    RENAME_DGPI10   
		QUALIFY(ROW_NUMBER () OVER (PARTITION BY DGPI10_DGPI_CD ORDER BY DGPI10_DGPI_EFF_DT DESC))=1
										 ),
FILTER_DGPI12                         as ( SELECT * from    RENAME_DGPI12   
		QUALIFY( ROW_NUMBER () OVER (PARTITION BY DGPI12_DGPI_CD ORDER BY DGPI12_DGPI_EFF_DT DESC))=1
										 ),
FILTER_DGPI14                         as ( SELECT * from    RENAME_DGPI14   
		QUALIFY( ROW_NUMBER () OVER (PARTITION BY DGPI14_DGPI_CD ORDER BY DGPI14_DGPI_EFF_DT DESC))=1
										 ),

----JOIN LAYER----
MGMD as ( SELECT * 
			from  FILTER_MGMD
				LEFT JOIN FILTER_DCR ON FILTER_MGMD.GPI_4_CLASS_CODE = FILTER_DCR.MEDISPAN_4_GPI_CLASS_CODE 
				LEFT JOIN FILTER_DGPI2 ON FILTER_MGMD.GPI_2_GROUP_CODE = FILTER_DGPI2.DGPI2_DGPI_CD 
				LEFT JOIN FILTER_DGPI4 ON FILTER_MGMD.GPI_4_CLASS_CODE = FILTER_DGPI4.DGPI4_DGPI_CD 
				LEFT JOIN FILTER_DGPI6 ON FILTER_MGMD.GPI_6_SUBCLASS_CODE = FILTER_DGPI6.DGPI6_DGPI_CD 
				LEFT JOIN FILTER_DGPI8 ON FILTER_MGMD.GPI_8_DRUG_NAME_CODE = FILTER_DGPI8.DGPI8_DGPI_CD 
				LEFT JOIN FILTER_DGPI10 ON FILTER_MGMD.GPI_10_DRUG_NAME_EXT_CODE = FILTER_DGPI10.DGPI10_DGPI_CD 
				LEFT JOIN FILTER_DGPI12 ON FILTER_MGMD.GPI_12_DRUG_DOSAGE_FORM_CODE = FILTER_DGPI12.DGPI12_DGPI_CD 
				LEFT JOIN FILTER_DGPI14 ON FILTER_MGMD.GPI_14_CODE = FILTER_DGPI14.DGPI14_DGPI_CD  ),
NDC_VRSN as ( SELECT * 
			from  FILTER_NDC_VRSN
				LEFT JOIN FILTER_DSCRPTR ON FILTER_NDC_VRSN.DRUG_DESCRIPTOR_IDENTIFIER = FILTER_DSCRPTR.DSCRPTR_FK_MDDI_NMBR 
				LEFT JOIN MGMD ON FILTER_NDC_VRSN.DRUG_DESCRIPTOR_IDENTIFIER = MGMD.MGMD_FK_MDDI_NMBR  
				LEFT JOIN FILTER_MEQ ON MGMD.GPI_14_CODE = FILTER_MEQ.MEDISPAN_14_GPI_CODE AND FILTER_DSCRPTR.ROUTE_OF_ADMINISTRATION_CODE = FILTER_MEQ.DRUG_ROUTE_CODE
					AND FILTER_DSCRPTR.DRUG_STRENGTH_CODE = FILTER_MEQ.MEQ_DRUG_STRENGTH_CODE AND FILTER_DSCRPTR.STRENGTH_MEASURE_CODE = FILTER_MEQ.DRUG_STRENGTH_MEASURE_CODE),
MINV as ( SELECT * 
			from  FILTER_MINV
				LEFT JOIN FILTER_MNMN ON FILTER_MINV.MINV_FK_MLBI_NMBR = FILTER_MNMN.MNMN_FK_MLBI_NMBR 
				 ),
NDC as ( SELECT * 
			from  FILTER_NDC
				LEFT JOIN FILTER_PCKG ON FILTER_NDC.NDC_5_LABELER_CODE = FILTER_PCKG.PCKG_FK_NDCL_CODE AND FILTER_NDC.NDC_4_PRODUCT_CODE = FILTER_PCKG.PCKG_FK_DCPR_CODE
					AND FILTER_NDC.NDC_2_PACKAGE_CODE = FILTER_PCKG.PCKG_FK_NDCP_CODE AND FILTER_NDC.NDC_VERSION_NUMBER = FILTER_PCKG.PCKG_FK_NDCV_NMBR
				LEFT JOIN FILTER_FRMLRY ON FILTER_NDC.NDC_5_LABELER_CODE = FILTER_FRMLRY.FRMLRY_FK_NDCL_CODE AND FILTER_NDC.NDC_4_PRODUCT_CODE = FILTER_FRMLRY.FRMLRY_FK_DCPR_CODE 
					AND FILTER_NDC.NDC_2_PACKAGE_CODE = FILTER_FRMLRY.FRMLRY_FK_NDCP_CODE AND FILTER_NDC.NDC_VERSION_NUMBER = FILTER_FRMLRY.FRMLRY_FK_NDCV_NMBR
				LEFT JOIN FILTER_ONPA ON FILTER_NDC.NDC_5_LABELER_CODE = FILTER_ONPA.ONPA_FK_NDCL_CODE AND FILTER_NDC.NDC_4_PRODUCT_CODE = FILTER_ONPA.ONPA_FK_DCPR_CODE 
					AND FILTER_NDC.NDC_2_PACKAGE_CODE = FILTER_ONPA.ONPA_FK_NDCP_CODE AND FILTER_NDC.NDC_VERSION_NUMBER = FILTER_ONPA.ONPA_FK_NDCV_NMBR
				LEFT JOIN FILTER_ONLC ON FILTER_NDC.NDC_5_LABELER_CODE = FILTER_ONLC.ONLC_FK_NDCL_CODE AND FILTER_NDC.NDC_4_PRODUCT_CODE = FILTER_ONLC.ONLC_FK_DCPR_CODE 
					AND FILTER_NDC.NDC_2_PACKAGE_CODE = FILTER_ONLC.ONLC_FK_NDCP_CODE AND FILTER_NDC.NDC_VERSION_NUMBER = FILTER_ONLC.ONLC_FK_NDCV_NMBR
				LEFT JOIN FILTER_OTC ON FILTER_NDC.NDC_5_LABELER_CODE = FILTER_OTC.OTC_FK_NDCL_CODE AND FILTER_NDC.NDC_4_PRODUCT_CODE = FILTER_OTC.OTC_FK_DCPR_CODE 
					AND FILTER_NDC.NDC_2_PACKAGE_CODE = FILTER_OTC.OTC_FK_NDCP_CODE AND FILTER_NDC.NDC_VERSION_NUMBER = FILTER_OTC.OTC_FK_NDCV_NMBR
				LEFT JOIN FILTER_MNDC ON FILTER_NDC.NDC_5_LABELER_CODE = FILTER_MNDC.MNDC_FK_NDCL_CODE AND FILTER_NDC.NDC_4_PRODUCT_CODE = FILTER_MNDC.MNDC_FK_DCPR_CODE 
					AND FILTER_NDC.NDC_2_PACKAGE_CODE = FILTER_MNDC.MNDC_FK_NDCP_CODE AND FILTER_NDC.NDC_VERSION_NUMBER = FILTER_MNDC.MNDC_FK_NDCV_NMBR
				LEFT JOIN FILTER_NDC_PRCNG ON FILTER_NDC.NDC_5_LABELER_CODE = FILTER_NDC_PRCNG.NDC_PRCNG_FK_NDCL_CODE AND FILTER_NDC.NDC_4_PRODUCT_CODE = FILTER_NDC_PRCNG.NDC_PRCNG_FK_DCPR_CODE
					AND FILTER_NDC.NDC_2_PACKAGE_CODE = FILTER_NDC_PRCNG.NDC_PRCNG_FK_NDCP_CODE AND FILTER_NDC.NDC_VERSION_NUMBER = FILTER_NDC_PRCNG.NDC_PRCNG_FK_NDCV_NMBR 
				LEFT JOIN MINV ON FILTER_NDC.NDC_5_LABELER_CODE = MINV.MINV_FK_NDCL_CODE AND FILTER_NDC.NDC_4_PRODUCT_CODE = MINV.MINV_FK_DCPR_CODE 
					AND FILTER_NDC.NDC_2_PACKAGE_CODE = MINV.MINV_FK_NDCP_CODE AND FILTER_NDC.NDC_VERSION_NUMBER = MINV.MINV_FK_NDCV_NMBR
				LEFT JOIN NDC_VRSN ON FILTER_NDC.NDC_5_LABELER_CODE = NDC_VRSN.NDC_VRSN_FK_NDCL_CODE AND FILTER_NDC.NDC_4_PRODUCT_CODE = NDC_VRSN.NDC_VRSN_FK_DCPR_CODE
					AND FILTER_NDC.NDC_2_PACKAGE_CODE = NDC_VRSN.NDC_VRSN_FK_NDCP_CODE AND FILTER_NDC.NDC_VERSION_NUMBER = NDC_VRSN.NDC_VRSN_FK_NDCV_NMBR ),
-----ETL LAYER----
ETL1 as (
	select 
		md5(cast(
    
    coalesce(cast(NDC_11_CODE as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
		, NDC_11_CODE
		, NDC_11_FORMATTED_CODE
		, NDC_5_LABELER_CODE
		, NDC_5_LABELER_NAME
		, DRUG_MANUFACTURER_ABREVIATED_NAME
		, MULTI_SOURCE_PRODUCT_CODE
		, MULTI_SOURCE_PRODUCT_NAME
		, NDC_4_PRODUCT_CODE
		, NDC_4_PRODUCT_DESC
		, NDC_2_PACKAGE_CODE
		, '03' AS DRUG_TYPE_CODE
		, 'NATIONAL DRUG CODE' AS DRUG_TYPE_DESC
		, GPI_14_CODE
		, GPI_14_DESC
		, GPI_14_EFFECTIVE_DATE
		, NULLIF(GPI_14_ENDING_DATE , '3000-01-01') AS GPI_14_ENDING_DATE 
		, GPI_2_GROUP_CODE
		, GPI_2_GROUP_DESC
		, GPI_4_CLASS_CODE
		, GPI_4_CLASS_DESC
		, NDC_VERSION_NUMBER
		, NDC_BEGINNING_DATE
		, NDC_ENDING_DATE
		, CASE WHEN NDC_ENDING_DATE IS NULL THEN 'Y' ELSE 'N' END AS NDC_ACTIVE_IND
		, DRUG_DESCRIPTOR_IDENTIFIER
		, GPI_6_SUBCLASS_CODE
		, GPI_6_SUBCLASS_DESC
		, GPI_8_DRUG_NAME_CODE
		, GPI_8_DRUG_NAME_DESC
		, GPI_10_DRUG_NAME_EXT_CODE
		, GPI_10_DRUG_NAME_EXT_DESC
		, GPI_12_DRUG_DOSAGE_FORM_CODE
		, GPI_12_DRUG_DOSAGE_FORM_DESC
		, BWC_FORMULARY_PRIOR_AUTHORIZATION_IND
		, BWC_FORMULARY_COVERAGE_CODE
		, BWC_FORMULARY_COVERAGE_DESC
		, BWC_FORMULARY_COVERAGE_LIMITATION_DESC
		, OVER_THE_COUNTER_CODE
		, OVER_THE_COUNTER_DESC
		, DEA_DRUG_SCHEDULE_CODE
		, DEA_DRUG_SCHEDULE_DESC
		, DRUG_STRENGTH_CODE
		, STRENGTH_MEASURE_CODE
		, ROUTE_OF_ADMINISTRATION_CODE
		, ROUTE_OF_ADMINISTRATION_DESC
		, DOSAGE_FORM_CODE
		, DOSAGE_FORM_DESC
		, PACKAGE_SIZE_UNIT_OF_MEASURE_CODE
		, PACKAGE_SIZE_UNIT_OF_MEASURE_DESC
		, PACKAGE_TYPE_CODE
		, PACKAGE_TYPE_DESC
		, PACKAGE_DOSE_CODE
		, PACKAGE_DOSE_DESC
		, GPI_PAYMENT_CATEGORY_DESC
		, MED_CONVERSION_FACTOR::NUMERIC(31,4) AS MED_CONVERSION_FACTOR
		, AED_CONVERSION_FACTOR::NUMERIC(31,4) AS AED_CONVERSION_FACTOR
		, CURRENT_AWP_DRUG_AMOUNT::NUMERIC(32,5) AS CURRENT_AWP_DRUG_AMOUNT
		, MILLIGRAM_EQUIVALENCE_PER_UNIT::NUMERIC(31,4) AS MILLIGRAM_EQUIVALENCE_PER_UNIT
		, EQUIVALENT_DRUG_NAME
		, ACTIVE_DRUG_STRENGTH_QUANTITY::NUMERIC(31,4) AS ACTIVE_DRUG_STRENGTH_QUANTITY
		, PACKAGE_SIZE_QTY::NUMERIC(9,3) AS PACKAGE_SIZE_QTY
		, PACKAGE_QTY
		, DCTVT_DTTM
		, NDC_VRSN_EFCTV_DATE
		, NDC_VRSN_ENDNG_DATE
		, NDC_VRSN_FK_NDCL_CODE
		, NDC_VRSN_FK_DCPR_CODE
		, NDC_VRSN_FK_NDCP_CODE
		, NDC_VRSN_FK_NDCV_NMBR
		, NDC_VRSN_DCTVT_DTTM
		, MGMD_FK_MDDI_NMBR
		, MGMD_FK_MTDC_CODE
		, MGMD_FK_MTDS_CODE
		, MGMD_FK_MTDN_CODE
		, MGMD_FK_MDNE_CODE
		, MGMD_FK_MDFT_CODE
		, MGMD_FK_MGGN_CODE
		, MGMD_DCTVT_DTTM
		, PCKG_FK_NDCL_CODE
		, FRMLRY_FK_NDCL_CODE
		, ONPA_FK_NDCL_CODE
		, ONLC_FK_NDCL_CODE
		, OTC_FK_NDCL_CODE
		, MNDC_FK_NDCL_CODE
		, MINV_FK_NDCL_CODE
		, MINV_FK_MLBI_NMBR
		, MNMN_FK_MLBI_NMBR
		, DSCRPTR_FK_MDDI_NMBR
		, DGPI2_DGPI_EFF_DT
		, DGPI4_DGPI_EFF_DT
		, DGPI6_DGPI_EFF_DT
		, DGPI8_DGPI_EFF_DT
		, DGPI10_DGPI_EFF_DT
		, DGPI12_DGPI_EFF_DT
		, DGPI14_DGPI_EFF_DT
		, DGPI2_DGPI_CD
		, DGPI4_DGPI_CD
		, DGPI6_DGPI_CD
		, DGPI8_DGPI_CD
		, DGPI10_DGPI_CD
		, DGPI12_DGPI_CD
		, DGPI14_DGPI_CD
		, PCKG_FK_MGGN_CODE
		, FRMLRY_FK_DCPR_CODE
		, ONPA_FK_DCPR_CODE
		, ONLC_FK_DCPR_CODE
		, OTC_FK_DCPR_CODE
		, MNDC_FK_DCPR_CODE
		, MINV_FK_DCPR_CODE
		, PCKG_FK_DCPR_CODE
		, FRMLRY_FK_NDCP_CODE
		, ONPA_FK_NDCP_CODE
		, ONLC_FK_NDCP_CODE
		, OTC_FK_NDCP_CODE
		, MNDC_FK_NDCP_CODE
		, MINV_FK_NDCP_CODE
		, PCKG_FK_NDCP_CODE
		, FRMLRY_FK_NDCV_NMBR
		, ONPA_FK_NDCV_NMBR
		, ONLC_FK_NDCV_NMBR
		, OTC_FK_NDCV_NMBR
		, MNDC_FK_NDCV_NMBR
		, MINV_FK_NDCV_NMBR
		, PCKG_FK_NDCV_NMBR
		, PCKG_DCTVT_DTTM
		, PCKG_EFCTV_DATE
		, PCKG_ENDNG_DATE
		, FRMLRY_DCTVT_DTTM
		, FRMLRY_EFCTV_DATE
		, FRMLRY_ENDNG_DATE
		, ONPA_DCTVT_DTTM
		, ONPA_EFCTV_DATE
		, ONPA_ENDNG_DATE
		, ONLC_DCTVT_DTTM
		, ONLC_EFCTV_DATE
		, ONLC_ENDNG_DATE
		, OTC_DCTVT_DTTM
		, OTC_EFCTV_DATE
		, OTC_ENDNG_DATE
		, MNDC_DCTVT_DTTM
		, MNDC_EFCTV_DATE
		, MNDC_ENDNG_DATE
		, MINV_DCTVT_DTTM
		, MINV_EFCTV_DATE
		, MINV_ENDNG_DATE
		, MNMN_DCTVT_DTTM
		, MNMN_EFCTV_DATE
		, MNMN_ENDNG_DATE
		, DSCRPTR_DCTVT_DTTM
		, DSCRPTR_EFCTV_DATE
		, DSCRPTR_ENDNG_DATE
		, MEDISPAN_4_GPI_CLASS_CODE
		, MEQ_DRUG_STRENGTH_CODE
		, MEDISPAN_14_GPI_CODE
		, DRUG_ROUTE_CODE
		, DRUG_STRENGTH_MEASURE_CODE
		, NDC_PRCNG_FK_NDCL_CODE
		, NDC_PRCNG_FK_DCPR_CODE
		, NDC_PRCNG_FK_NDCP_CODE
		, NDC_PRCNG_FK_NDCV_NMBR
		, FK_NDPT_CODE
		, NDC_PRCNG_EFCTV_DATE
		, NDC_PRCNG_ENDNG_DATE

	from NDC)
SELECT *
			from  ETL1