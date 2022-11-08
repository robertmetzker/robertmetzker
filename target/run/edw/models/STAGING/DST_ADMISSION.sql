

      create or replace  table DEV_EDW.STAGING.DST_ADMISSION  as
      (----SRC LAYER----
WITH
SRC_INV as ( SELECT *     from      STAGING.STG_INVOICE_HEADER ),
SRC_REF as ( SELECT *     from      STAGING.STG_CAM_REF )
/* SRC_TOA as ( SELECT *     from      STAGING.STG_CAM_REF ), */
/* SRC_SOA as ( SELECT *     from      STAGING.STG_CAM_REF ), */
/* SRC_DIS as ( SELECT *     from      STAGING.STG_CAM_REF ), */
/* SRC_TOB as ( SELECT *     from      STAGING.STG_CAM_REF ) */

-- SRC_INV as ( SELECT *     from      STG_INVOICE_HEADER),
-- SRC_REF as ( SELECT *     from      STG_CAM_REF),
/* 
	//SRC_TOA as ( SELECT *     from      STG_CAM_REF),
	//SRC_SOA as ( SELECT *     from      STG_CAM_REF),
	//SRC_DIS as ( SELECT *     from      STG_CAM_REF),
	//SRC_TOB as ( SELECT *     from      STG_CAM_REF) 
*/
----LOGIC LAYER----
,
LOGIC_INV as ( SELECT 
		md5(cast(
    
    coalesce(cast(ADMISSION_TYPE as 
    varchar
), '') || '-' || coalesce(cast(ADMISSION_SOURCE as 
    varchar
), '') || '-' || coalesce(cast(DISCHARGE_STATUS as 
    varchar
), '') || '-' || coalesce(cast(BILL_TYPE as 
    varchar
), '')

 as 
    varchar
))   as  UNIQUE_ID_KEY ,
		ADMISSION_TYPE AS ADMISSION_TYPE,
		ADMISSION_SOURCE AS ADMISSION_SOURCE,
		DISCHARGE_STATUS AS DISCHARGE_STATUS ,
		BILL_TYPE AS BILL_TYPE
				from SRC_INV
            ),
LOGIC_TOA as ( SELECT 
		REF_IDN AS REF_IDN,
		REF_DGN AS REF_DGN,
		REF_DSC AS REF_DSC 
				from SRC_REF  --prev SRC_TOA
            ),
LOGIC_SOA as ( SELECT 
		REF_IDN AS REF_IDN,
		REF_DGN AS REF_DGN,
		REF_DSC AS REF_DSC 
				from SRC_REF  --prev SRC_SOA
            ),
LOGIC_DIS as ( SELECT 
		REF_IDN AS REF_IDN,
		REF_DGN AS REF_DGN,
		REF_DSC AS REF_DSC 
				from SRC_REF  --prev SRC_DIS
            ),
LOGIC_TOB as ( SELECT 
		REF_IDN AS REF_IDN,
		REF_DGN AS REF_DGN,
		REF_DSC AS REF_DSC 
				from SRC_REF  --prev SRC_TOB
            )
----RENAME LAYER ----
,
RENAME_INV as ( SELECT 
		 UNIQUE_ID_KEY,
		 ADMISSION_TYPE AS ADMISSION_TYPE,
		 ADMISSION_SOURCE AS ADMISSION_SOURCE,
		 DISCHARGE_STATUS AS DISCHARGE_STATUS ,
		 BILL_TYPE AS BILL_TYPE
			from      LOGIC_INV
        ),
RENAME_TOA as ( SELECT 
			REF_IDN AS TOA_REF_IDN,
			REF_DGN AS TOA_REF_DGN,
			REF_DSC AS ADMISSION_TYPE_DESC 
			from      LOGIC_TOA
        ),
RENAME_SOA as ( SELECT 
			REF_IDN AS SOA_REF_IDN,
			REF_DGN AS SOA_REF_DGN,
			REF_DSC AS ADMISSION_SOURCE_DESC 
			from      LOGIC_SOA
        ),
RENAME_DIS as ( SELECT 
			REF_IDN AS DIS_REF_IDN,
			REF_DGN AS DIS_REF_DGN,
			REF_DSC AS DISCHARGE_STATUS_DESC 
			from      LOGIC_DIS
        ),
RENAME_TOB as ( SELECT 
			REF_IDN AS TOB_REF_IDN,
			REF_DGN AS TOB_REF_DGN,
			REF_DSC AS BILL_TYPE_DESC 
			from      LOGIC_TOB
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_INV as ( SELECT  * 
			from     RENAME_INV 
            
        ),

        FILTER_TOA as ( SELECT  * 
			from     RENAME_TOA 
            WHERE TOA_REF_DGN = 'TOA'
        ),

        FILTER_SOA as ( SELECT  * 
			from     RENAME_SOA 
            WHERE SOA_REF_DGN = 'SOA'
        ),

        FILTER_DIS as ( SELECT  * 
			from     RENAME_DIS 
            WHERE DIS_REF_DGN = 'DIS'
        ),

        FILTER_TOB as ( SELECT  * 
			from     RENAME_TOB 
            WHERE TOB_REF_DGN = 'TOB'
        )
----JOIN LAYER----
,
INV as ( SELECT * 
			from  FILTER_INV
				LEFT JOIN FILTER_TOA ON FILTER_INV.ADMISSION_TYPE = FILTER_TOA.TOA_REF_IDN 
				LEFT JOIN FILTER_SOA ON FILTER_INV.ADMISSION_SOURCE = FILTER_SOA.SOA_REF_IDN 
				LEFT JOIN FILTER_DIS ON FILTER_INV.DISCHARGE_STATUS = FILTER_DIS.DIS_REF_IDN 
				LEFT JOIN FILTER_TOB ON FILTER_INV.BILL_TYPE = FILTER_TOB.TOB_REF_IDN    )
select distinct UNIQUE_ID_KEY
, ADMISSION_TYPE
, ADMISSION_SOURCE
, DISCHARGE_STATUS
, BILL_TYPE
, TOA_REF_IDN
, TOA_REF_DGN
, ADMISSION_TYPE_DESC
, SOA_REF_IDN
, SOA_REF_DGN
, ADMISSION_SOURCE_DESC
, DIS_REF_IDN
, DIS_REF_DGN
, DISCHARGE_STATUS_DESC
, TOB_REF_IDN
, TOB_REF_DGN
, BILL_TYPE_DESC
 from INV
      );
    