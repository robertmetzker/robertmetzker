

      create or replace  table DEV_EDW.STAGING.STG_CERTIFY_AG_FNCL_TRAN_DTL  as
      (---- SRC LAYER ----
WITH
SRC_AG as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_CERTIFY_AG_FNCL_TRAN_DTL ),
//SRC_AG as ( SELECT *     from     BWC_CERTIFY_AG_FNCL_TRAN_DTL) ,

---- LOGIC LAYER ----

LOGIC_AG as ( SELECT 
		  CERT_AG_FNCL_TRAN_DTL_ID                           AS                           CERT_AG_FNCL_TRAN_DTL_ID 
		, PFT_ID                                             AS                                             PFT_ID 
		, cast( CERT_AG_FNCL_TRAN_DTL_ADD_DT as DATE )       AS                       CERT_AG_FNCL_TRAN_DTL_ADD_DT 
		, cast( CERT_AG_FNCL_TRAN_DTL_UPD_DT as DATE )       AS                       CERT_AG_FNCL_TRAN_DTL_UPD_DT 
		, upper( TRIM( CERT_AG_FNCL_TRAN_DTL_GRP_ID ) )      AS                       CERT_AG_FNCL_TRAN_DTL_GRP_ID 
		from SRC_AG
            )

---- RENAME LAYER ----
,

RENAME_AG as ( SELECT 
		  CERT_AG_FNCL_TRAN_DTL_ID                           as                           CERT_AG_FNCL_TRAN_DTL_ID
		, PFT_ID                                             as                                             PFT_ID
		, CERT_AG_FNCL_TRAN_DTL_ADD_DT                       as                       CERT_AG_FNCL_TRAN_DTL_ADD_DT
		, CERT_AG_FNCL_TRAN_DTL_UPD_DT                       as                       CERT_AG_FNCL_TRAN_DTL_UPD_DT
		, CERT_AG_FNCL_TRAN_DTL_GRP_ID                       as                       CERT_AG_FNCL_TRAN_DTL_GRP_ID 
				FROM     LOGIC_AG   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_AG                             as ( SELECT * from    RENAME_AG   ),

---- JOIN LAYER ----

 JOIN_AG  as  ( SELECT * 
				FROM  FILTER_AG )
 SELECT * FROM  JOIN_AG
      );
    