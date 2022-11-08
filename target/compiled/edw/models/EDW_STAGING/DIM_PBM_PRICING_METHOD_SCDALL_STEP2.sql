----SRC LAYER----
WITH
SCD1 as ( SELECT 
md5(cast(
    
    coalesce(cast(PRICE_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(PBM_PRICING_SOURCE_CODE as 
    varchar
), '')

 as 
    varchar
)) as PBM_PRICING_METHOD_HKEY,
UNIQUE_ID_KEY,
PRICE_TYPE_CODE,
PBM_PRICING_SOURCE_CODE,
PRICE_TYPE_DESC,
PBM_PRICING_SOURCE_DESC
    from      STAGING.DSV_PBM_PRICING_METHOD )

select * from SCD1