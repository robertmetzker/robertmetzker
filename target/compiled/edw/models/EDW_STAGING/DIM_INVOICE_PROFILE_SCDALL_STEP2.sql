----SRC LAYER----
WITH
SCD1 as ( SELECT *    from      STAGING.DSV_INVOICE_PROFILE )
select * from SCD1