

      create or replace  table DEV_EDW.EDW_STAGING.DIM_INVOICE_PROFILE_SCDALL_STEP2  as
      (----SRC LAYER----
WITH
SCD1 as ( SELECT *    from      STAGING.DSV_INVOICE_PROFILE )
select * from SCD1
      );
    