
  CREATE TABLE "INV"."MTL_INTERCOMPANY_PARAMETERS" 
   (	"SHIP_ORGANIZATION_ID" NUMBER NOT NULL ENABLE, 
	"SELL_ORGANIZATION_ID" NUMBER NOT NULL ENABLE, 
	"LAST_UPDATE_DATE" DATE NOT NULL ENABLE, 
	"LAST_UPDATED_BY" NUMBER NOT NULL ENABLE, 
	"CREATION_DATE" DATE NOT NULL ENABLE, 
	"CREATED_BY" NUMBER NOT NULL ENABLE, 
	"LAST_UPDATE_LOGIN" NUMBER, 
	"CUSTOMER_ID" NUMBER NOT NULL ENABLE, 
	"ADDRESS_ID" NUMBER NOT NULL ENABLE, 
	"CUSTOMER_SITE_ID" NUMBER NOT NULL ENABLE, 
	"CUST_TRX_TYPE_ID" NUMBER NOT NULL ENABLE, 
	"VENDOR_ID" NUMBER, 
	"VENDOR_SITE_ID" NUMBER, 
	"REVALUE_AVERAGE_FLAG" VARCHAR2(1), 
	"FREIGHT_CODE_COMBINATION_ID" NUMBER, 
	"ATTRIBUTE_CATEGORY" VARCHAR2(30), 
	"ATTRIBUTE1" VARCHAR2(150), 
	"ATTRIBUTE2" VARCHAR2(150), 
	"ATTRIBUTE3" VARCHAR2(150), 
	"ATTRIBUTE4" VARCHAR2(150), 
	"ATTRIBUTE5" VARCHAR2(150), 
	"ATTRIBUTE6" VARCHAR2(150), 
	"ATTRIBUTE7" VARCHAR2(150), 
	"ATTRIBUTE8" VARCHAR2(150), 
	"ATTRIBUTE9" VARCHAR2(150), 
	"ATTRIBUTE10" VARCHAR2(150), 
	"ATTRIBUTE11" VARCHAR2(150), 
	"ATTRIBUTE12" VARCHAR2(150), 
	"ATTRIBUTE13" VARCHAR2(150), 
	"ATTRIBUTE14" VARCHAR2(150), 
	"ATTRIBUTE15" VARCHAR2(150), 
	"FLOW_TYPE" NUMBER, 
	"INVENTORY_ACCRUAL_ACCOUNT_ID" NUMBER, 
	"EXPENSE_ACCRUAL_ACCOUNT_ID" NUMBER, 
	"INTERCOMPANY_COGS_ACCOUNT_ID" NUMBER, 
	"INV_CURRENCY_CODE" NUMBER
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 70 INITRANS 10 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 49152 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "APPS_TS_SEED" 
   CACHE 