
  CREATE TABLE "AR"."RA_CUST_TRX_LINE_GL_DIST_ALL" 
   (	"CUST_TRX_LINE_GL_DIST_ID" NUMBER(15,0) NOT NULL ENABLE, 
	"CUSTOMER_TRX_LINE_ID" NUMBER(15,0), 
	"CODE_COMBINATION_ID" NUMBER(15,0) NOT NULL ENABLE, 
	"SET_OF_BOOKS_ID" NUMBER(15,0) NOT NULL ENABLE, 
	"LAST_UPDATE_DATE" DATE NOT NULL ENABLE, 
	"LAST_UPDATED_BY" NUMBER(15,0) NOT NULL ENABLE, 
	"CREATION_DATE" DATE NOT NULL ENABLE, 
	"CREATED_BY" NUMBER(15,0) NOT NULL ENABLE, 
	"LAST_UPDATE_LOGIN" NUMBER(15,0), 
	"PERCENT" NUMBER, 
	"AMOUNT" NUMBER, 
	"GL_DATE" DATE, 
	"GL_POSTED_DATE" DATE, 
	"CUST_TRX_LINE_SALESREP_ID" NUMBER(15,0), 
	"COMMENTS" VARCHAR2(240), 
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
	"REQUEST_ID" NUMBER(15,0), 
	"PROGRAM_APPLICATION_ID" NUMBER(15,0), 
	"PROGRAM_ID" NUMBER(15,0), 
	"PROGRAM_UPDATE_DATE" DATE, 
	"CONCATENATED_SEGMENTS" VARCHAR2(240), 
	"ORIGINAL_GL_DATE" DATE, 
	"POST_REQUEST_ID" NUMBER(15,0), 
	"POSTING_CONTROL_ID" NUMBER(15,0) DEFAULT -3 NOT NULL ENABLE, 
	"ACCOUNT_CLASS" VARCHAR2(20) NOT NULL ENABLE, 
	"RA_POST_LOOP_NUMBER" NUMBER(15,0), 
	"CUSTOMER_TRX_ID" NUMBER(15,0) NOT NULL ENABLE, 
	"ACCOUNT_SET_FLAG" VARCHAR2(1) NOT NULL ENABLE, 
	"ACCTD_AMOUNT" NUMBER, 
	"USSGL_TRANSACTION_CODE" VARCHAR2(30), 
	"USSGL_TRANSACTION_CODE_CONTEXT" VARCHAR2(30), 
	"ATTRIBUTE11" VARCHAR2(150), 
	"ATTRIBUTE12" VARCHAR2(150), 
	"ATTRIBUTE13" VARCHAR2(150), 
	"ATTRIBUTE14" VARCHAR2(150), 
	"ATTRIBUTE15" VARCHAR2(150), 
	"LATEST_REC_FLAG" VARCHAR2(1), 
	"ORG_ID" NUMBER(15,0) DEFAULT NULL, 
	"MRC_ACCOUNT_CLASS" VARCHAR2(2000), 
	"MRC_CUSTOMER_TRX_ID" VARCHAR2(2000), 
	"MRC_AMOUNT" VARCHAR2(2000), 
	"MRC_GL_POSTED_DATE" VARCHAR2(2000), 
	"MRC_POSTING_CONTROL_ID" VARCHAR2(2000), 
	"MRC_ACCTD_AMOUNT" VARCHAR2(2000), 
	"COLLECTED_TAX_CCID" NUMBER(15,0), 
	"COLLECTED_TAX_CONCAT_SEG" VARCHAR2(240), 
	"REVENUE_ADJUSTMENT_ID" NUMBER(15,0), 
	"REV_ADJ_CLASS_TEMP" VARCHAR2(30), 
	"REC_OFFSET_FLAG" VARCHAR2(1), 
	"CCID_CHANGE_FLAG" VARCHAR2(1), 
	"EVENT_ID" NUMBER(15,0), 
	"USER_GENERATED_FLAG" VARCHAR2(1), 
	"ROUNDING_CORRECTION_FLAG" VARCHAR2(1), 
	"COGS_REQUEST_ID" NUMBER(15,0), 
	"GLOBAL_ATTRIBUTE1" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE2" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE3" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE4" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE5" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE6" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE7" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE8" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE9" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE10" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE11" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE12" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE13" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE14" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE15" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE16" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE17" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE18" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE19" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE20" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE21" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE22" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE23" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE24" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE25" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE26" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE27" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE28" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE29" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE30" VARCHAR2(150), 
	"GLOBAL_ATTRIBUTE_CATEGORY" VARCHAR2(30), 
	 SUPPLEMENTAL LOG DATA (ALL) COLUMNS
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 60 INITRANS 10 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 251658240 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "APPS_TS_TX_DATA" 