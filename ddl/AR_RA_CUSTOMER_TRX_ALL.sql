
  CREATE TABLE "AR"."RA_CUSTOMER_TRX_ALL" 
   (	"CUSTOMER_TRX_ID" NUMBER(15,0) NOT NULL ENABLE, 
	"LAST_UPDATE_DATE" DATE NOT NULL ENABLE, 
	"LAST_UPDATED_BY" NUMBER(15,0) NOT NULL ENABLE, 
	"CREATION_DATE" DATE NOT NULL ENABLE, 
	"CREATED_BY" NUMBER(15,0) NOT NULL ENABLE, 
	"LAST_UPDATE_LOGIN" NUMBER(15,0), 
	"TRX_NUMBER" VARCHAR2(20) NOT NULL ENABLE, 
	"CUST_TRX_TYPE_ID" NUMBER(15,0) NOT NULL ENABLE, 
	"TRX_DATE" DATE NOT NULL ENABLE, 
	"SET_OF_BOOKS_ID" NUMBER(15,0) NOT NULL ENABLE, 
	"BILL_TO_CONTACT_ID" NUMBER(15,0), 
	"BATCH_ID" NUMBER(15,0), 
	"BATCH_SOURCE_ID" NUMBER(15,0), 
	"REASON_CODE" VARCHAR2(30), 
	"SOLD_TO_CUSTOMER_ID" NUMBER(15,0), 
	"SOLD_TO_CONTACT_ID" NUMBER(15,0), 
	"SOLD_TO_SITE_USE_ID" NUMBER(15,0), 
	"BILL_TO_CUSTOMER_ID" NUMBER(15,0), 
	"BILL_TO_SITE_USE_ID" NUMBER(15,0), 
	"SHIP_TO_CUSTOMER_ID" NUMBER(15,0), 
	"SHIP_TO_CONTACT_ID" NUMBER(15,0), 
	"SHIP_TO_SITE_USE_ID" NUMBER(15,0), 
	"SHIPMENT_ID" NUMBER(15,0), 
	"REMIT_TO_ADDRESS_ID" NUMBER(15,0), 
	"TERM_ID" NUMBER(15,0), 
	"TERM_DUE_DATE" DATE, 
	"PREVIOUS_CUSTOMER_TRX_ID" NUMBER(15,0), 
	"PRIMARY_SALESREP_ID" NUMBER(15,0), 
	"PRINTING_ORIGINAL_DATE" DATE, 
	"PRINTING_LAST_PRINTED" DATE, 
	"PRINTING_OPTION" VARCHAR2(20), 
	"PRINTING_COUNT" NUMBER(15,0), 
	"PRINTING_PENDING" VARCHAR2(1), 
	"PURCHASE_ORDER" VARCHAR2(50), 
	"PURCHASE_ORDER_REVISION" VARCHAR2(50), 
	"PURCHASE_ORDER_DATE" DATE, 
	"CUSTOMER_REFERENCE" VARCHAR2(30), 
	"CUSTOMER_REFERENCE_DATE" DATE, 
	"COMMENTS" VARCHAR2(1760), 
	"INTERNAL_NOTES" VARCHAR2(240), 
	"EXCHANGE_RATE_TYPE" VARCHAR2(30), 
	"EXCHANGE_DATE" DATE, 
	"EXCHANGE_RATE" NUMBER, 
	"TERRITORY_ID" NUMBER(15,0), 
	"INVOICE_CURRENCY_CODE" VARCHAR2(15), 
	"INITIAL_CUSTOMER_TRX_ID" NUMBER(15,0), 
	"AGREEMENT_ID" NUMBER(15,0), 
	"END_DATE_COMMITMENT" DATE, 
	"START_DATE_COMMITMENT" DATE, 
	"LAST_PRINTED_SEQUENCE_NUM" NUMBER(15,0), 
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
	"ORIG_SYSTEM_BATCH_NAME" VARCHAR2(40), 
	"POST_REQUEST_ID" NUMBER(15,0), 
	"REQUEST_ID" NUMBER(15,0), 
	"PROGRAM_APPLICATION_ID" NUMBER(15,0), 
	"PROGRAM_ID" NUMBER(15,0), 
	"PROGRAM_UPDATE_DATE" DATE, 
	"FINANCE_CHARGES" VARCHAR2(1), 
	"COMPLETE_FLAG" VARCHAR2(1) NOT NULL ENABLE, 
	"POSTING_CONTROL_ID" NUMBER(15,0), 
	"BILL_TO_ADDRESS_ID" NUMBER(15,0), 
	"RA_POST_LOOP_NUMBER" NUMBER(15,0), 
	"SHIP_TO_ADDRESS_ID" NUMBER(15,0), 
	"CREDIT_METHOD_FOR_RULES" VARCHAR2(30), 
	"CREDIT_METHOD_FOR_INSTALLMENTS" VARCHAR2(30), 
	"RECEIPT_METHOD_ID" NUMBER(15,0), 
	"ATTRIBUTE11" VARCHAR2(150), 
	"ATTRIBUTE12" VARCHAR2(150), 
	"ATTRIBUTE13" VARCHAR2(150), 
	"ATTRIBUTE14" VARCHAR2(150), 
	"ATTRIBUTE15" VARCHAR2(150), 
	"RELATED_CUSTOMER_TRX_ID" NUMBER(15,0), 
	"INVOICING_RULE_ID" NUMBER(15,0), 
	"SHIP_VIA" VARCHAR2(30), 
	"SHIP_DATE_ACTUAL" DATE, 
	"WAYBILL_NUMBER" VARCHAR2(50), 
	"FOB_POINT" VARCHAR2(30), 
	"CUSTOMER_BANK_ACCOUNT_ID" NUMBER(15,0), 
	"INTERFACE_HEADER_ATTRIBUTE1" VARCHAR2(150), 
	"INTERFACE_HEADER_ATTRIBUTE2" VARCHAR2(150), 
	"INTERFACE_HEADER_ATTRIBUTE3" VARCHAR2(150), 
	"INTERFACE_HEADER_ATTRIBUTE4" VARCHAR2(150), 
	"INTERFACE_HEADER_ATTRIBUTE5" VARCHAR2(150), 
	"INTERFACE_HEADER_ATTRIBUTE6" VARCHAR2(150), 
	"INTERFACE_HEADER_ATTRIBUTE7" VARCHAR2(150), 
	"INTERFACE_HEADER_ATTRIBUTE8" VARCHAR2(150), 
	"INTERFACE_HEADER_CONTEXT" VARCHAR2(30), 
	"DEFAULT_USSGL_TRX_CODE_CONTEXT" VARCHAR2(30), 
	"INTERFACE_HEADER_ATTRIBUTE10" VARCHAR2(150), 
	"INTERFACE_HEADER_ATTRIBUTE11" VARCHAR2(150), 
	"INTERFACE_HEADER_ATTRIBUTE12" VARCHAR2(150), 
	"INTERFACE_HEADER_ATTRIBUTE13" VARCHAR2(150), 
	"INTERFACE_HEADER_ATTRIBUTE14" VARCHAR2(150), 
	"INTERFACE_HEADER_ATTRIBUTE15" VARCHAR2(150), 
	"INTERFACE_HEADER_ATTRIBUTE9" VARCHAR2(150), 
	"DEFAULT_USSGL_TRANSACTION_CODE" VARCHAR2(30), 
	"RECURRED_FROM_TRX_NUMBER" VARCHAR2(20), 
	"STATUS_TRX" VARCHAR2(30), 
	"DOC_SEQUENCE_ID" NUMBER(15,0), 
	"DOC_SEQUENCE_VALUE" NUMBER(15,0), 
	"PAYING_CUSTOMER_ID" NUMBER(15,0), 
	"PAYING_SITE_USE_ID" NUMBER(15,0), 
	"RELATED_BATCH_SOURCE_ID" NUMBER(15,0), 
	"DEFAULT_TAX_EXEMPT_FLAG" VARCHAR2(1), 
	"CREATED_FROM" VARCHAR2(30) NOT NULL ENABLE, 
	"ORG_ID" NUMBER(15,0) DEFAULT NULL, 
	"WH_UPDATE_DATE" DATE, 
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
	"GLOBAL_ATTRIBUTE_CATEGORY" VARCHAR2(30), 
	"EDI_PROCESSED_FLAG" VARCHAR2(1), 
	"EDI_PROCESSED_STATUS" VARCHAR2(10), 
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
	"MRC_EXCHANGE_RATE_TYPE" VARCHAR2(2000), 
	"MRC_EXCHANGE_DATE" VARCHAR2(2000), 
	"MRC_EXCHANGE_RATE" VARCHAR2(2000), 
	"PAYMENT_SERVER_ORDER_NUM" VARCHAR2(80), 
	"APPROVAL_CODE" VARCHAR2(80), 
	"ADDRESS_VERIFICATION_CODE" VARCHAR2(80), 
	"OLD_TRX_NUMBER" VARCHAR2(20), 
	"BR_AMOUNT" NUMBER, 
	"BR_UNPAID_FLAG" VARCHAR2(1), 
	"BR_ON_HOLD_FLAG" VARCHAR2(1), 
	"DRAWEE_ID" NUMBER(15,0), 
	"DRAWEE_CONTACT_ID" NUMBER(15,0), 
	"DRAWEE_SITE_USE_ID" NUMBER(15,0), 
	"REMITTANCE_BANK_ACCOUNT_ID" NUMBER(15,0), 
	"OVERRIDE_REMIT_ACCOUNT_FLAG" VARCHAR2(1), 
	"DRAWEE_BANK_ACCOUNT_ID" NUMBER(15,0), 
	"SPECIAL_INSTRUCTIONS" VARCHAR2(240), 
	"REMITTANCE_BATCH_ID" NUMBER(15,0), 
	"PREPAYMENT_FLAG" VARCHAR2(1), 
	"CT_REFERENCE" VARCHAR2(150), 
	"CONTRACT_ID" NUMBER, 
	"BILL_TEMPLATE_ID" NUMBER(15,0), 
	"REVERSED_CASH_RECEIPT_ID" NUMBER(15,0), 
	"CC_ERROR_CODE" VARCHAR2(80), 
	"CC_ERROR_TEXT" VARCHAR2(255), 
	"CC_ERROR_FLAG" VARCHAR2(1), 
	"MANDATE_LAST_TRX_FLAG" VARCHAR2(2), 
	"UPGRADE_METHOD" VARCHAR2(30), 
	"LEGAL_ENTITY_ID" NUMBER(15,0), 
	"REMIT_BANK_ACCT_USE_ID" NUMBER(15,0), 
	"PAYMENT_TRXN_EXTENSION_ID" NUMBER(15,0), 
	"AX_ACCOUNTED_FLAG" VARCHAR2(1), 
	"APPLICATION_ID" NUMBER(15,0), 
	"PAYMENT_ATTRIBUTES" VARCHAR2(1000), 
	"BILLING_DATE" DATE, 
	"INTEREST_HEADER_ID" NUMBER(15,0), 
	"LATE_CHARGES_ASSESSED" VARCHAR2(30), 
	"TRAILER_NUMBER" VARCHAR2(50), 
	"REV_REC_APPLICATION" VARCHAR2(30), 
	"DOCUMENT_TYPE_ID" NUMBER(18,0), 
	"DOCUMENT_CREATION_DATE" DATE, 
	"SRC_INVOICING_RULE_ID" NUMBER(15,0), 
	"BILLING_EXT_REQUEST" NUMBER, 
	 SUPPLEMENTAL LOG DATA (ALL) COLUMNS
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 60 INITRANS 10 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 251658240 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "APPS_TS_TX_DATA" 