CREATE TABLE dbo.IV00102 (
	ITEMNMBR CHAR(31),
	LOCNCODE CHAR(11),
	BINNMBR CHAR(21),
	RCRDTYPE SMALLINT(5),
	PRIMVNDR CHAR(15),
	ITMFRFLG TINYINT(3),
	BGNGQTY NUMERIC(19,5),
	LSORDQTY NUMERIC(19,5),
	LRCPTQTY NUMERIC(19,5),
	LSTORDDT TIMESTAMP,
	LSORDVND CHAR(15),
	LSRCPTDT TIMESTAMP,
	QTYRQSTN NUMERIC(19,5),
	QTYONORD NUMERIC(19,5),
	QTYBKORD NUMERIC(19,5),
	QTY_DROP_SHIPPED NUMERIC(19,5),
	QTYINUSE NUMERIC(19,5),
	QTYINSVC NUMERIC(19,5),
	QTYRTRND NUMERIC(19,5),
	QTYDMGED NUMERIC(19,5),
	QTYONHND NUMERIC(19,5),
	ATYALLOC NUMERIC(19,5),
	QTYCOMTD NUMERIC(19,5),
	QTYSOLD NUMERIC(19,5),
	NXTCNTDT TIMESTAMP,
	NXTCNTTM TIMESTAMP,
	LSTCNTDT TIMESTAMP,
	LSTCNTTM TIMESTAMP,
	STCKCNTINTRVL SMALLINT(5),
	LANDED_COST_GROUP_ID CHAR(15),
	BUYERID CHAR(15),
	PLANNERID CHAR(15),
	ORDERPOLICY SMALLINT(5),
	FXDORDRQTY NUMERIC(19,5),
	ORDRPNTQTY NUMERIC(19,5),
	NMBROFDYS SMALLINT(5),
	MNMMORDRQTY NUMERIC(19,5),
	MXMMORDRQTY NUMERIC(19,5),
	ORDERMULTIPLE NUMERIC(19,5),
	REPLENISHMENTMETHOD SMALLINT(5),
	SHRINKAGEFACTOR NUMERIC(19,5),
	PRCHSNGLDTM NUMERIC(19,5),
	MNFCTRNGFXDLDTM NUMERIC(19,5),
	MNFCTRNGVRBLLDTM NUMERIC(19,5),
	STAGINGLDTME NUMERIC(19,5),
	PLNNNGTMFNCDYS SMALLINT(5),
	DMNDTMFNCPRDS SMALLINT(5),
	INCLDDINPLNNNG TINYINT(3),
	CALCULATEATP TINYINT(3),
	AUTOCHKATP TINYINT(3),
	PLNFNLPAB TINYINT(3),
	FRCSTCNSMPTNPRD SMALLINT(5),
	ORDRUPTOLVL NUMERIC(19,5),
	SFTYSTCKQTY NUMERIC(19,5),
	REORDERVARIANCE NUMERIC(19,5),
	PORECEIPTBIN CHAR(15),
	PORETRNBIN CHAR(15),
	SOFULFILLMENTBIN CHAR(15),
	SORETURNBIN CHAR(15),
	BOMRCPTBIN CHAR(15),
	MATERIALISSUEBIN CHAR(15),
	MORECEIPTBIN CHAR(15),
	REPAIRISSUESBIN CHAR(15),
	REPLENISHMENTLEVEL SMALLINT(5),
	POPORDERMETHOD SMALLINT(5),
	MASTERLOCATIONCODE CHAR(11),
	POPVENDORSELECTION SMALLINT(5),
	POPPRICINGSELECTION SMALLINT(5),
	PURCHASEPRICE NUMERIC(19,5),
	INCLUDEALLOCATIONS TINYINT(3),
	INCLUDEBACKORDERS TINYINT(3),
	INCLUDEREQUISITIONS TINYINT(3),
	PICKTICKETITEMOPT SMALLINT(5),
	DEX_ROW_ID INT(10)
);