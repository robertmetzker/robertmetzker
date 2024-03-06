CREATE TABLE dbo.SOP30200 (
	SOPTYPE SMALLINT(5),
	SOPNUMBE CHAR(21),
	ORIGTYPE SMALLINT(5),
	ORIGNUMB CHAR(21),
	DOCID CHAR(15),
	DOCDATE TIMESTAMP,
	GLPOSTDT TIMESTAMP,
	QUOTEDAT TIMESTAMP,
	QUOEXPDA TIMESTAMP,
	ORDRDATE TIMESTAMP,
	INVODATE TIMESTAMP,
	BACKDATE TIMESTAMP,
	RETUDATE TIMESTAMP,
	REQSHIPDATE TIMESTAMP,
	FUFILDAT TIMESTAMP,
	ACTLSHIP TIMESTAMP,
	DISCDATE TIMESTAMP,
	DUEDATE TIMESTAMP,
	REPTING TINYINT(3),
	TRXFREQU SMALLINT(5),
	TIMEREPD SMALLINT(5),
	TIMETREP SMALLINT(5),
	DYSTINCR SMALLINT(5),
	DTLSTREP TIMESTAMP,
	DSTBTCH1 CHAR(15),
	DSTBTCH2 CHAR(15),
	USDOCID1 CHAR(15),
	USDOCID2 CHAR(15),
	DISCFRGT NUMERIC(19,5),
	ORDAVFRT NUMERIC(19,5),
	DISCMISC NUMERIC(19,5),
	ORDAVMSC NUMERIC(19,5),
	DISAVAMT NUMERIC(19,5),
	ORDAVAMT NUMERIC(19,5),
	DISCRTND NUMERIC(19,5),
	ORDISRTD NUMERIC(19,5),
	DISTKNAM NUMERIC(19,5),
	ORDISTKN NUMERIC(19,5),
	DSCPCTAM SMALLINT(5),
	DSCDLRAM NUMERIC(19,5),
	ORDDLRAT NUMERIC(19,5),
	DISAVTKN NUMERIC(19,5),
	ORDATKN NUMERIC(19,5),
	PYMTRMID CHAR(21),
	PRCLEVEL CHAR(11),
	LOCNCODE CHAR(11),
	BCHSOURC CHAR(15),
	BACHNUMB CHAR(15),
	CUSTNMBR CHAR(15),
	CUSTNAME CHAR(65),
	CSTPONBR CHAR(21),
	PROSPECT SMALLINT(5),
	MSTRNUMB INT(10),
	PCKSLPNO CHAR(21),
	PICTICNU CHAR(21),
	MRKDNAMT NUMERIC(19,5),
	ORMRKDAM NUMERIC(19,5),
	PRBTADCD CHAR(15),
	PRSTADCD CHAR(15),
	CNTCPRSN CHAR(61),
	SHIPTONAME CHAR(65),
	ADDRESS1 CHAR(61),
	ADDRESS2 CHAR(61),
	ADDRESS3 CHAR(61),
	CITY CHAR(35),
	STATE CHAR(29),
	ZIPCODE CHAR(11),
	CCODE CHAR(7),
	COUNTRY CHAR(61),
	PHNUMBR1 CHAR(21),
	PHNUMBR2 CHAR(21),
	PHONE3 CHAR(21),
	FAXNUMBR CHAR(21),
	COMAPPTO SMALLINT(5),
	COMMAMNT NUMERIC(19,5),
	OCOMMAMT NUMERIC(19,5),
	CMMSLAMT NUMERIC(19,5),
	ORCOSAMT NUMERIC(19,5),
	NCOMAMNT NUMERIC(19,5),
	ORNCMAMT NUMERIC(19,5),
	SHIPMTHD CHAR(15),
	TRDISAMT NUMERIC(19,5),
	ORTDISAM NUMERIC(19,5),
	TRDISPCT SMALLINT(5),
	SUBTOTAL NUMERIC(19,5),
	ORSUBTOT NUMERIC(19,5),
	REMSUBTO NUMERIC(19,5),
	OREMSUBT NUMERIC(19,5),
	EXTDCOST NUMERIC(19,5),
	OREXTCST NUMERIC(19,5),
	FRTAMNT NUMERIC(19,5),
	ORFRTAMT NUMERIC(19,5),
	MISCAMNT NUMERIC(19,5),
	ORMISCAMT NUMERIC(19,5),
	TXENGCLD TINYINT(3),
	TAXEXMT1 CHAR(25),
	TAXEXMT2 CHAR(25),
	TXRGNNUM CHAR(25),
	TAXSCHID CHAR(15),
	TXSCHSRC SMALLINT(5),
	BSIVCTTL TINYINT(3),
	FRTSCHID CHAR(15),
	FRTTXAMT NUMERIC(19,5),
	ORFRTTAX NUMERIC(19,5),
	FRGTTXBL SMALLINT(5),
	MSCSCHID CHAR(15),
	MSCTXAMT NUMERIC(19,5),
	ORMSCTAX NUMERIC(19,5),
	MISCTXBL SMALLINT(5),
	BKTFRTAM NUMERIC(19,5),
	ORBKTFRT NUMERIC(19,5),
	BKTMSCAM NUMERIC(19,5),
	ORBKTMSC NUMERIC(19,5),
	BCKTXAMT NUMERIC(19,5),
	OBTAXAMT NUMERIC(19,5),
	TXBTXAMT NUMERIC(19,5),
	OTAXTAMT NUMERIC(19,5),
	TAXAMNT NUMERIC(19,5),
	ORTAXAMT NUMERIC(19,5),
	ECTRX TINYINT(3),
	DOCAMNT NUMERIC(19,5),
	ORDOCAMT NUMERIC(19,5),
	PYMTRCVD NUMERIC(19,5),
	ORPMTRVD NUMERIC(19,5),
	DEPRECVD NUMERIC(19,5),
	ORDEPRVD NUMERIC(19,5),
	CODAMNT NUMERIC(19,5),
	ORCODAMT NUMERIC(19,5),
	ACCTAMNT NUMERIC(19,5),
	ORACTAMT NUMERIC(19,5),
	SALSTERR CHAR(15),
	SLPRSNID CHAR(15),
	UPSZONE CHAR(3),
	TIMESPRT SMALLINT(5),
	PSTGSTUS SMALLINT(5),
	VOIDSTTS SMALLINT(5),
	ALLOCABY SMALLINT(5),
	NOTEINDX NUMERIC(19,5),
	CURNCYID CHAR(15),
	CURRNIDX SMALLINT(5),
	RATETPID CHAR(15),
	EXGTBLID CHAR(15),
	XCHGRATE NUMERIC(19,7),
	DENXRATE NUMERIC(19,7),
	EXCHDATE TIMESTAMP,
	TIME1 TIMESTAMP,
	RTCLCMTD SMALLINT(5),
	MCTRXSTT SMALLINT(5),
	TRXSORCE CHAR(13),
	SOPHDRE1 BINARY,
	SOPHDRE2 BINARY,
	SOPLNERR BINARY,
	SOPHDRFL BINARY,
	COMMNTID CHAR(15),
	REFRENCE CHAR(31),
	POSTEDDT TIMESTAMP,
	PTDUSRID CHAR(15),
	USER2ENT CHAR(15),
	CREATDDT TIMESTAMP,
	MODIFDT TIMESTAMP,
	TAX_DATE TIMESTAMP,
	APLYWITH TINYINT(3),
	WITHHAMT NUMERIC(19,5),
	SHPPGDOC TINYINT(3),
	CORRCTN TINYINT(3),
	SIMPLIFD TINYINT(3),
	DOCNCORR CHAR(21),
	SEQNCORR SMALLINT(5),
	SALEDATE TIMESTAMP,
	EXCEPTIONALDEMAND TINYINT(3),
	FLAGS SMALLINT(5),
	SOPSTATUS SMALLINT(5),
	SHIPCOMPLETE TINYINT(3),
	DIRECTDEBIT TINYINT(3),
	DEX_ROW_ID INT(10)
);