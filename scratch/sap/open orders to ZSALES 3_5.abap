************************************************************************
*
*   Generated report for update: Business Information Warehouse
*
*   Template......: RSTMPLUR
*   InfoCube......: ZSALES
*   InfoSource....: 8ZOPNORD
*   Author........: CYBAGENT
*   Date..........: 06/02/2023 02:01:04
*
*   Do not change this report !
*
************************************************************************
REPORT GP3Y9DFGB7DROX3F8WGYMIY8U81 MESSAGE-ID rsau.

TYPE-POOLS:
  rsau, rsaa, rsarr, rsa, rs, rssm, rrsv, rsdw, rsudt, rsdm, rsarc,
  rstim, ruom.


TYPES:
  BEGIN OF g_s_hashed_cube.
        INCLUDE STRUCTURE /BIC/VZSALEST.
TYPES:
    fOPNORDDLR TYPE rs_bool,
    fOPNORDQTY TYPE rs_bool,
    fGIDATE TYPE rs_bool,
    fREQDATE TYPE rs_bool,
    f0CONF_QTY TYPE rs_bool,
    fVVGBP TYPE rs_bool,
    fZOPNORDLN TYPE rs_bool,
    f0GROSS_WGT TYPE rs_bool,
    fZNONCONF1 TYPE rs_bool,
    fZZPDTCNT TYPE rs_bool,
    recno      LIKE rssm_s_crosstab-icrecord,
  END   OF g_s_hashed_cube,
  g_t_cube        TYPE TABLE OF g_s_hashed_cube,
  g_t_hashed_cube TYPE HASHED TABLE OF g_s_hashed_cube
    WITH UNIQUE KEY
         DISTR_CHAN
         DIVISION
         DOC_TYPE
         DSDEL_DATE
         MATAV_DATE
         MATERIAL
         ORD_TYPE
         PLANT
         REQ_DATE
         SALESORG
         SALES_OFF
         SOLD_TO
         /BIC/ZMATAVDAT
         LOAD_DATE
         /BIC/ZCMRCAT
         /BIC/ZPTYP_GRP
         TCTVAPRCTP
         SHIP_TO
         /BIC/ZWORKDAY
         REASON_REJ
         CREATEDON
         CUST_SALES
         /BIC/ZMAT_PTG
         /BIC/ZZORC
         DEALTYPE
         ITEM_CATEG
         DELIV_NUMB
         DOC_NUMBER
         MAT_PLANT
         /BIC/ZZREC
         /BIC/ZZRECMAN
         S_ORD_ITEM
         PO_NUMBER
         /BIC/ZZCRSDRC
         /BIC/ZZOPDT
         /BIC/ZZPDT
         FISCVARNT
         CALDAY
         FISCPER
         FISCYEAR
         SALES_UNIT
         DOC_CURRCY
         UNIT
         BASE_UOM
         CURRENCY
         UNIT_OF_WT
         ,

BEGIN OF g_s_comstru.
        INCLUDE STRUCTURE /BIC/CS8ZOPNORD.
TYPES:     recno  LIKE sy-tabix,
  END OF g_s_comstru,
  BEGIN OF data_package_structure.
        INCLUDE STRUCTURE /BIC/CS8ZOPNORD.
TYPES:     recno   LIKE sy-tabix,
  END OF data_package_structure,
  g_t_comstru TYPE g_s_comstru OCCURS 0.


data:
 g               LIKE /BIC/VZSALEST,
 g_s_kbtbx       TYPE g_s_hashed_cube,
 g_s_kb          TYPE g_s_hashed_cube,
 g_t_kb          TYPE g_t_hashed_cube,
 icube_values    LIKE /BIC/VZSALEST,
 result_table    LIKE icube_values OCCURS 0 WITH HEADER LINE,"#EC *
 g_s_cha_calculation LIKE /BIC/VZSALEST,
 g_s_is          TYPE g_s_comstru,
 g_bookmode      TYPE num1,                                 "#EC NEEDED
 g_continue_at_err TYPE rs_bool,
 g_simulation    TYPE rs_bool,
 g_cluster_writing TYPE rs_bool,
* for error handling
 g_s_object       TYPE rssm_s_error_handler_object,
 g_s_error_log    TYPE rssm_s_errorlog_int,                 "#EC *
 g_t_error_log    TYPE rssm_t_errorlog_int,                 "#EC *
 g_r_error_handler TYPE REF TO cl_rssm_error_handler,
* name convention violated in the following variables
 comm_structure LIKE /BIC/CS8ZOPNORD,                 "#EC *
 g_record_no_cha_calculation LIKE sy-tabix,
 i_record_no     LIKE sy-tabix,
   record_no     LIKE sy-tabix,
 i_record_all    LIKE sy-tabix,
   record_all    LIKE sy-tabix,                             "#EC NEEDED
 i_logsys        LIKE rsupdsimulh-logsys,
   source_system LIKE rsupdsimulh-logsys,                   "#EC NEEDED
 i_datap         TYPE rsarr_s_receive_header3-datapakid,
 i_requnr        TYPE rsarr_s_receive_header3-request,
 i_infocube      TYPE rsau_s_updinfo-infocube,              "#EC *
 monitor         LIKE rsmonitor OCCURS 0 WITH HEADER LINE,  "#EC *
 monitor_recno   LIKE rsmonitors OCCURS 0 WITH HEADER LINE, "#EC *
 abort           LIKE sy-subrc,                             "#EC *
 g_result        TYPE REF TO data,                          "#EC *
 g_t_order       TYPE rsd_t_iobjnm,                         "#EC *
 rsmoninc5m,                                                "#EC *
 rsmoninc5c,                                                "#EC *
 rsmoninc5w,                                                "#EC *
 rsmoninc5g.                                                "#EC *

FIELD-SYMBOLS:
  <g_s_share>     TYPE rstim_s_share.                       "#EC *
DATA:
  g_s_share       TYPE rstim_s_share,                       "#EC *
  g_t_share       TYPE rstim_t_share,                       "#EC *
  g_stop          TYPE i VALUE 0.                           "#EC *

* for unicode
DATA:
  l_split_position TYPE i.                                  "#EC *

* for 0RECORDMODE
DATA:
  g_flg_rec       TYPE rs_bool.                             "#EC NEEDED

* for initial return table
DATA:
  g_routmulti_init TYPE rs_bool.                            "#EC NEEDED

DATA:
  g_break         TYPE rs_bool,
  g_break_record  TYPE rs_bool,
  g_break_table   TYPE rs_bool,
  g_break_startr  TYPE rs_bool,                             "#EC NEEDED
  g_break_routine TYPE rs_bool,                             "#EC NEEDED

  g_error_in_formula TYPE REF TO cx_root,
  g_previous         TYPE string,

  g_s_minfo TYPE rssm_s_minfo.

FIELD-SYMBOLS: <g_rresult> TYPE ANY.                        "#EC *


DATA:
  g_count_error    TYPE sy-tabix,
  g_error          TYPE rs_bool,
  g_dummy.                                                  "#EC *

***********************************************************************
* fill infocube with infosource data
***********************************************************************
FORM update_infocube
  TABLES   i_t_isource    TYPE g_t_comstru
  USING    i_s_minfo      TYPE rssm_s_minfo
           i_datap_init   TYPE rsarr_s_receive_header3-datapakid
  CHANGING c_t_idocstate  TYPE rsarr_t_idocstate
           c_subrc        LIKE sy-subrc.             "#EC * "#EC CALLED

  DATA:
    l_dont_continue TYPE rs_bool,
    g_t_isource   TYPE g_t_comstru,
    l_t_comstru   TYPE g_t_comstru,

    l_s_ic           TYPE g_s_hashed_cube,
    l_t_ic           TYPE g_t_hashed_cube,
    l_wa_new         TYPE rs_bool,
    l_val_set        TYPE rs_bool,
    l_fscvtsrc       TYPE rsau_s_updinfo-fscvtsrc,
    l_s_iobjnm       TYPE rsd_s_iobjnm,                     "#EC *
    l_s_chavlerr     TYPE rsdw_s_chavlerr,
    l_t_chavlerr     TYPE rsdw_t_chavlerr,
    l_t_rsmondata    LIKE rsmonview OCCURS 0 WITH HEADER LINE,
*   error handling  / crosstab
    l_recno         LIKE rssm_s_crosstab-icrecord VALUE 0,
    l_t_crosstab    TYPE rssm_t_crosstab,
    l_s_crosstab    TYPE rssm_s_crosstab,
    l_abort         LIKE sy-subrc.

  TRY.

* init global vars
      i_infocube    = 'ZSALES'.
      i_requnr      = i_s_minfo-requnr.
      g_bookmode    = i_s_minfo-bookmode.
      g_continue_at_err = i_s_minfo-continue_at_err.
      g_simulation  = i_s_minfo-simulation.
      g_cluster_writing = i_s_minfo-only_to_indx.
      g_t_isource   = i_t_isource[].
      source_system = i_s_minfo-logsys.
      i_logsys      = i_s_minfo-logsys.
      i_datap       = i_datap_init.
      g_s_minfo     = i_s_minfo.
      g_s_minfo-updr_processed = rs_c_true.
      REFRESH: monitor.
      REFRESH: monitor_recno.
      CLEAR: g_record_no_cha_calculation,
             g_s_is.
      CLEAR: g_count_error.


* Error Handler
      IF NOT g_s_minfo-crt_package = 'X'.
        g_s_object-infocube  = i_infocube.
        g_s_object-request   = g_s_minfo-requnr.
        g_s_object-datapakid = i_datap.

        CALL METHOD cl_rssm_error_handler=>create
          EXPORTING
            i_object      = g_s_object
            i_simulation  = g_simulation
            i_rebuild     = g_s_minfo-rebuilding
          IMPORTING
            e_r_reference = g_r_error_handler.
      ENDIF.

      IF g_s_minfo-debugmode = rs_c_true.
        BREAK-POINT.                                       "#EC NOBREAK
        g_break = rs_c_false.         " any key figure - default ' '
        g_break_record = rs_c_true.   " any record     - default 'X'
        g_break_table = rs_c_true.    " complete table - default 'X'
        g_break_startr = rs_c_true.   " before startroutine - default 'X'
        g_break_routine = rs_c_false. " before custmer routine -
        "                       default ' '
      ENDIF.

* check, if source of fiscvarnt has changed
      CALL FUNCTION 'RSAU_FISCVARNT_INFO_GET'
        EXPORTING
          i_infocube        = 'ZSALES'
          i_isource         = '8ZOPNORD'
        IMPORTING
          e_fscvtsrc        = l_fscvtsrc
        EXCEPTIONS
          isource_not_found = 1
          unspecified_error = 2
          OTHERS            = 3.
      IF sy-subrc   <> 0 OR
         l_fscvtsrc <> 'IC'.
        PERFORM request_error USING 'RSAU' 'E' '497' 'UPDATE_INFOCUBE'
                                    space space space 'UPDATE_INFOCUBE'
                              CHANGING c_t_idocstate.
        c_subrc = 1.
        EXIT.
      ENDIF.

      IF g_s_minfo-cancellation = rs_c_true.
        PERFORM request_error USING 'RSAU' 'E' '491' 'UPDATE_INFOCUBE'
                                    space space space 'UPDATE_INFOCUBE'
                              CHANGING c_t_idocstate.
        c_subrc = 1.
        EXIT.
      ENDIF.
      IF g_simulation = rs_c_false AND
         NOT g_s_minfo-crt_package = 'X'.
*   log 'start update'-event
        INCLUDE rsmoninc55.
      ENDIF.
      DESCRIBE TABLE g_t_isource LINES i_record_all.
      record_all = i_record_all.
*chandra
      IF g_simulation = rs_c_true.
        l_t_comstru = g_t_isource.
      ENDIF.
*chandra

      LOOP AT g_t_isource INTO g_s_is.
        CLEAR l_s_crosstab.
        l_s_crosstab-csrecord = g_s_is-recno.
        i_record_no = sy-tabix.
        record_no   = i_record_no.
        l_wa_new = rs_c_false.
        l_val_set = rs_c_false.
        CLEAR   g_s_kb.
        REFRESH g_t_kb.
        CLEAR   g_s_kbtbx.
        g_routmulti_init = rs_c_false.
*   *******************************************************************
*   * update rule no...: 0009
*   * update infoobject: OPNORDDLR
*   *******************************************************************
        PERFORM r0009_OPNORDDLR
          CHANGING l_wa_new l_val_set c_t_idocstate c_subrc l_abort.
        IF l_abort <> 0.
          EXIT.
        ELSEIF c_subrc <> 0.
*     skip this record and continue
          c_subrc = 0.
          REFRESH g_t_kb.
          CLEAR g_s_kb.
          CONTINUE.
        ENDIF.
        g_flg_rec = rs_c_true.
*   *******************************************************************
*   * update rule no...: 0010
*   * update infoobject: OPNORDQTY
*   *******************************************************************
        PERFORM r0010_OPNORDQTY
          CHANGING l_wa_new l_val_set c_t_idocstate c_subrc l_abort.
        IF l_abort <> 0.
          EXIT.
        ELSEIF c_subrc <> 0.
*     skip this record and continue
          c_subrc = 0.
          REFRESH g_t_kb.
          CLEAR g_s_kb.
          CONTINUE.
        ENDIF.
        g_flg_rec = rs_c_true.
*   *******************************************************************
*   * update rule no...: 0011
*   * update infoobject: GIDATE
*   *******************************************************************
        PERFORM r0011_GIDATE
          CHANGING l_wa_new l_val_set c_t_idocstate c_subrc l_abort.
        IF l_abort <> 0.
          EXIT.
        ELSEIF c_subrc <> 0.
*     skip this record and continue
          c_subrc = 0.
          REFRESH g_t_kb.
          CLEAR g_s_kb.
          CONTINUE.
        ENDIF.
        g_flg_rec = rs_c_true.
*   *******************************************************************
*   * update rule no...: 0012
*   * update infoobject: REQDATE
*   *******************************************************************
        PERFORM r0012_REQDATE
          CHANGING l_wa_new l_val_set c_t_idocstate c_subrc l_abort.
        IF l_abort <> 0.
          EXIT.
        ELSEIF c_subrc <> 0.
*     skip this record and continue
          c_subrc = 0.
          REFRESH g_t_kb.
          CLEAR g_s_kb.
          CONTINUE.
        ENDIF.
        g_flg_rec = rs_c_true.
*   *******************************************************************
*   * update rule no...: 0016
*   * update infoobject: 0CONF_QTY
*   *******************************************************************
        PERFORM r0016_0CONF_QTY
          CHANGING l_wa_new l_val_set c_t_idocstate c_subrc l_abort.
        IF l_abort <> 0.
          EXIT.
        ELSEIF c_subrc <> 0.
*     skip this record and continue
          c_subrc = 0.
          REFRESH g_t_kb.
          CLEAR g_s_kb.
          CONTINUE.
        ENDIF.
        g_flg_rec = rs_c_true.
*   *******************************************************************
*   * update rule no...: 0017
*   * update infoobject: VVGBP
*   *******************************************************************
        PERFORM r0017_VVGBP
          CHANGING l_wa_new l_val_set c_t_idocstate c_subrc l_abort.
        IF l_abort <> 0.
          EXIT.
        ELSEIF c_subrc <> 0.
*     skip this record and continue
          c_subrc = 0.
          REFRESH g_t_kb.
          CLEAR g_s_kb.
          CONTINUE.
        ENDIF.
        g_flg_rec = rs_c_true.
*   *******************************************************************
*   * update rule no...: 0019
*   * update infoobject: ZOPNORDLN
*   *******************************************************************
        PERFORM r0019_ZOPNORDLN
          CHANGING l_wa_new l_val_set c_t_idocstate c_subrc l_abort.
        IF l_abort <> 0.
          EXIT.
        ELSEIF c_subrc <> 0.
*     skip this record and continue
          c_subrc = 0.
          REFRESH g_t_kb.
          CLEAR g_s_kb.
          CONTINUE.
        ENDIF.
        g_flg_rec = rs_c_true.
*   *******************************************************************
*   * update rule no...: 0020
*   * update infoobject: 0GROSS_WGT
*   *******************************************************************
        PERFORM r0020_0GROSS_WGT
          CHANGING l_wa_new l_val_set c_t_idocstate c_subrc l_abort.
        IF l_abort <> 0.
          EXIT.
        ELSEIF c_subrc <> 0.
*     skip this record and continue
          c_subrc = 0.
          REFRESH g_t_kb.
          CLEAR g_s_kb.
          CONTINUE.
        ENDIF.
        g_flg_rec = rs_c_true.
*   *******************************************************************
*   * update rule no...: 0021
*   * update infoobject: ZNONCONF1
*   *******************************************************************
        PERFORM r0021_ZNONCONF1
          CHANGING l_wa_new l_val_set c_t_idocstate c_subrc l_abort.
        IF l_abort <> 0.
          EXIT.
        ELSEIF c_subrc <> 0.
*     skip this record and continue
          c_subrc = 0.
          REFRESH g_t_kb.
          CLEAR g_s_kb.
          CONTINUE.
        ENDIF.
        g_flg_rec = rs_c_true.
*   *******************************************************************
*   * update rule no...: 0022
*   * update infoobject: ZZPDTCNT
*   *******************************************************************
        PERFORM r0022_ZZPDTCNT
          CHANGING l_wa_new l_val_set c_t_idocstate c_subrc l_abort.
        IF l_abort <> 0.
          EXIT.
        ELSEIF c_subrc <> 0.
*     skip this record and continue
          c_subrc = 0.
          REFRESH g_t_kb.
          CLEAR g_s_kb.
          CONTINUE.
        ENDIF.
        g_flg_rec = rs_c_true.
*   *******************************************************************
*   * save last key figure to key figure buffer table
*   *******************************************************************
        IF l_val_set = rs_c_true.
          IF l_wa_new = rs_c_false.        "read from table !!!
            DELETE TABLE g_t_kb FROM g_s_kbtbx.
          ENDIF.
          INSERT g_s_kb INTO TABLE g_t_kb.
          l_val_set = rs_c_false.
          CLEAR g_s_kb.
        ENDIF.


*   insert records from the keyfigure buffer table into the local cube
        LOOP AT g_t_kb INTO g_s_kb.

          READ TABLE l_t_ic INTO l_s_ic WITH TABLE KEY
            DISTR_CHAN = g_s_kb-DISTR_CHAN
            DIVISION = g_s_kb-DIVISION
            DOC_TYPE = g_s_kb-DOC_TYPE
            DSDEL_DATE = g_s_kb-DSDEL_DATE
            MATAV_DATE = g_s_kb-MATAV_DATE
            MATERIAL = g_s_kb-MATERIAL
            ORD_TYPE = g_s_kb-ORD_TYPE
            PLANT = g_s_kb-PLANT
            REQ_DATE = g_s_kb-REQ_DATE
            SALESORG = g_s_kb-SALESORG
            SALES_OFF = g_s_kb-SALES_OFF
            SOLD_TO = g_s_kb-SOLD_TO
            /BIC/ZMATAVDAT = g_s_kb-/BIC/ZMATAVDAT
            LOAD_DATE = g_s_kb-LOAD_DATE
            /BIC/ZCMRCAT = g_s_kb-/BIC/ZCMRCAT
            /BIC/ZPTYP_GRP = g_s_kb-/BIC/ZPTYP_GRP
            TCTVAPRCTP = g_s_kb-TCTVAPRCTP
            SHIP_TO = g_s_kb-SHIP_TO
            /BIC/ZWORKDAY = g_s_kb-/BIC/ZWORKDAY
            REASON_REJ = g_s_kb-REASON_REJ
            CREATEDON = g_s_kb-CREATEDON
            CUST_SALES = g_s_kb-CUST_SALES
            /BIC/ZMAT_PTG = g_s_kb-/BIC/ZMAT_PTG
            /BIC/ZZORC = g_s_kb-/BIC/ZZORC
            DEALTYPE = g_s_kb-DEALTYPE
            ITEM_CATEG = g_s_kb-ITEM_CATEG
            DELIV_NUMB = g_s_kb-DELIV_NUMB
            DOC_NUMBER = g_s_kb-DOC_NUMBER
            MAT_PLANT = g_s_kb-MAT_PLANT
            /BIC/ZZREC = g_s_kb-/BIC/ZZREC
            /BIC/ZZRECMAN = g_s_kb-/BIC/ZZRECMAN
            S_ORD_ITEM = g_s_kb-S_ORD_ITEM
            PO_NUMBER = g_s_kb-PO_NUMBER
            /BIC/ZZCRSDRC = g_s_kb-/BIC/ZZCRSDRC
            /BIC/ZZOPDT = g_s_kb-/BIC/ZZOPDT
            /BIC/ZZPDT = g_s_kb-/BIC/ZZPDT
            FISCVARNT = g_s_kb-FISCVARNT
            CALDAY = g_s_kb-CALDAY
            FISCPER = g_s_kb-FISCPER
            FISCYEAR = g_s_kb-FISCYEAR
            SALES_UNIT = g_s_kb-SALES_UNIT
            DOC_CURRCY = g_s_kb-DOC_CURRCY
            UNIT = g_s_kb-UNIT
            BASE_UOM = g_s_kb-BASE_UOM
            CURRENCY = g_s_kb-CURRENCY
            UNIT_OF_WT = g_s_kb-UNIT_OF_WT
            .
          IF sy-subrc = 0.
            IF g_s_kb-fOPNORDDLR = rs_c_true.
              l_s_ic-fOPNORDDLR = rs_c_true.
              l_s_ic-/BIC/OPNORDDLR = l_s_ic-/BIC/OPNORDDLR +
                g_s_kb-/BIC/OPNORDDLR.
            ENDIF.
            IF g_s_kb-fOPNORDQTY = rs_c_true.
              l_s_ic-fOPNORDQTY = rs_c_true.
              l_s_ic-/BIC/OPNORDQTY = l_s_ic-/BIC/OPNORDQTY +
                g_s_kb-/BIC/OPNORDQTY.
            ENDIF.
            IF g_s_kb-fGIDATE = rs_c_true  AND
             ( l_s_ic-fGIDATE = rs_c_false OR
               l_s_ic-/BIC/GIDATE < g_s_kb-/BIC/GIDATE ).
              l_s_ic-fGIDATE = rs_c_true.
              l_s_ic-/BIC/GIDATE = g_s_kb-/BIC/GIDATE.
            ENDIF.
            IF g_s_kb-fREQDATE = rs_c_true  AND
             ( l_s_ic-fREQDATE = rs_c_false OR
               l_s_ic-/BIC/REQDATE < g_s_kb-/BIC/REQDATE ).
              l_s_ic-fREQDATE = rs_c_true.
              l_s_ic-/BIC/REQDATE = g_s_kb-/BIC/REQDATE.
            ENDIF.
            IF g_s_kb-f0CONF_QTY = rs_c_true.
              l_s_ic-f0CONF_QTY = rs_c_true.
              l_s_ic-CONF_QTY = l_s_ic-CONF_QTY +
                g_s_kb-CONF_QTY.
            ENDIF.
            IF g_s_kb-fVVGBP = rs_c_true.
              l_s_ic-fVVGBP = rs_c_true.
              l_s_ic-/BIC/VVGBP = l_s_ic-/BIC/VVGBP +
                g_s_kb-/BIC/VVGBP.
            ENDIF.
            IF g_s_kb-fZOPNORDLN = rs_c_true.
              l_s_ic-fZOPNORDLN = rs_c_true.
              l_s_ic-/BIC/ZOPNORDLN = l_s_ic-/BIC/ZOPNORDLN +
                g_s_kb-/BIC/ZOPNORDLN.
            ENDIF.
            IF g_s_kb-f0GROSS_WGT = rs_c_true.
              l_s_ic-f0GROSS_WGT = rs_c_true.
              l_s_ic-GROSS_WGT = l_s_ic-GROSS_WGT +
                g_s_kb-GROSS_WGT.
            ENDIF.
            IF g_s_kb-fZNONCONF1 = rs_c_true.
              l_s_ic-fZNONCONF1 = rs_c_true.
              l_s_ic-/BIC/ZNONCONF1 = l_s_ic-/BIC/ZNONCONF1 +
                g_s_kb-/BIC/ZNONCONF1.
            ENDIF.
            IF g_s_kb-fZZPDTCNT = rs_c_true.
              l_s_ic-fZZPDTCNT = rs_c_true.
              l_s_ic-/BIC/ZZPDTCNT = l_s_ic-/BIC/ZZPDTCNT +
                g_s_kb-/BIC/ZZPDTCNT.
            ENDIF.
            l_s_crosstab-icrecord = l_s_ic-recno.
            APPEND l_s_crosstab TO l_t_crosstab.
            MODIFY TABLE l_t_ic FROM l_s_ic.
          ELSE.
            l_recno = l_recno + 1.
            g_s_kb-recno = l_recno.
            l_s_crosstab-icrecord = l_recno.
            APPEND l_s_crosstab TO l_t_crosstab.
            INSERT g_s_kb INTO TABLE l_t_ic.
          ENDIF.
        ENDLOOP.

        IF g_break_record = rs_c_true. "any record
          BREAK-POINT.                                     "#EC NOBREAK
        ENDIF.

      ENDLOOP.

      IF g_break_table = rs_c_true. "complete table
        BREAK-POINT.                                       "#EC NOBREAK
      ENDIF.

      IF c_subrc <> 0.
        PERFORM abort_message
         USING sy-msgid sy-msgty sy-msgno sy-msgv1
          sy-msgv2 sy-msgv3 sy-msgv4 0 rs_c_true.
        PERFORM error_end
          USING    'UPDATE_INFOCUBE'
          CHANGING c_t_idocstate.
        EXIT.
      ELSEIF g_continue_at_err = rs_c_false AND l_dont_continue IS NOT INITIAL.
         PERFORM error_end
          USING    'UPDATE_INFOCUBE'
          CHANGING c_t_idocstate.
      ENDIF.

* fill the InfoObject 0REQUID
        l_s_ic-REQUID = g_s_minfo-requnr.
      MODIFY l_t_ic FROM l_s_ic
        TRANSPORTING REQUID
        WHERE     REQUID IS INITIAL OR
              NOT REQUID IS INITIAL.



      IF g_simulation = rs_c_false.
*   log 'stop update'-event
        DESCRIBE TABLE l_t_ic LINES sy-tfill.
        IF NOT g_s_minfo-crt_package = 'X'.
          INCLUDE rsmoninc59.
        ENDIF.
      ENDIF.
      IF NOT g_s_minfo-crt_package = 'X'.
        CALL METHOD g_r_error_handler->add_entries_cross_tab
          EXPORTING
            i_t_crosstab = l_t_crosstab.
      ENDIF.
      IF NOT g_s_minfo-crt_package = 'X'.
        CALL METHOD g_r_error_handler->commit
          EXPORTING
            i_aufrufer = '59'
            i_abort    = rs_c_false.
      ENDIF.
      IF g_cluster_writing = rs_c_true.
*       cluster data definition
        DATA: l_t_target TYPE STANDARD TABLE OF /BIC/VZSALEST,
              l_s_target_wa TYPE /BIC/VZSALEST,
              l_s_rsmondata TYPE rsmonview,
              l_datasets_inserted TYPE i,
              l_s_cross TYPE rssm_s_crosstab,
              l_s_tech_key TYPE rstran_tmpl_comp_tech_key,
              l_r_keystruct TYPE REF TO data,
              l_r_tgtstruct TYPE REF TO data,
              l_r_outstruct TYPE REF TO data,
              l_r_out_tab TYPE REF TO data.

*       cluster field-symbols for output
        FIELD-SYMBOLS: <fs_out_tab> TYPE STANDARD TABLE,
                       <fs_out_wa> TYPE any,
                       <fs_out_field> TYPE any.

*       concatenate technical key and target struct and assign
        GET REFERENCE OF: l_s_tech_key INTO l_r_keystruct,
                          l_s_target_wa INTO l_r_tgtstruct.
        cl_rstran_compare_backend=>concat_structures(
          EXPORTING
            i_r_workarea1 = l_r_keystruct
            i_r_workarea2 = l_r_tgtstruct
            i_for_df_comparison = rs_c_true
          RECEIVING
            r_r_new_workarea = l_r_outstruct ).
        ASSIGN l_r_outstruct->* TO <fs_out_wa>.
*       create output table
        CREATE DATA l_r_out_tab LIKE STANDARD TABLE OF <fs_out_wa>.
        ASSIGN l_r_out_tab->* TO <fs_out_tab>.
*       move target data into target table
        LOOP AT l_t_ic INTO l_s_ic.
          CLEAR <fs_out_wa>.
          "write corresponding application data
          MOVE-CORRESPONDING l_s_ic TO <fs_out_wa>.
          "fill technical key and append to export table
          ASSIGN COMPONENT 'REQUID' OF STRUCTURE <fs_out_wa> TO <fs_out_field>.
          <fs_out_field> = g_s_minfo-requnr.
          ASSIGN COMPONENT 'DATAPAKID' OF STRUCTURE <fs_out_wa> TO <fs_out_field>.
          <fs_out_field> = g_s_minfo-datapakid.
          ASSIGN COMPONENT 'RECORD' OF STRUCTURE <fs_out_wa> TO <fs_out_field>.
          READ TABLE l_t_crosstab WITH KEY icrecord = l_s_ic-recno INTO l_s_cross.
          <fs_out_field> = l_s_cross-csrecord.
*         insert the filled dataset
          APPEND <fs_out_wa> TO <fs_out_tab>.
        ENDLOOP.
        DESCRIBE TABLE <fs_out_tab> LINES l_datasets_inserted.
*       fill the cluster table with data
        CALL FUNCTION 'RSTRAN_RSAU_CLUSTER_DATA_FILL'
          EXPORTING
            i_r_data    = l_r_out_tab
            i_datapakid = g_s_minfo-datapakid
            i_dftype    = cl_rstran_compare_backend=>c_dftype_updaterules.
*       set target structure for dtp dataflow
        cl_rstran_compare_backend=>set_tgt_structname( '/BIC/VZSALEST' ).
*       set successful status
        l_s_rsmondata-rnr        = g_s_minfo-requnr.
        l_s_rsmondata-datapakid  = g_s_minfo-datapakid.
        l_s_rsmondata-infocube   = 'ZSALES'.
        l_s_rsmondata-aufrufer   = 60.
        l_s_rsmondata-msgid      = ''.
        l_s_rsmondata-msgty      = 'I'.
        l_s_rsmondata-msgno      = 5.
        l_s_rsmondata-msgv1      = g_s_minfo-datapakid.
        l_s_rsmondata-msgv2      = l_datasets_inserted.
        l_s_rsmondata-req_insert = l_datasets_inserted.
        l_s_rsmondata-req_update = 0.
        APPEND l_s_rsmondata TO l_t_rsmondata.
        CALL FUNCTION 'RSSM_MON_WRITE_MONITOR_IC'
          EXPORTING
            aggregate = 'N'
          TABLES
            data      = l_t_rsmondata.
      ELSEIF g_simulation = rs_c_false.
        CALL METHOD cl_rsdd_cube_writer=>write_ur
          EXPORTING
            i_t_data      = l_t_ic
            i_s_minfo     = g_s_minfo
            i_datap       = i_datap
            i_infocube    = 'ZSALES'
          IMPORTING
            e_subrc       = c_subrc
          changing
            c_t_chavlerr  = l_t_chavlerr
            c_t_idocstate = c_t_idocstate.
        IF c_subrc <> 0 and c_t_idocstate[] is initial.
          perform error_end using 'write_ic'
                            changing c_t_idocstate.
        ENDIF.
      ELSE.
*   simulation
*   save simulation data to database §$%&
        DATA:
          BEGIN OF l_s_icvw.
        INCLUDE    STRUCTURE /BIC/VZSALEST.
        DATA:
          recno      TYPE sy-tabix,
          t_scol     TYPE lvc_t_scol,
          t_chavlerr TYPE rsdw_t_chavlerr,
          errtype    TYPE rsdw_errtype,
        END   OF l_s_icvw,
        l_t_icvw   LIKE l_s_icvw OCCURS 0,
        l_t_sim_wi TYPE g_t_cube,
        l_s_scol   TYPE lvc_s_scol,
        l_at_new   TYPE rs_bool.
        FIELD-SYMBOLS:
          <l_s_icvw> LIKE l_s_icvw.

        l_t_sim_wi = l_t_ic.
        CALL METHOD cl_rsdd_cube_writer=>write_ur
          EXPORTING
            i_t_data      = l_t_sim_wi
            i_s_minfo     = g_s_minfo
            i_datap       = i_datap
            i_infocube    = 'ZSALES'
          IMPORTING
            e_subrc       = c_subrc
          changing
            c_t_chavlerr  = l_t_chavlerr
            c_t_idocstate = c_t_idocstate.
        LOOP AT l_t_ic INTO l_s_ic.
          MOVE-CORRESPONDING l_s_ic TO l_s_icvw.
          APPEND l_s_icvw TO l_t_icvw.
        ENDLOOP.

        SORT l_t_chavlerr BY recno.
        l_s_scol-color-col = 6.
        l_s_scol-nokeycol  = rs_c_true.
        LOOP AT l_t_chavlerr INTO l_s_chavlerr WHERE recno > 0.
          CLEAR l_at_new.
          AT NEW recno.                                     "#EC *
            l_at_new = rs_c_true.
          ENDAT.
          IF l_at_new = rs_c_true.
            READ TABLE l_t_icvw ASSIGNING <l_s_icvw>
                 INDEX l_s_chavlerr-recno.
          ENDIF.
          l_s_scol-fname = l_s_chavlerr-fieldnm.
          APPEND l_s_scol     TO <l_s_icvw>-t_scol.
          APPEND l_s_chavlerr TO <l_s_icvw>-t_chavlerr.
          <l_s_icvw>-errtype = l_s_chavlerr-errtype.
        ENDLOOP.

  CALL FUNCTION 'RSAU_SIMULATION_SAVE_DATA'
    EXPORTING
      i_rnr        = i_requnr
      i_simul_id   = g_s_minfo-simrequnr
      i_infocube   = 'ZSALES'
      i_isource    = '8ZOPNORD'
      i_logsys     = i_logsys
      i_tabname_c0 = '/BIC/CS8ZOPNORD'
      i_tabname_v0 = '/BIC/VZSALEST'
    TABLES
      c_t_c0       = l_t_comstru
      c_t_v0       = l_t_sim_wi
    EXCEPTIONS
      OTHERS       = 1.
  IF sy-subrc <> 0.
    c_subrc = sy-subrc.
  ENDIF.
  CALL FUNCTION 'RSAU_SIMULATION_DISPLAY_DATA'
    EXPORTING
      i_rnr                      = i_requnr
      i_simul_id                 = g_s_minfo-simrequnr
      i_select_idoc_records      = rs_c_false
      i_delete_data_after_import = rs_c_true
    TABLES
      c_t_v0                     = l_t_icvw
      c_t_c0                     = l_t_comstru
      c_t_crosstab               = l_t_crosstab
    EXCEPTIONS
      OTHERS                     = 1.                       "#EC *
ENDIF.

CATCH cx_sy_arithmetic_error.
  sy-subrc = 10.
  c_subrc = sy-subrc.
  CATCH cx_sy_conversion_error.
    sy-subrc = 9.
    c_subrc = sy-subrc.
ENDTRY.
IF c_subrc <> 0.
  IF g_simulation = rs_c_false.
    PERFORM error_end USING 'UPDATE_INFOCUBE'
                      CHANGING c_t_idocstate.
  ENDIF.
ENDIF.
* the end

ENDFORM.                    "update_infocube
************************************************************************
* handle the end error message
************************************************************************
FORM error_end USING i_routid TYPE c
     CHANGING c_t_idocstate TYPE rsarr_t_idocstate.

  DATA:
    l_s_monitor   LIKE rsmonitor,
    l_s_idocstate TYPE rsarr_s_idocstate,
    l_s_rsmondata LIKE rsmonview,
    l_t_rsmondata LIKE rsmonview OCCURS 0.

  l_s_idocstate-status = rsarr_c_idocstate_error.
  l_s_idocstate-uname  = sy-uname.
  l_s_idocstate-repid  = sy-repid.
  l_s_idocstate-routid = i_routid.

  l_s_rsmondata-msgno  = '499'.
  l_s_rsmondata-msgid  = 'RSAU'.
  l_s_rsmondata-msgty  = rs_c_error.
  l_s_rsmondata-msgv1  = i_routid.
  l_s_rsmondata-msgv2  = 'ZSALES'.
  MOVE-CORRESPONDING l_s_rsmondata TO l_s_idocstate.
  APPEND l_s_idocstate TO c_t_idocstate.

  l_s_rsmondata-rnr       = i_requnr.
  l_s_rsmondata-datapakid = i_datap.
  l_s_rsmondata-aufrufer  = '59'.
  l_s_rsmondata-error   = rs_c_true.
  APPEND l_s_rsmondata TO l_t_rsmondata.

  IF g_s_minfo-crt_package = 'X'.
    LOOP AT l_t_rsmondata INTO l_s_rsmondata.
      CALL METHOD g_s_minfo-crt_dta_object->ur_msg
        EXPORTING
          i_datapakid = g_s_minfo-datapakid
          i_msgno     = l_s_rsmondata-msgno
          i_msgid     = l_s_rsmondata-msgid
          i_msgty     = l_s_rsmondata-msgty
          i_msgv1     = l_s_rsmondata-msgv1.
    ENDLOOP.
  ELSE.
    CALL FUNCTION 'RSSM_MON_WRITE_MONITOR_IC'
      EXPORTING
        aggregate = 'N'
      TABLES
        data      = l_t_rsmondata.
  ENDIF.

ENDFORM.  "ERROR_END





*else.
************************************************************************
* routine no.: 0001
************************************************************************
FORM routine_0001
  CHANGING
    result         TYPE g_s_hashed_cube-/BIC/ZOPNORDLN
    returncode     LIKE sy-subrc
    c_t_idocstate  TYPE rsarr_t_idocstate
    c_subrc        LIKE sy-subrc
    c_abort        LIKE sy-subrc.                           "#EC *
  DATA:
    l_t_rsmondata LIKE rsmonview OCCURS 0 WITH HEADER LINE. "#EC *

  TRY.
* init variables
      MOVE-CORRESPONDING g_s_is TO comm_structure.

******************************************************
*This ABAP Code was generated automatically          *
*Formula Calculator                                  *
******************************************************
*Generated :2023:06:02-02:01
*User: CYBAGENT
******************************************************
*
*Calculation:
RESULT = '1.000 '.
    CATCH cx_sy_arithmetic_error.
      sy-subrc = 10.
    CATCH cx_sy_conversion_error.
      sy-subrc = 9.
    catch cx_foev_error_in_function.
      perform error_message using 'RSAU' 'E' '510'
              'ROUTINE_0001' g_s_is-recno
              rs_c_false rs_c_false g_s_is-recno
              changing c_abort.
  ENDTRY.
  IF sy-subrc = 10 or
     sy-subrc = 9.
    PERFORM error_message USING 'RSAU' 'E' '507'
            'ROUTINE_0001' g_s_is-recno
            rs_c_false rs_c_false g_s_is-recno
            CHANGING c_abort.
  ENDIF.
ENDFORM.                    "routine_0001


************************************************************************
* cha_calculation
************************************************************************
FORM cha_calculation
  CHANGING  l_returncode  LIKE sy-subrc
            c_t_idocstate TYPE rsarr_t_idocstate
            c_subrc       LIKE sy-subrc
            c_abort       LIKE sy-subrc.                    "#EC *

  DATA:
    l_t_rsmondata LIKE rsmonview OCCURS 0 WITH HEADER LINE, "#EC *
    l_fscvtval TYPE rsau_s_updinfo-fscvtval,                "#EC *
    l_chavl TYPE rsd_chavl,                                 "#EC *
    l_date        TYPE dats,                                "#EC *
    l_timchavl    TYPE rsd_chavl,                           "#EC *
    l_timresult   TYPE rsd_chavl,                           "#EC *
    l_s_dep       TYPE rrsv_s_dep,                          "#EC *
    l_t_dep       TYPE rrsv_t_dep,                          "#EC *
    l_t_share     TYPE rstim_t_share,                       "#EC *
    l_s_share     TYPE rstim_s_share,                       "#EC *
    l_s_timvl     TYPE rstim_s_timvl,                       "#EC *
    l_chavl_in    TYPE rschavl60,                           "#EC *
    l_chavl_out   TYPE rschavl60.                           "#EC *

  c_abort = 0.
  g-DISTR_CHAN = g_s_is-DISTR_CHAN.

  g-DIVISION = g_s_is-DIVISION.

  g-DOC_TYPE = g_s_is-DOC_TYPE.

  g-DSDEL_DATE = g_s_is-DSDEL_DATE.

  g-MATAV_DATE = g_s_is-MATAV_DATE.

  g-MATERIAL = g_s_is-MATERIAL.

  g-ORD_TYPE = g_s_is-ORD_TYPE.

  g-PLANT = g_s_is-PLANT.

  g-REQ_DATE = g_s_is-REQ_DATE.

  g-SALESORG = g_s_is-SALESORG.

  g-SALES_OFF = g_s_is-SALES_OFF.

  g-SOLD_TO = g_s_is-SOLD_TO.

  g-/BIC/ZMATAVDAT = g_s_is-/BIC/ZMATAVDAT.

  g-LOAD_DATE = g_s_is-LOAD_DATE.

  g-/BIC/ZCMRCAT = g_s_is-/BIC/ZCMRCAT.

  g-/BIC/ZPTYP_GRP = g_s_is-/BIC/ZPTYP_GRP.

  g-TCTVAPRCTP = g_s_is-TCTVAPRCTP.

  g-SHIP_TO = g_s_is-SHIP_TO.

  g-/BIC/ZWORKDAY = g_s_is-/BIC/ZWORKDAY.

  g-CUST_SALES = g_s_is-CUST_SALES.

  g-/BIC/ZZORC = g_s_is-/BIC/ZZORC.

  g-DEALTYPE = g_s_is-DEALTYPE.

  g-ITEM_CATEG = g_s_is-ITEM_CATEG.

  g-DOC_NUMBER = g_s_is-DOC_NUMBER.

  g-MAT_PLANT = g_s_is-MAT_PLANT.

  g-/BIC/ZZREC = g_s_is-/BIC/ZZREC.

  g-/BIC/ZZRECMAN = g_s_is-/BIC/ZZRECMAN.

  g-S_ORD_ITEM = g_s_is-S_ORD_ITEM.

  g-PO_NUMBER = g_s_is-PO_NUMBER.

  g-/BIC/ZZCRSDRC = g_s_is-/BIC/ZZCRSDRC.

  g-/BIC/ZZOPDT = g_s_is-/BIC/ZZOPDT.

  g-/BIC/ZZPDT = g_s_is-/BIC/ZZPDT.

  g-FISCVARNT = 'MO'.

  g-CALDAY = g_s_is-CALDAY.


* save the changes
  g_s_cha_calculation = g.
* remember that saving
  g_record_no_cha_calculation = record_no.
ENDFORM.                    "cha_calculation
************************************************************************
* update rule no...: 0009
* update infoobject: OPNORDDLR
* update field.....: /BIC/OPNORDDLR
************************************************************************
FORM r0009_OPNORDDLR
  CHANGING c_wa_new       TYPE rs_bool
           c_val_set      TYPE rs_bool
           c_t_idocstate  TYPE rsarr_t_idocstate
           c_subrc        LIKE sy-subrc
           c_abort        LIKE sy-subrc.

  DATA:
    l_kyf         TYPE g_s_hashed_cube-/BIC/OPNORDDLR,
    l_fscvtval    TYPE rsau_s_updinfo-fscvtval,             "#EC *
    l_subrc       LIKE sy-subrc,                            "#EC *
    l_returncode  LIKE sy-subrc,                            "#EC *
    l_t_rsmondata LIKE rsmonview OCCURS 0 WITH HEADER LINE, "#EC *
    l_chavl       TYPE rsd_chavl,                           "#EC *
    l_date        TYPE dats,                                "#EC *
    l_timchavl    TYPE rsd_chavl,                           "#EC *
    l_timresult   TYPE rsd_chavl,                           "#EC *
    l_s_dep       TYPE rrsv_s_dep,                          "#EC *
    l_t_dep       TYPE rrsv_t_dep,                          "#EC *
    l_chavl_in    TYPE rschavl60,                             "#EC *
    l_chavl_out   TYPE rschavl60,                             "#EC *
    l_s_uom_val   TYPE rsuom_s_calc_runt_values,            "#EC *
    l_s_uom_prop  TYPE rsuom_s_calc_runt_prop_ext.          "#EC *

  IF g_break = rs_c_true. " any key figure
    BREAK-POINT.                                           "#EC NOBREAK
  ENDIF.
  c_abort = 0.
  TRY.                                                      "JD 180102
      CLEAR: g, g_error.


      IF g_record_no_cha_calculation <> record_no.
*   common characteristics calculation
        PERFORM cha_calculation
          CHANGING l_returncode c_t_idocstate c_subrc c_abort.
        IF l_returncode <> 0 OR c_subrc <> 0 OR c_abort <> 0.
          EXIT.
        ENDIF.
      ELSE.
*   don´t calculate, if same record
        g = g_s_cha_calculation.
      ENDIF.


      l_fscvtval = g-fiscvarnt.
      PERFORM time_conversion
                USING  '0CALDAY'
                       '0FISCPER'
                        g_s_is-CALDAY
                        l_fscvtval
                CHANGING g-FISCPER
                         c_t_idocstate
                         c_subrc
                         c_abort.
      IF c_subrc <> 0 OR c_abort <> 0.
        EXIT.
      ENDIF.
      l_fscvtval = g-fiscvarnt.
      PERFORM time_conversion
                USING  '0CALDAY'
                       '0FISCYEAR'
                        g_s_is-CALDAY
                        l_fscvtval
                CHANGING g-FISCYEAR
                         c_t_idocstate
                         c_subrc
                         c_abort.
      IF c_subrc <> 0 OR c_abort <> 0.
        EXIT.
      ENDIF.
      g-DOC_CURRCY = g_s_is-DOC_CURRCY.

        l_kyf = g_s_is-/BIC/OPNORDDLR.


              g_s_kb-DISTR_CHAN = g-DISTR_CHAN.
              g_s_kb-DIVISION = g-DIVISION.
              g_s_kb-DOC_TYPE = g-DOC_TYPE.
              g_s_kb-DSDEL_DATE = g-DSDEL_DATE.
              g_s_kb-MATAV_DATE = g-MATAV_DATE.
              g_s_kb-MATERIAL = g-MATERIAL.
              g_s_kb-ORD_TYPE = g-ORD_TYPE.
              g_s_kb-PLANT = g-PLANT.
              g_s_kb-REQ_DATE = g-REQ_DATE.
              g_s_kb-SALESORG = g-SALESORG.
              g_s_kb-SALES_OFF = g-SALES_OFF.
              g_s_kb-SOLD_TO = g-SOLD_TO.
              g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT.
              g_s_kb-LOAD_DATE = g-LOAD_DATE.
              g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT.
              g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP.
              g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP.
              g_s_kb-SHIP_TO = g-SHIP_TO.
              g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY.
              g_s_kb-REASON_REJ = g-REASON_REJ.
              g_s_kb-CREATEDON = g-CREATEDON.
              g_s_kb-CUST_SALES = g-CUST_SALES.
              g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG.
              g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC.
              g_s_kb-DEALTYPE = g-DEALTYPE.
              g_s_kb-ITEM_CATEG = g-ITEM_CATEG.
              g_s_kb-DELIV_NUMB = g-DELIV_NUMB.
              g_s_kb-DOC_NUMBER = g-DOC_NUMBER.
              g_s_kb-MAT_PLANT = g-MAT_PLANT.
              g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC.
              g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN.
              g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM.
              g_s_kb-PO_NUMBER = g-PO_NUMBER.
              g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC.
              g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT.
              g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT.
              g_s_kb-FISCVARNT = g-FISCVARNT.
              g_s_kb-CALDAY = g-CALDAY.
              g_s_kb-FISCPER = g-FISCPER.
              g_s_kb-FISCYEAR = g-FISCYEAR.
              g_s_kb-DOC_CURRCY = g-DOC_CURRCY.
              c_wa_new = rs_c_true.

            g_s_kb-/BIC/OPNORDDLR = g_s_kb-/BIC/OPNORDDLR + l_kyf.
            g_s_kb-fOPNORDDLR = rs_c_true.
            c_val_set = rs_c_true.
            g_s_kb-DOC_CURRCY = g-DOC_CURRCY.
      CATCH cx_rsfo_skip_record.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'S' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_skip_record_as_error.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_abort_package.
*     In der Formel wurde ein Abbruch ausgelöst.
        sy-subrc = 24.
        c_abort = c_subrc = sy-subrc.
        PERFORM abort_message
         USING 'RSAR' 'E' '514' sy-msgv1
          sy-msgv2 sy-msgv3 sy-msgv4 0 rs_c_true.
        EXIT.

      CATCH cx_sy_arithmetic_error.
        sy-subrc = 10.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0009_OPNORDDLR' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_sy_conversion_error.
        sy-subrc = 9.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0009_OPNORDDLR' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_dynamic_check INTO g_error_in_formula.

        WHILE g_error_in_formula->previous IS BOUND.
          g_error_in_formula = g_error_in_formula->previous.
        ENDWHILE.
        CALL METHOD g_error_in_formula->get_text
          RECEIVING
            result = g_previous.

        sy-subrc = 20.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '194'
                'OPNORDDLR' g_previous
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.
    ENDTRY.

*BEGIN TB09042002 HW510455
*END TB09042002

  ENDFORM.                    "r0009_OPNORDDLR
************************************************************************
* update rule no...: 0010
* update infoobject: OPNORDQTY
* update field.....: /BIC/OPNORDQTY
************************************************************************
FORM r0010_OPNORDQTY
  CHANGING c_wa_new       TYPE rs_bool
           c_val_set      TYPE rs_bool
           c_t_idocstate  TYPE rsarr_t_idocstate
           c_subrc        LIKE sy-subrc
           c_abort        LIKE sy-subrc.

  DATA:
    l_kyf         TYPE g_s_hashed_cube-/BIC/OPNORDQTY,
    l_fscvtval    TYPE rsau_s_updinfo-fscvtval,             "#EC *
    l_subrc       LIKE sy-subrc,                            "#EC *
    l_returncode  LIKE sy-subrc,                            "#EC *
    l_t_rsmondata LIKE rsmonview OCCURS 0 WITH HEADER LINE, "#EC *
    l_chavl       TYPE rsd_chavl,                           "#EC *
    l_date        TYPE dats,                                "#EC *
    l_timchavl    TYPE rsd_chavl,                           "#EC *
    l_timresult   TYPE rsd_chavl,                           "#EC *
    l_s_dep       TYPE rrsv_s_dep,                          "#EC *
    l_t_dep       TYPE rrsv_t_dep,                          "#EC *
    l_chavl_in    TYPE rschavl60,                             "#EC *
    l_chavl_out   TYPE rschavl60,                             "#EC *
    l_s_uom_val   TYPE rsuom_s_calc_runt_values,            "#EC *
    l_s_uom_prop  TYPE rsuom_s_calc_runt_prop_ext.          "#EC *

  IF g_break = rs_c_true. " any key figure
    BREAK-POINT.                                           "#EC NOBREAK
  ENDIF.
  c_abort = 0.
  TRY.                                                      "JD 180102
      CLEAR: g, g_error.


      IF g_record_no_cha_calculation <> record_no.
*   common characteristics calculation
        PERFORM cha_calculation
          CHANGING l_returncode c_t_idocstate c_subrc c_abort.
        IF l_returncode <> 0 OR c_subrc <> 0 OR c_abort <> 0.
          EXIT.
        ENDIF.
      ELSE.
*   don´t calculate, if same record
        g = g_s_cha_calculation.
      ENDIF.


      l_fscvtval = g-fiscvarnt.
      PERFORM time_conversion
                USING  '0CALDAY'
                       '0FISCPER'
                        g_s_is-CALDAY
                        l_fscvtval
                CHANGING g-FISCPER
                         c_t_idocstate
                         c_subrc
                         c_abort.
      IF c_subrc <> 0 OR c_abort <> 0.
        EXIT.
      ENDIF.
      l_fscvtval = g-fiscvarnt.
      PERFORM time_conversion
                USING  '0CALDAY'
                       '0FISCYEAR'
                        g_s_is-CALDAY
                        l_fscvtval
                CHANGING g-FISCYEAR
                         c_t_idocstate
                         c_subrc
                         c_abort.
      IF c_subrc <> 0 OR c_abort <> 0.
        EXIT.
      ENDIF.
      g-SALES_UNIT = g_s_is-SALES_UNIT.

        l_kyf = g_s_is-/BIC/OPNORDQTY.


          IF NOT g_s_kb IS INITIAL
              AND   g_s_kb-DISTR_CHAN = g-DISTR_CHAN
              AND   g_s_kb-DIVISION = g-DIVISION
              AND   g_s_kb-DOC_TYPE = g-DOC_TYPE
              AND   g_s_kb-DSDEL_DATE = g-DSDEL_DATE
              AND   g_s_kb-MATAV_DATE = g-MATAV_DATE
              AND   g_s_kb-MATERIAL = g-MATERIAL
              AND   g_s_kb-ORD_TYPE = g-ORD_TYPE
              AND   g_s_kb-PLANT = g-PLANT
              AND   g_s_kb-REQ_DATE = g-REQ_DATE
              AND   g_s_kb-SALESORG = g-SALESORG
              AND   g_s_kb-SALES_OFF = g-SALES_OFF
              AND   g_s_kb-SOLD_TO = g-SOLD_TO
              AND   g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              AND   g_s_kb-LOAD_DATE = g-LOAD_DATE
              AND   g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              AND   g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              AND   g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP
              AND   g_s_kb-SHIP_TO = g-SHIP_TO
              AND   g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              AND   g_s_kb-REASON_REJ = g-REASON_REJ
              AND   g_s_kb-CREATEDON = g-CREATEDON
              AND   g_s_kb-CUST_SALES = g-CUST_SALES
              AND   g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              AND   g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC
              AND   g_s_kb-DEALTYPE = g-DEALTYPE
              AND   g_s_kb-ITEM_CATEG = g-ITEM_CATEG
              AND   g_s_kb-DELIV_NUMB = g-DELIV_NUMB
              AND   g_s_kb-DOC_NUMBER = g-DOC_NUMBER
              AND   g_s_kb-MAT_PLANT = g-MAT_PLANT
              AND   g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC
              AND   g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              AND   g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM
              AND   g_s_kb-PO_NUMBER = g-PO_NUMBER
              AND   g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              AND   g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT
              AND   g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT
              AND   g_s_kb-FISCVARNT = g-FISCVARNT
              AND   g_s_kb-CALDAY = g-CALDAY
              AND   g_s_kb-FISCPER = g-FISCPER
              AND   g_s_kb-FISCYEAR = g-FISCYEAR
              AND   g_s_kb-SALES_UNIT = g-SALES_UNIT
                  .
            g_s_kb-/BIC/OPNORDQTY = g_s_kb-/BIC/OPNORDQTY + l_kyf.
            g_s_kb-fOPNORDQTY = rs_c_true.
            c_val_set = rs_c_true.
            g_s_kb-SALES_UNIT = g-SALES_UNIT.
          ELSE.
            IF c_val_set = rs_c_true.
              IF c_wa_new = rs_c_false.      "read from table !!!
                DELETE TABLE g_t_kb FROM g_s_kbtbx.
              ENDIF.
              INSERT g_s_kb INTO TABLE g_t_kb.
              c_val_set = rs_c_false.
              CLEAR g_s_kb.
            ENDIF.
            READ TABLE g_t_kb INTO g_s_kb WITH KEY
              DISTR_CHAN = g-DISTR_CHAN
              DIVISION = g-DIVISION
              DOC_TYPE = g-DOC_TYPE
              DSDEL_DATE = g-DSDEL_DATE
              MATAV_DATE = g-MATAV_DATE
              MATERIAL = g-MATERIAL
              ORD_TYPE = g-ORD_TYPE
              PLANT = g-PLANT
              REQ_DATE = g-REQ_DATE
              SALESORG = g-SALESORG
              SALES_OFF = g-SALES_OFF
              SOLD_TO = g-SOLD_TO
              /BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              LOAD_DATE = g-LOAD_DATE
              /BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              /BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              TCTVAPRCTP = g-TCTVAPRCTP
              SHIP_TO = g-SHIP_TO
              /BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              REASON_REJ = g-REASON_REJ
              CREATEDON = g-CREATEDON
              CUST_SALES = g-CUST_SALES
              /BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              /BIC/ZZORC = g-/BIC/ZZORC
              DEALTYPE = g-DEALTYPE
              ITEM_CATEG = g-ITEM_CATEG
              DELIV_NUMB = g-DELIV_NUMB
              DOC_NUMBER = g-DOC_NUMBER
              MAT_PLANT = g-MAT_PLANT
              /BIC/ZZREC = g-/BIC/ZZREC
              /BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              S_ORD_ITEM = g-S_ORD_ITEM
              PO_NUMBER = g-PO_NUMBER
              /BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              /BIC/ZZOPDT = g-/BIC/ZZOPDT
              /BIC/ZZPDT = g-/BIC/ZZPDT
              FISCVARNT = g-FISCVARNT
              CALDAY = g-CALDAY
              FISCPER = g-FISCPER
              FISCYEAR = g-FISCYEAR
              SALES_UNIT = g-SALES_UNIT
              .
            l_subrc = sy-subrc.
            IF l_subrc <> 0.
              READ TABLE g_t_kb INTO g_s_kb WITH KEY
                DISTR_CHAN = g-DISTR_CHAN
                DIVISION = g-DIVISION
                DOC_TYPE = g-DOC_TYPE
                DSDEL_DATE = g-DSDEL_DATE
                MATAV_DATE = g-MATAV_DATE
                MATERIAL = g-MATERIAL
                ORD_TYPE = g-ORD_TYPE
                PLANT = g-PLANT
                REQ_DATE = g-REQ_DATE
                SALESORG = g-SALESORG
                SALES_OFF = g-SALES_OFF
                SOLD_TO = g-SOLD_TO
                /BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
                LOAD_DATE = g-LOAD_DATE
                /BIC/ZCMRCAT = g-/BIC/ZCMRCAT
                /BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
                TCTVAPRCTP = g-TCTVAPRCTP
                SHIP_TO = g-SHIP_TO
                /BIC/ZWORKDAY = g-/BIC/ZWORKDAY
                REASON_REJ = g-REASON_REJ
                CREATEDON = g-CREATEDON
                CUST_SALES = g-CUST_SALES
                /BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
                /BIC/ZZORC = g-/BIC/ZZORC
                DEALTYPE = g-DEALTYPE
                ITEM_CATEG = g-ITEM_CATEG
                DELIV_NUMB = g-DELIV_NUMB
                DOC_NUMBER = g-DOC_NUMBER
                MAT_PLANT = g-MAT_PLANT
                /BIC/ZZREC = g-/BIC/ZZREC
                /BIC/ZZRECMAN = g-/BIC/ZZRECMAN
                S_ORD_ITEM = g-S_ORD_ITEM
                PO_NUMBER = g-PO_NUMBER
                /BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
                /BIC/ZZOPDT = g-/BIC/ZZOPDT
                /BIC/ZZPDT = g-/BIC/ZZPDT
                FISCVARNT = g-FISCVARNT
                CALDAY = g-CALDAY
                FISCPER = g-FISCPER
                FISCYEAR = g-FISCYEAR
                SALES_UNIT = space
                .
              l_subrc = sy-subrc.
            ENDIF.
            IF l_subrc = 0.
              g_s_kbtbx = g_s_kb.
              g_s_kb-SALES_UNIT = g-SALES_UNIT.
              c_wa_new = rs_c_false.
            ELSE.
              CLEAR g_s_kbtbx.
              CLEAR g_s_kb.
              g_s_kb-DISTR_CHAN = g-DISTR_CHAN.
              g_s_kb-DIVISION = g-DIVISION.
              g_s_kb-DOC_TYPE = g-DOC_TYPE.
              g_s_kb-DSDEL_DATE = g-DSDEL_DATE.
              g_s_kb-MATAV_DATE = g-MATAV_DATE.
              g_s_kb-MATERIAL = g-MATERIAL.
              g_s_kb-ORD_TYPE = g-ORD_TYPE.
              g_s_kb-PLANT = g-PLANT.
              g_s_kb-REQ_DATE = g-REQ_DATE.
              g_s_kb-SALESORG = g-SALESORG.
              g_s_kb-SALES_OFF = g-SALES_OFF.
              g_s_kb-SOLD_TO = g-SOLD_TO.
              g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT.
              g_s_kb-LOAD_DATE = g-LOAD_DATE.
              g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT.
              g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP.
              g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP.
              g_s_kb-SHIP_TO = g-SHIP_TO.
              g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY.
              g_s_kb-REASON_REJ = g-REASON_REJ.
              g_s_kb-CREATEDON = g-CREATEDON.
              g_s_kb-CUST_SALES = g-CUST_SALES.
              g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG.
              g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC.
              g_s_kb-DEALTYPE = g-DEALTYPE.
              g_s_kb-ITEM_CATEG = g-ITEM_CATEG.
              g_s_kb-DELIV_NUMB = g-DELIV_NUMB.
              g_s_kb-DOC_NUMBER = g-DOC_NUMBER.
              g_s_kb-MAT_PLANT = g-MAT_PLANT.
              g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC.
              g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN.
              g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM.
              g_s_kb-PO_NUMBER = g-PO_NUMBER.
              g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC.
              g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT.
              g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT.
              g_s_kb-FISCVARNT = g-FISCVARNT.
              g_s_kb-CALDAY = g-CALDAY.
              g_s_kb-FISCPER = g-FISCPER.
              g_s_kb-FISCYEAR = g-FISCYEAR.
              g_s_kb-SALES_UNIT = g-SALES_UNIT.
              c_wa_new = rs_c_true.
            ENDIF.

            g_s_kb-/BIC/OPNORDQTY = g_s_kb-/BIC/OPNORDQTY + l_kyf.
            g_s_kb-fOPNORDQTY = rs_c_true.
            c_val_set = rs_c_true.
            g_s_kb-SALES_UNIT = g-SALES_UNIT.
          ENDIF.
      CATCH cx_rsfo_skip_record.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'S' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_skip_record_as_error.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_abort_package.
*     In der Formel wurde ein Abbruch ausgelöst.
        sy-subrc = 24.
        c_abort = c_subrc = sy-subrc.
        PERFORM abort_message
         USING 'RSAR' 'E' '514' sy-msgv1
          sy-msgv2 sy-msgv3 sy-msgv4 0 rs_c_true.
        EXIT.

      CATCH cx_sy_arithmetic_error.
        sy-subrc = 10.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0010_OPNORDQTY' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_sy_conversion_error.
        sy-subrc = 9.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0010_OPNORDQTY' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_dynamic_check INTO g_error_in_formula.

        WHILE g_error_in_formula->previous IS BOUND.
          g_error_in_formula = g_error_in_formula->previous.
        ENDWHILE.
        CALL METHOD g_error_in_formula->get_text
          RECEIVING
            result = g_previous.

        sy-subrc = 20.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '194'
                'OPNORDQTY' g_previous
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.
    ENDTRY.

*BEGIN TB09042002 HW510455
*END TB09042002

  ENDFORM.                    "r0010_OPNORDQTY
************************************************************************
* update rule no...: 0011
* update infoobject: GIDATE
* update field.....: /BIC/GIDATE
************************************************************************
FORM r0011_GIDATE
  CHANGING c_wa_new       TYPE rs_bool
           c_val_set      TYPE rs_bool
           c_t_idocstate  TYPE rsarr_t_idocstate
           c_subrc        LIKE sy-subrc
           c_abort        LIKE sy-subrc.

  DATA:
    l_kyf         TYPE g_s_hashed_cube-/BIC/GIDATE,
    l_fscvtval    TYPE rsau_s_updinfo-fscvtval,             "#EC *
    l_subrc       LIKE sy-subrc,                            "#EC *
    l_returncode  LIKE sy-subrc,                            "#EC *
    l_t_rsmondata LIKE rsmonview OCCURS 0 WITH HEADER LINE, "#EC *
    l_chavl       TYPE rsd_chavl,                           "#EC *
    l_date        TYPE dats,                                "#EC *
    l_timchavl    TYPE rsd_chavl,                           "#EC *
    l_timresult   TYPE rsd_chavl,                           "#EC *
    l_s_dep       TYPE rrsv_s_dep,                          "#EC *
    l_t_dep       TYPE rrsv_t_dep,                          "#EC *
    l_chavl_in    TYPE rschavl60,                             "#EC *
    l_chavl_out   TYPE rschavl60,                             "#EC *
    l_s_uom_val   TYPE rsuom_s_calc_runt_values,            "#EC *
    l_s_uom_prop  TYPE rsuom_s_calc_runt_prop_ext.          "#EC *

  IF g_break = rs_c_true. " any key figure
    BREAK-POINT.                                           "#EC NOBREAK
  ENDIF.
  c_abort = 0.
  TRY.                                                      "JD 180102
      CLEAR: g, g_error.


      IF g_record_no_cha_calculation <> record_no.
*   common characteristics calculation
        PERFORM cha_calculation
          CHANGING l_returncode c_t_idocstate c_subrc c_abort.
        IF l_returncode <> 0 OR c_subrc <> 0 OR c_abort <> 0.
          EXIT.
        ENDIF.
      ELSE.
*   don´t calculate, if same record
        g = g_s_cha_calculation.
      ENDIF.


      l_fscvtval = g-fiscvarnt.
      PERFORM time_conversion
                USING  '0CALDAY'
                       '0FISCPER'
                        g_s_is-CALDAY
                        l_fscvtval
                CHANGING g-FISCPER
                         c_t_idocstate
                         c_subrc
                         c_abort.
      IF c_subrc <> 0 OR c_abort <> 0.
        EXIT.
      ENDIF.
      l_fscvtval = g-fiscvarnt.
      PERFORM time_conversion
                USING  '0CALDAY'
                       '0FISCYEAR'
                        g_s_is-CALDAY
                        l_fscvtval
                CHANGING g-FISCYEAR
                         c_t_idocstate
                         c_subrc
                         c_abort.
      IF c_subrc <> 0 OR c_abort <> 0.
        EXIT.
      ENDIF.

        l_kyf = g_s_is-/BIC/GIDATE.


          IF NOT g_s_kb IS INITIAL
              AND   g_s_kb-DISTR_CHAN = g-DISTR_CHAN
              AND   g_s_kb-DIVISION = g-DIVISION
              AND   g_s_kb-DOC_TYPE = g-DOC_TYPE
              AND   g_s_kb-DSDEL_DATE = g-DSDEL_DATE
              AND   g_s_kb-MATAV_DATE = g-MATAV_DATE
              AND   g_s_kb-MATERIAL = g-MATERIAL
              AND   g_s_kb-ORD_TYPE = g-ORD_TYPE
              AND   g_s_kb-PLANT = g-PLANT
              AND   g_s_kb-REQ_DATE = g-REQ_DATE
              AND   g_s_kb-SALESORG = g-SALESORG
              AND   g_s_kb-SALES_OFF = g-SALES_OFF
              AND   g_s_kb-SOLD_TO = g-SOLD_TO
              AND   g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              AND   g_s_kb-LOAD_DATE = g-LOAD_DATE
              AND   g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              AND   g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              AND   g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP
              AND   g_s_kb-SHIP_TO = g-SHIP_TO
              AND   g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              AND   g_s_kb-REASON_REJ = g-REASON_REJ
              AND   g_s_kb-CREATEDON = g-CREATEDON
              AND   g_s_kb-CUST_SALES = g-CUST_SALES
              AND   g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              AND   g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC
              AND   g_s_kb-DEALTYPE = g-DEALTYPE
              AND   g_s_kb-ITEM_CATEG = g-ITEM_CATEG
              AND   g_s_kb-DELIV_NUMB = g-DELIV_NUMB
              AND   g_s_kb-DOC_NUMBER = g-DOC_NUMBER
              AND   g_s_kb-MAT_PLANT = g-MAT_PLANT
              AND   g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC
              AND   g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              AND   g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM
              AND   g_s_kb-PO_NUMBER = g-PO_NUMBER
              AND   g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              AND   g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT
              AND   g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT
              AND   g_s_kb-FISCVARNT = g-FISCVARNT
              AND   g_s_kb-CALDAY = g-CALDAY
              AND   g_s_kb-FISCPER = g-FISCPER
              AND   g_s_kb-FISCYEAR = g-FISCYEAR
                  .
            IF g_s_kb-fGIDATE = rs_c_false OR
               g_s_kb-/BIC/GIDATE < l_kyf.
              g_s_kb-/BIC/GIDATE = l_kyf.
            ENDIF.
            g_s_kb-fGIDATE = rs_c_true.
            c_val_set = rs_c_true.
          ELSE.
            IF c_val_set = rs_c_true.
              IF c_wa_new = rs_c_false.      "read from table !!!
                DELETE TABLE g_t_kb FROM g_s_kbtbx.
              ENDIF.
              INSERT g_s_kb INTO TABLE g_t_kb.
              c_val_set = rs_c_false.
              CLEAR g_s_kb.
            ENDIF.
            READ TABLE g_t_kb INTO g_s_kb WITH KEY
              DISTR_CHAN = g-DISTR_CHAN
              DIVISION = g-DIVISION
              DOC_TYPE = g-DOC_TYPE
              DSDEL_DATE = g-DSDEL_DATE
              MATAV_DATE = g-MATAV_DATE
              MATERIAL = g-MATERIAL
              ORD_TYPE = g-ORD_TYPE
              PLANT = g-PLANT
              REQ_DATE = g-REQ_DATE
              SALESORG = g-SALESORG
              SALES_OFF = g-SALES_OFF
              SOLD_TO = g-SOLD_TO
              /BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              LOAD_DATE = g-LOAD_DATE
              /BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              /BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              TCTVAPRCTP = g-TCTVAPRCTP
              SHIP_TO = g-SHIP_TO
              /BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              REASON_REJ = g-REASON_REJ
              CREATEDON = g-CREATEDON
              CUST_SALES = g-CUST_SALES
              /BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              /BIC/ZZORC = g-/BIC/ZZORC
              DEALTYPE = g-DEALTYPE
              ITEM_CATEG = g-ITEM_CATEG
              DELIV_NUMB = g-DELIV_NUMB
              DOC_NUMBER = g-DOC_NUMBER
              MAT_PLANT = g-MAT_PLANT
              /BIC/ZZREC = g-/BIC/ZZREC
              /BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              S_ORD_ITEM = g-S_ORD_ITEM
              PO_NUMBER = g-PO_NUMBER
              /BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              /BIC/ZZOPDT = g-/BIC/ZZOPDT
              /BIC/ZZPDT = g-/BIC/ZZPDT
              FISCVARNT = g-FISCVARNT
              CALDAY = g-CALDAY
              FISCPER = g-FISCPER
              FISCYEAR = g-FISCYEAR
              .
            l_subrc = sy-subrc.
            IF l_subrc = 0.
              g_s_kbtbx = g_s_kb.
              c_wa_new = rs_c_false.
            ELSE.
              CLEAR g_s_kbtbx.
              CLEAR g_s_kb.
              g_s_kb-DISTR_CHAN = g-DISTR_CHAN.
              g_s_kb-DIVISION = g-DIVISION.
              g_s_kb-DOC_TYPE = g-DOC_TYPE.
              g_s_kb-DSDEL_DATE = g-DSDEL_DATE.
              g_s_kb-MATAV_DATE = g-MATAV_DATE.
              g_s_kb-MATERIAL = g-MATERIAL.
              g_s_kb-ORD_TYPE = g-ORD_TYPE.
              g_s_kb-PLANT = g-PLANT.
              g_s_kb-REQ_DATE = g-REQ_DATE.
              g_s_kb-SALESORG = g-SALESORG.
              g_s_kb-SALES_OFF = g-SALES_OFF.
              g_s_kb-SOLD_TO = g-SOLD_TO.
              g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT.
              g_s_kb-LOAD_DATE = g-LOAD_DATE.
              g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT.
              g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP.
              g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP.
              g_s_kb-SHIP_TO = g-SHIP_TO.
              g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY.
              g_s_kb-REASON_REJ = g-REASON_REJ.
              g_s_kb-CREATEDON = g-CREATEDON.
              g_s_kb-CUST_SALES = g-CUST_SALES.
              g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG.
              g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC.
              g_s_kb-DEALTYPE = g-DEALTYPE.
              g_s_kb-ITEM_CATEG = g-ITEM_CATEG.
              g_s_kb-DELIV_NUMB = g-DELIV_NUMB.
              g_s_kb-DOC_NUMBER = g-DOC_NUMBER.
              g_s_kb-MAT_PLANT = g-MAT_PLANT.
              g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC.
              g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN.
              g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM.
              g_s_kb-PO_NUMBER = g-PO_NUMBER.
              g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC.
              g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT.
              g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT.
              g_s_kb-FISCVARNT = g-FISCVARNT.
              g_s_kb-CALDAY = g-CALDAY.
              g_s_kb-FISCPER = g-FISCPER.
              g_s_kb-FISCYEAR = g-FISCYEAR.
              c_wa_new = rs_c_true.
            ENDIF.

            IF g_s_kb-fGIDATE = rs_c_false OR
               g_s_kb-/BIC/GIDATE < l_kyf.
              g_s_kb-/BIC/GIDATE = l_kyf.
            ENDIF.
            g_s_kb-fGIDATE = rs_c_true.
            c_val_set = rs_c_true.
          ENDIF.
      CATCH cx_rsfo_skip_record.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'S' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_skip_record_as_error.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_abort_package.
*     In der Formel wurde ein Abbruch ausgelöst.
        sy-subrc = 24.
        c_abort = c_subrc = sy-subrc.
        PERFORM abort_message
         USING 'RSAR' 'E' '514' sy-msgv1
          sy-msgv2 sy-msgv3 sy-msgv4 0 rs_c_true.
        EXIT.

      CATCH cx_sy_arithmetic_error.
        sy-subrc = 10.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0011_GIDATE' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_sy_conversion_error.
        sy-subrc = 9.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0011_GIDATE' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_dynamic_check INTO g_error_in_formula.

        WHILE g_error_in_formula->previous IS BOUND.
          g_error_in_formula = g_error_in_formula->previous.
        ENDWHILE.
        CALL METHOD g_error_in_formula->get_text
          RECEIVING
            result = g_previous.

        sy-subrc = 20.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '194'
                'GIDATE' g_previous
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.
    ENDTRY.

*BEGIN TB09042002 HW510455
*END TB09042002

  ENDFORM.                    "r0011_GIDATE
************************************************************************
* update rule no...: 0012
* update infoobject: REQDATE
* update field.....: /BIC/REQDATE
************************************************************************
FORM r0012_REQDATE
  CHANGING c_wa_new       TYPE rs_bool
           c_val_set      TYPE rs_bool
           c_t_idocstate  TYPE rsarr_t_idocstate
           c_subrc        LIKE sy-subrc
           c_abort        LIKE sy-subrc.

  DATA:
    l_kyf         TYPE g_s_hashed_cube-/BIC/REQDATE,
    l_fscvtval    TYPE rsau_s_updinfo-fscvtval,             "#EC *
    l_subrc       LIKE sy-subrc,                            "#EC *
    l_returncode  LIKE sy-subrc,                            "#EC *
    l_t_rsmondata LIKE rsmonview OCCURS 0 WITH HEADER LINE, "#EC *
    l_chavl       TYPE rsd_chavl,                           "#EC *
    l_date        TYPE dats,                                "#EC *
    l_timchavl    TYPE rsd_chavl,                           "#EC *
    l_timresult   TYPE rsd_chavl,                           "#EC *
    l_s_dep       TYPE rrsv_s_dep,                          "#EC *
    l_t_dep       TYPE rrsv_t_dep,                          "#EC *
    l_chavl_in    TYPE rschavl60,                             "#EC *
    l_chavl_out   TYPE rschavl60,                             "#EC *
    l_s_uom_val   TYPE rsuom_s_calc_runt_values,            "#EC *
    l_s_uom_prop  TYPE rsuom_s_calc_runt_prop_ext.          "#EC *

  IF g_break = rs_c_true. " any key figure
    BREAK-POINT.                                           "#EC NOBREAK
  ENDIF.
  c_abort = 0.
  TRY.                                                      "JD 180102
      CLEAR: g, g_error.


      IF g_record_no_cha_calculation <> record_no.
*   common characteristics calculation
        PERFORM cha_calculation
          CHANGING l_returncode c_t_idocstate c_subrc c_abort.
        IF l_returncode <> 0 OR c_subrc <> 0 OR c_abort <> 0.
          EXIT.
        ENDIF.
      ELSE.
*   don´t calculate, if same record
        g = g_s_cha_calculation.
      ENDIF.


      l_fscvtval = g-fiscvarnt.
      PERFORM time_conversion
                USING  '0CALDAY'
                       '0FISCPER'
                        g_s_is-CALDAY
                        l_fscvtval
                CHANGING g-FISCPER
                         c_t_idocstate
                         c_subrc
                         c_abort.
      IF c_subrc <> 0 OR c_abort <> 0.
        EXIT.
      ENDIF.
      l_fscvtval = g-fiscvarnt.
      PERFORM time_conversion
                USING  '0CALDAY'
                       '0FISCYEAR'
                        g_s_is-CALDAY
                        l_fscvtval
                CHANGING g-FISCYEAR
                         c_t_idocstate
                         c_subrc
                         c_abort.
      IF c_subrc <> 0 OR c_abort <> 0.
        EXIT.
      ENDIF.

        l_kyf = g_s_is-/BIC/REQDATE.


          IF NOT g_s_kb IS INITIAL
              AND   g_s_kb-DISTR_CHAN = g-DISTR_CHAN
              AND   g_s_kb-DIVISION = g-DIVISION
              AND   g_s_kb-DOC_TYPE = g-DOC_TYPE
              AND   g_s_kb-DSDEL_DATE = g-DSDEL_DATE
              AND   g_s_kb-MATAV_DATE = g-MATAV_DATE
              AND   g_s_kb-MATERIAL = g-MATERIAL
              AND   g_s_kb-ORD_TYPE = g-ORD_TYPE
              AND   g_s_kb-PLANT = g-PLANT
              AND   g_s_kb-REQ_DATE = g-REQ_DATE
              AND   g_s_kb-SALESORG = g-SALESORG
              AND   g_s_kb-SALES_OFF = g-SALES_OFF
              AND   g_s_kb-SOLD_TO = g-SOLD_TO
              AND   g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              AND   g_s_kb-LOAD_DATE = g-LOAD_DATE
              AND   g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              AND   g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              AND   g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP
              AND   g_s_kb-SHIP_TO = g-SHIP_TO
              AND   g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              AND   g_s_kb-REASON_REJ = g-REASON_REJ
              AND   g_s_kb-CREATEDON = g-CREATEDON
              AND   g_s_kb-CUST_SALES = g-CUST_SALES
              AND   g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              AND   g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC
              AND   g_s_kb-DEALTYPE = g-DEALTYPE
              AND   g_s_kb-ITEM_CATEG = g-ITEM_CATEG
              AND   g_s_kb-DELIV_NUMB = g-DELIV_NUMB
              AND   g_s_kb-DOC_NUMBER = g-DOC_NUMBER
              AND   g_s_kb-MAT_PLANT = g-MAT_PLANT
              AND   g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC
              AND   g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              AND   g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM
              AND   g_s_kb-PO_NUMBER = g-PO_NUMBER
              AND   g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              AND   g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT
              AND   g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT
              AND   g_s_kb-FISCVARNT = g-FISCVARNT
              AND   g_s_kb-CALDAY = g-CALDAY
              AND   g_s_kb-FISCPER = g-FISCPER
              AND   g_s_kb-FISCYEAR = g-FISCYEAR
                  .
            IF g_s_kb-fREQDATE = rs_c_false OR
               g_s_kb-/BIC/REQDATE < l_kyf.
              g_s_kb-/BIC/REQDATE = l_kyf.
            ENDIF.
            g_s_kb-fREQDATE = rs_c_true.
            c_val_set = rs_c_true.
          ELSE.
            IF c_val_set = rs_c_true.
              IF c_wa_new = rs_c_false.      "read from table !!!
                DELETE TABLE g_t_kb FROM g_s_kbtbx.
              ENDIF.
              INSERT g_s_kb INTO TABLE g_t_kb.
              c_val_set = rs_c_false.
              CLEAR g_s_kb.
            ENDIF.
            READ TABLE g_t_kb INTO g_s_kb WITH KEY
              DISTR_CHAN = g-DISTR_CHAN
              DIVISION = g-DIVISION
              DOC_TYPE = g-DOC_TYPE
              DSDEL_DATE = g-DSDEL_DATE
              MATAV_DATE = g-MATAV_DATE
              MATERIAL = g-MATERIAL
              ORD_TYPE = g-ORD_TYPE
              PLANT = g-PLANT
              REQ_DATE = g-REQ_DATE
              SALESORG = g-SALESORG
              SALES_OFF = g-SALES_OFF
              SOLD_TO = g-SOLD_TO
              /BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              LOAD_DATE = g-LOAD_DATE
              /BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              /BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              TCTVAPRCTP = g-TCTVAPRCTP
              SHIP_TO = g-SHIP_TO
              /BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              REASON_REJ = g-REASON_REJ
              CREATEDON = g-CREATEDON
              CUST_SALES = g-CUST_SALES
              /BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              /BIC/ZZORC = g-/BIC/ZZORC
              DEALTYPE = g-DEALTYPE
              ITEM_CATEG = g-ITEM_CATEG
              DELIV_NUMB = g-DELIV_NUMB
              DOC_NUMBER = g-DOC_NUMBER
              MAT_PLANT = g-MAT_PLANT
              /BIC/ZZREC = g-/BIC/ZZREC
              /BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              S_ORD_ITEM = g-S_ORD_ITEM
              PO_NUMBER = g-PO_NUMBER
              /BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              /BIC/ZZOPDT = g-/BIC/ZZOPDT
              /BIC/ZZPDT = g-/BIC/ZZPDT
              FISCVARNT = g-FISCVARNT
              CALDAY = g-CALDAY
              FISCPER = g-FISCPER
              FISCYEAR = g-FISCYEAR
              .
            l_subrc = sy-subrc.
            IF l_subrc = 0.
              g_s_kbtbx = g_s_kb.
              c_wa_new = rs_c_false.
            ELSE.
              CLEAR g_s_kbtbx.
              CLEAR g_s_kb.
              g_s_kb-DISTR_CHAN = g-DISTR_CHAN.
              g_s_kb-DIVISION = g-DIVISION.
              g_s_kb-DOC_TYPE = g-DOC_TYPE.
              g_s_kb-DSDEL_DATE = g-DSDEL_DATE.
              g_s_kb-MATAV_DATE = g-MATAV_DATE.
              g_s_kb-MATERIAL = g-MATERIAL.
              g_s_kb-ORD_TYPE = g-ORD_TYPE.
              g_s_kb-PLANT = g-PLANT.
              g_s_kb-REQ_DATE = g-REQ_DATE.
              g_s_kb-SALESORG = g-SALESORG.
              g_s_kb-SALES_OFF = g-SALES_OFF.
              g_s_kb-SOLD_TO = g-SOLD_TO.
              g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT.
              g_s_kb-LOAD_DATE = g-LOAD_DATE.
              g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT.
              g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP.
              g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP.
              g_s_kb-SHIP_TO = g-SHIP_TO.
              g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY.
              g_s_kb-REASON_REJ = g-REASON_REJ.
              g_s_kb-CREATEDON = g-CREATEDON.
              g_s_kb-CUST_SALES = g-CUST_SALES.
              g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG.
              g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC.
              g_s_kb-DEALTYPE = g-DEALTYPE.
              g_s_kb-ITEM_CATEG = g-ITEM_CATEG.
              g_s_kb-DELIV_NUMB = g-DELIV_NUMB.
              g_s_kb-DOC_NUMBER = g-DOC_NUMBER.
              g_s_kb-MAT_PLANT = g-MAT_PLANT.
              g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC.
              g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN.
              g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM.
              g_s_kb-PO_NUMBER = g-PO_NUMBER.
              g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC.
              g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT.
              g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT.
              g_s_kb-FISCVARNT = g-FISCVARNT.
              g_s_kb-CALDAY = g-CALDAY.
              g_s_kb-FISCPER = g-FISCPER.
              g_s_kb-FISCYEAR = g-FISCYEAR.
              c_wa_new = rs_c_true.
            ENDIF.

            IF g_s_kb-fREQDATE = rs_c_false OR
               g_s_kb-/BIC/REQDATE < l_kyf.
              g_s_kb-/BIC/REQDATE = l_kyf.
            ENDIF.
            g_s_kb-fREQDATE = rs_c_true.
            c_val_set = rs_c_true.
          ENDIF.
      CATCH cx_rsfo_skip_record.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'S' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_skip_record_as_error.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_abort_package.
*     In der Formel wurde ein Abbruch ausgelöst.
        sy-subrc = 24.
        c_abort = c_subrc = sy-subrc.
        PERFORM abort_message
         USING 'RSAR' 'E' '514' sy-msgv1
          sy-msgv2 sy-msgv3 sy-msgv4 0 rs_c_true.
        EXIT.

      CATCH cx_sy_arithmetic_error.
        sy-subrc = 10.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0012_REQDATE' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_sy_conversion_error.
        sy-subrc = 9.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0012_REQDATE' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_dynamic_check INTO g_error_in_formula.

        WHILE g_error_in_formula->previous IS BOUND.
          g_error_in_formula = g_error_in_formula->previous.
        ENDWHILE.
        CALL METHOD g_error_in_formula->get_text
          RECEIVING
            result = g_previous.

        sy-subrc = 20.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '194'
                'REQDATE' g_previous
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.
    ENDTRY.

*BEGIN TB09042002 HW510455
*END TB09042002

  ENDFORM.                    "r0012_REQDATE
************************************************************************
* update rule no...: 0016
* update infoobject: 0CONF_QTY
* update field.....: CONF_QTY
************************************************************************
FORM r0016_0CONF_QTY
  CHANGING c_wa_new       TYPE rs_bool
           c_val_set      TYPE rs_bool
           c_t_idocstate  TYPE rsarr_t_idocstate
           c_subrc        LIKE sy-subrc
           c_abort        LIKE sy-subrc.

  DATA:
    l_kyf         TYPE g_s_hashed_cube-CONF_QTY,
    l_fscvtval    TYPE rsau_s_updinfo-fscvtval,             "#EC *
    l_subrc       LIKE sy-subrc,                            "#EC *
    l_returncode  LIKE sy-subrc,                            "#EC *
    l_t_rsmondata LIKE rsmonview OCCURS 0 WITH HEADER LINE, "#EC *
    l_chavl       TYPE rsd_chavl,                           "#EC *
    l_date        TYPE dats,                                "#EC *
    l_timchavl    TYPE rsd_chavl,                           "#EC *
    l_timresult   TYPE rsd_chavl,                           "#EC *
    l_s_dep       TYPE rrsv_s_dep,                          "#EC *
    l_t_dep       TYPE rrsv_t_dep,                          "#EC *
    l_chavl_in    TYPE rschavl60,                             "#EC *
    l_chavl_out   TYPE rschavl60,                             "#EC *
    l_s_uom_val   TYPE rsuom_s_calc_runt_values,            "#EC *
    l_s_uom_prop  TYPE rsuom_s_calc_runt_prop_ext.          "#EC *

  IF g_break = rs_c_true. " any key figure
    BREAK-POINT.                                           "#EC NOBREAK
  ENDIF.
  c_abort = 0.
  TRY.                                                      "JD 180102
      CLEAR: g, g_error.


      IF g_record_no_cha_calculation <> record_no.
*   common characteristics calculation
        PERFORM cha_calculation
          CHANGING l_returncode c_t_idocstate c_subrc c_abort.
        IF l_returncode <> 0 OR c_subrc <> 0 OR c_abort <> 0.
          EXIT.
        ENDIF.
      ELSE.
*   don´t calculate, if same record
        g = g_s_cha_calculation.
      ENDIF.


      g-FISCPER = g_s_is-FISCPER.
      g-FISCYEAR = g_s_is-FISCYEAR.
      g-SALES_UNIT = g_s_is-SALES_UNIT.

        l_kyf = g_s_is-CONF_QTY.


          IF NOT g_s_kb IS INITIAL
              AND   g_s_kb-CREATEDON = g-CREATEDON
              AND   g_s_kb-CUST_SALES = g-CUST_SALES
              AND   g_s_kb-DEALTYPE = g-DEALTYPE
              AND   g_s_kb-DISTR_CHAN = g-DISTR_CHAN
              AND   g_s_kb-DIVISION = g-DIVISION
              AND   g_s_kb-DOC_TYPE = g-DOC_TYPE
              AND   g_s_kb-DSDEL_DATE = g-DSDEL_DATE
              AND   g_s_kb-ITEM_CATEG = g-ITEM_CATEG
              AND   g_s_kb-LOAD_DATE = g-LOAD_DATE
              AND   g_s_kb-MATAV_DATE = g-MATAV_DATE
              AND   g_s_kb-MATERIAL = g-MATERIAL
              AND   g_s_kb-ORD_TYPE = g-ORD_TYPE
              AND   g_s_kb-PLANT = g-PLANT
              AND   g_s_kb-REASON_REJ = g-REASON_REJ
              AND   g_s_kb-REQ_DATE = g-REQ_DATE
              AND   g_s_kb-SALESORG = g-SALESORG
              AND   g_s_kb-SALES_OFF = g-SALES_OFF
              AND   g_s_kb-SHIP_TO = g-SHIP_TO
              AND   g_s_kb-SOLD_TO = g-SOLD_TO
              AND   g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP
              AND   g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              AND   g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              AND   g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              AND   g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              AND   g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              AND   g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC
              AND   g_s_kb-DELIV_NUMB = g-DELIV_NUMB
              AND   g_s_kb-DOC_NUMBER = g-DOC_NUMBER
              AND   g_s_kb-MAT_PLANT = g-MAT_PLANT
              AND   g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC
              AND   g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              AND   g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM
              AND   g_s_kb-PO_NUMBER = g-PO_NUMBER
              AND   g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              AND   g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT
              AND   g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT
              AND   g_s_kb-FISCVARNT = g-FISCVARNT
              AND   g_s_kb-CALDAY = g-CALDAY
              AND   g_s_kb-FISCPER = g-FISCPER
              AND   g_s_kb-FISCYEAR = g-FISCYEAR
              AND   g_s_kb-SALES_UNIT = g-SALES_UNIT
                  .
            g_s_kb-CONF_QTY = g_s_kb-CONF_QTY + l_kyf.
            g_s_kb-f0CONF_QTY = rs_c_true.
            c_val_set = rs_c_true.
            g_s_kb-SALES_UNIT = g-SALES_UNIT.
          ELSE.
            IF c_val_set = rs_c_true.
              IF c_wa_new = rs_c_false.      "read from table !!!
                DELETE TABLE g_t_kb FROM g_s_kbtbx.
              ENDIF.
              INSERT g_s_kb INTO TABLE g_t_kb.
              c_val_set = rs_c_false.
              CLEAR g_s_kb.
            ENDIF.
            READ TABLE g_t_kb INTO g_s_kb WITH KEY
              CREATEDON = g-CREATEDON
              CUST_SALES = g-CUST_SALES
              DEALTYPE = g-DEALTYPE
              DISTR_CHAN = g-DISTR_CHAN
              DIVISION = g-DIVISION
              DOC_TYPE = g-DOC_TYPE
              DSDEL_DATE = g-DSDEL_DATE
              ITEM_CATEG = g-ITEM_CATEG
              LOAD_DATE = g-LOAD_DATE
              MATAV_DATE = g-MATAV_DATE
              MATERIAL = g-MATERIAL
              ORD_TYPE = g-ORD_TYPE
              PLANT = g-PLANT
              REASON_REJ = g-REASON_REJ
              REQ_DATE = g-REQ_DATE
              SALESORG = g-SALESORG
              SALES_OFF = g-SALES_OFF
              SHIP_TO = g-SHIP_TO
              SOLD_TO = g-SOLD_TO
              TCTVAPRCTP = g-TCTVAPRCTP
              /BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              /BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              /BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              /BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              /BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              /BIC/ZZORC = g-/BIC/ZZORC
              DELIV_NUMB = g-DELIV_NUMB
              DOC_NUMBER = g-DOC_NUMBER
              MAT_PLANT = g-MAT_PLANT
              /BIC/ZZREC = g-/BIC/ZZREC
              /BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              S_ORD_ITEM = g-S_ORD_ITEM
              PO_NUMBER = g-PO_NUMBER
              /BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              /BIC/ZZOPDT = g-/BIC/ZZOPDT
              /BIC/ZZPDT = g-/BIC/ZZPDT
              FISCVARNT = g-FISCVARNT
              CALDAY = g-CALDAY
              FISCPER = g-FISCPER
              FISCYEAR = g-FISCYEAR
              SALES_UNIT = g-SALES_UNIT
              .
            l_subrc = sy-subrc.
            IF l_subrc <> 0.
              READ TABLE g_t_kb INTO g_s_kb WITH KEY
                CREATEDON = g-CREATEDON
                CUST_SALES = g-CUST_SALES
                DEALTYPE = g-DEALTYPE
                DISTR_CHAN = g-DISTR_CHAN
                DIVISION = g-DIVISION
                DOC_TYPE = g-DOC_TYPE
                DSDEL_DATE = g-DSDEL_DATE
                ITEM_CATEG = g-ITEM_CATEG
                LOAD_DATE = g-LOAD_DATE
                MATAV_DATE = g-MATAV_DATE
                MATERIAL = g-MATERIAL
                ORD_TYPE = g-ORD_TYPE
                PLANT = g-PLANT
                REASON_REJ = g-REASON_REJ
                REQ_DATE = g-REQ_DATE
                SALESORG = g-SALESORG
                SALES_OFF = g-SALES_OFF
                SHIP_TO = g-SHIP_TO
                SOLD_TO = g-SOLD_TO
                TCTVAPRCTP = g-TCTVAPRCTP
                /BIC/ZCMRCAT = g-/BIC/ZCMRCAT
                /BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
                /BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
                /BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
                /BIC/ZWORKDAY = g-/BIC/ZWORKDAY
                /BIC/ZZORC = g-/BIC/ZZORC
                DELIV_NUMB = g-DELIV_NUMB
                DOC_NUMBER = g-DOC_NUMBER
                MAT_PLANT = g-MAT_PLANT
                /BIC/ZZREC = g-/BIC/ZZREC
                /BIC/ZZRECMAN = g-/BIC/ZZRECMAN
                S_ORD_ITEM = g-S_ORD_ITEM
                PO_NUMBER = g-PO_NUMBER
                /BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
                /BIC/ZZOPDT = g-/BIC/ZZOPDT
                /BIC/ZZPDT = g-/BIC/ZZPDT
                FISCVARNT = g-FISCVARNT
                CALDAY = g-CALDAY
                FISCPER = g-FISCPER
                FISCYEAR = g-FISCYEAR
                SALES_UNIT = space
                .
              l_subrc = sy-subrc.
            ENDIF.
            IF l_subrc = 0.
              g_s_kbtbx = g_s_kb.
              g_s_kb-SALES_UNIT = g-SALES_UNIT.
              c_wa_new = rs_c_false.
            ELSE.
              CLEAR g_s_kbtbx.
              CLEAR g_s_kb.
              g_s_kb-CREATEDON = g-CREATEDON.
              g_s_kb-CUST_SALES = g-CUST_SALES.
              g_s_kb-DEALTYPE = g-DEALTYPE.
              g_s_kb-DISTR_CHAN = g-DISTR_CHAN.
              g_s_kb-DIVISION = g-DIVISION.
              g_s_kb-DOC_TYPE = g-DOC_TYPE.
              g_s_kb-DSDEL_DATE = g-DSDEL_DATE.
              g_s_kb-ITEM_CATEG = g-ITEM_CATEG.
              g_s_kb-LOAD_DATE = g-LOAD_DATE.
              g_s_kb-MATAV_DATE = g-MATAV_DATE.
              g_s_kb-MATERIAL = g-MATERIAL.
              g_s_kb-ORD_TYPE = g-ORD_TYPE.
              g_s_kb-PLANT = g-PLANT.
              g_s_kb-REASON_REJ = g-REASON_REJ.
              g_s_kb-REQ_DATE = g-REQ_DATE.
              g_s_kb-SALESORG = g-SALESORG.
              g_s_kb-SALES_OFF = g-SALES_OFF.
              g_s_kb-SHIP_TO = g-SHIP_TO.
              g_s_kb-SOLD_TO = g-SOLD_TO.
              g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP.
              g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT.
              g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT.
              g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG.
              g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP.
              g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY.
              g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC.
              g_s_kb-DELIV_NUMB = g-DELIV_NUMB.
              g_s_kb-DOC_NUMBER = g-DOC_NUMBER.
              g_s_kb-MAT_PLANT = g-MAT_PLANT.
              g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC.
              g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN.
              g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM.
              g_s_kb-PO_NUMBER = g-PO_NUMBER.
              g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC.
              g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT.
              g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT.
              g_s_kb-FISCVARNT = g-FISCVARNT.
              g_s_kb-CALDAY = g-CALDAY.
              g_s_kb-FISCPER = g-FISCPER.
              g_s_kb-FISCYEAR = g-FISCYEAR.
              g_s_kb-SALES_UNIT = g-SALES_UNIT.
              c_wa_new = rs_c_true.
            ENDIF.

            g_s_kb-CONF_QTY = g_s_kb-CONF_QTY + l_kyf.
            g_s_kb-f0CONF_QTY = rs_c_true.
            c_val_set = rs_c_true.
            g_s_kb-SALES_UNIT = g-SALES_UNIT.
          ENDIF.
      CATCH cx_rsfo_skip_record.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'S' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_skip_record_as_error.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_abort_package.
*     In der Formel wurde ein Abbruch ausgelöst.
        sy-subrc = 24.
        c_abort = c_subrc = sy-subrc.
        PERFORM abort_message
         USING 'RSAR' 'E' '514' sy-msgv1
          sy-msgv2 sy-msgv3 sy-msgv4 0 rs_c_true.
        EXIT.

      CATCH cx_sy_arithmetic_error.
        sy-subrc = 10.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0016_0CONF_QTY' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_sy_conversion_error.
        sy-subrc = 9.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0016_0CONF_QTY' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_dynamic_check INTO g_error_in_formula.

        WHILE g_error_in_formula->previous IS BOUND.
          g_error_in_formula = g_error_in_formula->previous.
        ENDWHILE.
        CALL METHOD g_error_in_formula->get_text
          RECEIVING
            result = g_previous.

        sy-subrc = 20.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '194'
                '0CONF_QTY' g_previous
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.
    ENDTRY.

*BEGIN TB09042002 HW510455
*END TB09042002

  ENDFORM.                    "r0016_0CONF_QTY
************************************************************************
* update rule no...: 0017
* update infoobject: VVGBP
* update field.....: /BIC/VVGBP
************************************************************************
FORM r0017_VVGBP
  CHANGING c_wa_new       TYPE rs_bool
           c_val_set      TYPE rs_bool
           c_t_idocstate  TYPE rsarr_t_idocstate
           c_subrc        LIKE sy-subrc
           c_abort        LIKE sy-subrc.

  DATA:
    l_kyf         TYPE g_s_hashed_cube-/BIC/VVGBP,
    l_fscvtval    TYPE rsau_s_updinfo-fscvtval,             "#EC *
    l_subrc       LIKE sy-subrc,                            "#EC *
    l_returncode  LIKE sy-subrc,                            "#EC *
    l_t_rsmondata LIKE rsmonview OCCURS 0 WITH HEADER LINE, "#EC *
    l_chavl       TYPE rsd_chavl,                           "#EC *
    l_date        TYPE dats,                                "#EC *
    l_timchavl    TYPE rsd_chavl,                           "#EC *
    l_timresult   TYPE rsd_chavl,                           "#EC *
    l_s_dep       TYPE rrsv_s_dep,                          "#EC *
    l_t_dep       TYPE rrsv_t_dep,                          "#EC *
    l_chavl_in    TYPE rschavl60,                             "#EC *
    l_chavl_out   TYPE rschavl60,                             "#EC *
    l_s_uom_val   TYPE rsuom_s_calc_runt_values,            "#EC *
    l_s_uom_prop  TYPE rsuom_s_calc_runt_prop_ext.          "#EC *

  IF g_break = rs_c_true. " any key figure
    BREAK-POINT.                                           "#EC NOBREAK
  ENDIF.
  c_abort = 0.
  TRY.                                                      "JD 180102
      CLEAR: g, g_error.


      IF g_record_no_cha_calculation <> record_no.
*   common characteristics calculation
        PERFORM cha_calculation
          CHANGING l_returncode c_t_idocstate c_subrc c_abort.
        IF l_returncode <> 0 OR c_subrc <> 0 OR c_abort <> 0.
          EXIT.
        ENDIF.
      ELSE.
*   don´t calculate, if same record
        g = g_s_cha_calculation.
      ENDIF.


      g-FISCPER = g_s_is-FISCPER.
      g-FISCYEAR = g_s_is-FISCYEAR.
      g-CURRENCY = g_s_is-CURRENCY.

        l_kyf = g_s_is-/BIC/VVGBP.


          IF NOT g_s_kb IS INITIAL
              AND   g_s_kb-CREATEDON = g-CREATEDON
              AND   g_s_kb-CUST_SALES = g-CUST_SALES
              AND   g_s_kb-DEALTYPE = g-DEALTYPE
              AND   g_s_kb-DISTR_CHAN = g-DISTR_CHAN
              AND   g_s_kb-DIVISION = g-DIVISION
              AND   g_s_kb-DOC_TYPE = g-DOC_TYPE
              AND   g_s_kb-DSDEL_DATE = g-DSDEL_DATE
              AND   g_s_kb-ITEM_CATEG = g-ITEM_CATEG
              AND   g_s_kb-LOAD_DATE = g-LOAD_DATE
              AND   g_s_kb-MATAV_DATE = g-MATAV_DATE
              AND   g_s_kb-MATERIAL = g-MATERIAL
              AND   g_s_kb-ORD_TYPE = g-ORD_TYPE
              AND   g_s_kb-PLANT = g-PLANT
              AND   g_s_kb-REASON_REJ = g-REASON_REJ
              AND   g_s_kb-REQ_DATE = g-REQ_DATE
              AND   g_s_kb-SALESORG = g-SALESORG
              AND   g_s_kb-SALES_OFF = g-SALES_OFF
              AND   g_s_kb-SHIP_TO = g-SHIP_TO
              AND   g_s_kb-SOLD_TO = g-SOLD_TO
              AND   g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP
              AND   g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              AND   g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              AND   g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              AND   g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              AND   g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              AND   g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC
              AND   g_s_kb-DELIV_NUMB = g-DELIV_NUMB
              AND   g_s_kb-DOC_NUMBER = g-DOC_NUMBER
              AND   g_s_kb-MAT_PLANT = g-MAT_PLANT
              AND   g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC
              AND   g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              AND   g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM
              AND   g_s_kb-PO_NUMBER = g-PO_NUMBER
              AND   g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              AND   g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT
              AND   g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT
              AND   g_s_kb-FISCVARNT = g-FISCVARNT
              AND   g_s_kb-CALDAY = g-CALDAY
              AND   g_s_kb-FISCPER = g-FISCPER
              AND   g_s_kb-FISCYEAR = g-FISCYEAR
              AND   g_s_kb-CURRENCY = g-CURRENCY
                  .
            g_s_kb-/BIC/VVGBP = g_s_kb-/BIC/VVGBP + l_kyf.
            g_s_kb-fVVGBP = rs_c_true.
            c_val_set = rs_c_true.
            g_s_kb-CURRENCY = g-CURRENCY.
          ELSE.
            IF c_val_set = rs_c_true.
              IF c_wa_new = rs_c_false.      "read from table !!!
                DELETE TABLE g_t_kb FROM g_s_kbtbx.
              ENDIF.
              INSERT g_s_kb INTO TABLE g_t_kb.
              c_val_set = rs_c_false.
              CLEAR g_s_kb.
            ENDIF.
            READ TABLE g_t_kb INTO g_s_kb WITH KEY
              CREATEDON = g-CREATEDON
              CUST_SALES = g-CUST_SALES
              DEALTYPE = g-DEALTYPE
              DISTR_CHAN = g-DISTR_CHAN
              DIVISION = g-DIVISION
              DOC_TYPE = g-DOC_TYPE
              DSDEL_DATE = g-DSDEL_DATE
              ITEM_CATEG = g-ITEM_CATEG
              LOAD_DATE = g-LOAD_DATE
              MATAV_DATE = g-MATAV_DATE
              MATERIAL = g-MATERIAL
              ORD_TYPE = g-ORD_TYPE
              PLANT = g-PLANT
              REASON_REJ = g-REASON_REJ
              REQ_DATE = g-REQ_DATE
              SALESORG = g-SALESORG
              SALES_OFF = g-SALES_OFF
              SHIP_TO = g-SHIP_TO
              SOLD_TO = g-SOLD_TO
              TCTVAPRCTP = g-TCTVAPRCTP
              /BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              /BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              /BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              /BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              /BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              /BIC/ZZORC = g-/BIC/ZZORC
              DELIV_NUMB = g-DELIV_NUMB
              DOC_NUMBER = g-DOC_NUMBER
              MAT_PLANT = g-MAT_PLANT
              /BIC/ZZREC = g-/BIC/ZZREC
              /BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              S_ORD_ITEM = g-S_ORD_ITEM
              PO_NUMBER = g-PO_NUMBER
              /BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              /BIC/ZZOPDT = g-/BIC/ZZOPDT
              /BIC/ZZPDT = g-/BIC/ZZPDT
              FISCVARNT = g-FISCVARNT
              CALDAY = g-CALDAY
              FISCPER = g-FISCPER
              FISCYEAR = g-FISCYEAR
              CURRENCY = g-CURRENCY
              .
            l_subrc = sy-subrc.
            IF l_subrc <> 0.
              READ TABLE g_t_kb INTO g_s_kb WITH KEY
                CREATEDON = g-CREATEDON
                CUST_SALES = g-CUST_SALES
                DEALTYPE = g-DEALTYPE
                DISTR_CHAN = g-DISTR_CHAN
                DIVISION = g-DIVISION
                DOC_TYPE = g-DOC_TYPE
                DSDEL_DATE = g-DSDEL_DATE
                ITEM_CATEG = g-ITEM_CATEG
                LOAD_DATE = g-LOAD_DATE
                MATAV_DATE = g-MATAV_DATE
                MATERIAL = g-MATERIAL
                ORD_TYPE = g-ORD_TYPE
                PLANT = g-PLANT
                REASON_REJ = g-REASON_REJ
                REQ_DATE = g-REQ_DATE
                SALESORG = g-SALESORG
                SALES_OFF = g-SALES_OFF
                SHIP_TO = g-SHIP_TO
                SOLD_TO = g-SOLD_TO
                TCTVAPRCTP = g-TCTVAPRCTP
                /BIC/ZCMRCAT = g-/BIC/ZCMRCAT
                /BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
                /BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
                /BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
                /BIC/ZWORKDAY = g-/BIC/ZWORKDAY
                /BIC/ZZORC = g-/BIC/ZZORC
                DELIV_NUMB = g-DELIV_NUMB
                DOC_NUMBER = g-DOC_NUMBER
                MAT_PLANT = g-MAT_PLANT
                /BIC/ZZREC = g-/BIC/ZZREC
                /BIC/ZZRECMAN = g-/BIC/ZZRECMAN
                S_ORD_ITEM = g-S_ORD_ITEM
                PO_NUMBER = g-PO_NUMBER
                /BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
                /BIC/ZZOPDT = g-/BIC/ZZOPDT
                /BIC/ZZPDT = g-/BIC/ZZPDT
                FISCVARNT = g-FISCVARNT
                CALDAY = g-CALDAY
                FISCPER = g-FISCPER
                FISCYEAR = g-FISCYEAR
                CURRENCY = space
                .
              l_subrc = sy-subrc.
            ENDIF.
            IF l_subrc = 0.
              g_s_kbtbx = g_s_kb.
              g_s_kb-CURRENCY = g-CURRENCY.
              c_wa_new = rs_c_false.
            ELSE.
              CLEAR g_s_kbtbx.
              CLEAR g_s_kb.
              g_s_kb-CREATEDON = g-CREATEDON.
              g_s_kb-CUST_SALES = g-CUST_SALES.
              g_s_kb-DEALTYPE = g-DEALTYPE.
              g_s_kb-DISTR_CHAN = g-DISTR_CHAN.
              g_s_kb-DIVISION = g-DIVISION.
              g_s_kb-DOC_TYPE = g-DOC_TYPE.
              g_s_kb-DSDEL_DATE = g-DSDEL_DATE.
              g_s_kb-ITEM_CATEG = g-ITEM_CATEG.
              g_s_kb-LOAD_DATE = g-LOAD_DATE.
              g_s_kb-MATAV_DATE = g-MATAV_DATE.
              g_s_kb-MATERIAL = g-MATERIAL.
              g_s_kb-ORD_TYPE = g-ORD_TYPE.
              g_s_kb-PLANT = g-PLANT.
              g_s_kb-REASON_REJ = g-REASON_REJ.
              g_s_kb-REQ_DATE = g-REQ_DATE.
              g_s_kb-SALESORG = g-SALESORG.
              g_s_kb-SALES_OFF = g-SALES_OFF.
              g_s_kb-SHIP_TO = g-SHIP_TO.
              g_s_kb-SOLD_TO = g-SOLD_TO.
              g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP.
              g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT.
              g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT.
              g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG.
              g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP.
              g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY.
              g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC.
              g_s_kb-DELIV_NUMB = g-DELIV_NUMB.
              g_s_kb-DOC_NUMBER = g-DOC_NUMBER.
              g_s_kb-MAT_PLANT = g-MAT_PLANT.
              g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC.
              g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN.
              g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM.
              g_s_kb-PO_NUMBER = g-PO_NUMBER.
              g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC.
              g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT.
              g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT.
              g_s_kb-FISCVARNT = g-FISCVARNT.
              g_s_kb-CALDAY = g-CALDAY.
              g_s_kb-FISCPER = g-FISCPER.
              g_s_kb-FISCYEAR = g-FISCYEAR.
              g_s_kb-CURRENCY = g-CURRENCY.
              c_wa_new = rs_c_true.
            ENDIF.

            g_s_kb-/BIC/VVGBP = g_s_kb-/BIC/VVGBP + l_kyf.
            g_s_kb-fVVGBP = rs_c_true.
            c_val_set = rs_c_true.
            g_s_kb-CURRENCY = g-CURRENCY.
          ENDIF.
      CATCH cx_rsfo_skip_record.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'S' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_skip_record_as_error.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_abort_package.
*     In der Formel wurde ein Abbruch ausgelöst.
        sy-subrc = 24.
        c_abort = c_subrc = sy-subrc.
        PERFORM abort_message
         USING 'RSAR' 'E' '514' sy-msgv1
          sy-msgv2 sy-msgv3 sy-msgv4 0 rs_c_true.
        EXIT.

      CATCH cx_sy_arithmetic_error.
        sy-subrc = 10.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0017_VVGBP' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_sy_conversion_error.
        sy-subrc = 9.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0017_VVGBP' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_dynamic_check INTO g_error_in_formula.

        WHILE g_error_in_formula->previous IS BOUND.
          g_error_in_formula = g_error_in_formula->previous.
        ENDWHILE.
        CALL METHOD g_error_in_formula->get_text
          RECEIVING
            result = g_previous.

        sy-subrc = 20.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '194'
                'VVGBP' g_previous
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.
    ENDTRY.

*BEGIN TB09042002 HW510455
*END TB09042002

  ENDFORM.                    "r0017_VVGBP
************************************************************************
* update rule no...: 0019
* update infoobject: ZOPNORDLN
* update field.....: /BIC/ZOPNORDLN
************************************************************************
FORM r0019_ZOPNORDLN
  CHANGING c_wa_new       TYPE rs_bool
           c_val_set      TYPE rs_bool
           c_t_idocstate  TYPE rsarr_t_idocstate
           c_subrc        LIKE sy-subrc
           c_abort        LIKE sy-subrc.

  DATA:
    l_kyf         TYPE g_s_hashed_cube-/BIC/ZOPNORDLN,
    l_fscvtval    TYPE rsau_s_updinfo-fscvtval,             "#EC *
    l_subrc       LIKE sy-subrc,                            "#EC *
    l_returncode  LIKE sy-subrc,                            "#EC *
    l_t_rsmondata LIKE rsmonview OCCURS 0 WITH HEADER LINE, "#EC *
    l_chavl       TYPE rsd_chavl,                           "#EC *
    l_date        TYPE dats,                                "#EC *
    l_timchavl    TYPE rsd_chavl,                           "#EC *
    l_timresult   TYPE rsd_chavl,                           "#EC *
    l_s_dep       TYPE rrsv_s_dep,                          "#EC *
    l_t_dep       TYPE rrsv_t_dep,                          "#EC *
    l_chavl_in    TYPE rschavl60,                             "#EC *
    l_chavl_out   TYPE rschavl60,                             "#EC *
    l_s_uom_val   TYPE rsuom_s_calc_runt_values,            "#EC *
    l_s_uom_prop  TYPE rsuom_s_calc_runt_prop_ext.          "#EC *

  IF g_break = rs_c_true. " any key figure
    BREAK-POINT.                                           "#EC NOBREAK
  ENDIF.
  c_abort = 0.
  TRY.                                                      "JD 180102
      CLEAR: g, g_error.


      IF g_record_no_cha_calculation <> record_no.
*   common characteristics calculation
        PERFORM cha_calculation
          CHANGING l_returncode c_t_idocstate c_subrc c_abort.
        IF l_returncode <> 0 OR c_subrc <> 0 OR c_abort <> 0.
          EXIT.
        ENDIF.
      ELSE.
*   don´t calculate, if same record
        g = g_s_cha_calculation.
      ENDIF.


      g-FISCPER = g_s_is-FISCPER.
      g-FISCYEAR = g_s_is-FISCYEAR.

      CLEAR l_returncode.
* key figure routine no.: 0001

* before customer routine
      IF g_break_routine = rs_c_true.
        BREAK-POINT.                                       "#EC NOBREAK
      ENDIF.

      PERFORM routine_0001
        CHANGING
          l_kyf
          l_returncode
          c_t_idocstate
          c_subrc
          c_abort.
      IF NOT monitor[] IS INITIAL.
        LOOP AT monitor.
          PERFORM msg_to_handler_2 USING monitor-msgid monitor-msgty
                                       monitor-msgno monitor-msgv1
                                       monitor-msgv2 monitor-msgv3
                                       monitor-msgv4 g_s_is-recno
                                 CHANGING c_subrc.

        ENDLOOP.
        REFRESH monitor.
      ENDIF.

      IF abort <> 0.
        PERFORM abort_message
           USING 'RSAU' 'E' '723' '0001'
                  space space space
                  g_s_is-recno rs_c_true.
        c_subrc = 1.
        c_abort = abort.
        EXIT.
      ELSEIF c_subrc <> 0 OR c_abort <> 0.
        IF c_subrc <> 0.
          c_abort = c_subrc.
        ENDIF.
        EXIT.
      ENDIF.
      IF l_returncode <> 0.
        EXIT.
      ENDIF.


          IF NOT g_s_kb IS INITIAL
              AND   g_s_kb-CREATEDON = g-CREATEDON
              AND   g_s_kb-CUST_SALES = g-CUST_SALES
              AND   g_s_kb-DEALTYPE = g-DEALTYPE
              AND   g_s_kb-DISTR_CHAN = g-DISTR_CHAN
              AND   g_s_kb-DIVISION = g-DIVISION
              AND   g_s_kb-DOC_TYPE = g-DOC_TYPE
              AND   g_s_kb-DSDEL_DATE = g-DSDEL_DATE
              AND   g_s_kb-ITEM_CATEG = g-ITEM_CATEG
              AND   g_s_kb-LOAD_DATE = g-LOAD_DATE
              AND   g_s_kb-MATAV_DATE = g-MATAV_DATE
              AND   g_s_kb-MATERIAL = g-MATERIAL
              AND   g_s_kb-ORD_TYPE = g-ORD_TYPE
              AND   g_s_kb-PLANT = g-PLANT
              AND   g_s_kb-REASON_REJ = g-REASON_REJ
              AND   g_s_kb-REQ_DATE = g-REQ_DATE
              AND   g_s_kb-SALESORG = g-SALESORG
              AND   g_s_kb-SALES_OFF = g-SALES_OFF
              AND   g_s_kb-SHIP_TO = g-SHIP_TO
              AND   g_s_kb-SOLD_TO = g-SOLD_TO
              AND   g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP
              AND   g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              AND   g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              AND   g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              AND   g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              AND   g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              AND   g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC
              AND   g_s_kb-DELIV_NUMB = g-DELIV_NUMB
              AND   g_s_kb-DOC_NUMBER = g-DOC_NUMBER
              AND   g_s_kb-MAT_PLANT = g-MAT_PLANT
              AND   g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC
              AND   g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              AND   g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM
              AND   g_s_kb-PO_NUMBER = g-PO_NUMBER
              AND   g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              AND   g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT
              AND   g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT
              AND   g_s_kb-FISCVARNT = g-FISCVARNT
              AND   g_s_kb-CALDAY = g-CALDAY
              AND   g_s_kb-FISCPER = g-FISCPER
              AND   g_s_kb-FISCYEAR = g-FISCYEAR
                  .
            g_s_kb-/BIC/ZOPNORDLN = g_s_kb-/BIC/ZOPNORDLN + l_kyf.
            g_s_kb-fZOPNORDLN = rs_c_true.
            c_val_set = rs_c_true.
          ELSE.
            IF c_val_set = rs_c_true.
              IF c_wa_new = rs_c_false.      "read from table !!!
                DELETE TABLE g_t_kb FROM g_s_kbtbx.
              ENDIF.
              INSERT g_s_kb INTO TABLE g_t_kb.
              c_val_set = rs_c_false.
              CLEAR g_s_kb.
            ENDIF.
            READ TABLE g_t_kb INTO g_s_kb WITH KEY
              CREATEDON = g-CREATEDON
              CUST_SALES = g-CUST_SALES
              DEALTYPE = g-DEALTYPE
              DISTR_CHAN = g-DISTR_CHAN
              DIVISION = g-DIVISION
              DOC_TYPE = g-DOC_TYPE
              DSDEL_DATE = g-DSDEL_DATE
              ITEM_CATEG = g-ITEM_CATEG
              LOAD_DATE = g-LOAD_DATE
              MATAV_DATE = g-MATAV_DATE
              MATERIAL = g-MATERIAL
              ORD_TYPE = g-ORD_TYPE
              PLANT = g-PLANT
              REASON_REJ = g-REASON_REJ
              REQ_DATE = g-REQ_DATE
              SALESORG = g-SALESORG
              SALES_OFF = g-SALES_OFF
              SHIP_TO = g-SHIP_TO
              SOLD_TO = g-SOLD_TO
              TCTVAPRCTP = g-TCTVAPRCTP
              /BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              /BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              /BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              /BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              /BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              /BIC/ZZORC = g-/BIC/ZZORC
              DELIV_NUMB = g-DELIV_NUMB
              DOC_NUMBER = g-DOC_NUMBER
              MAT_PLANT = g-MAT_PLANT
              /BIC/ZZREC = g-/BIC/ZZREC
              /BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              S_ORD_ITEM = g-S_ORD_ITEM
              PO_NUMBER = g-PO_NUMBER
              /BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              /BIC/ZZOPDT = g-/BIC/ZZOPDT
              /BIC/ZZPDT = g-/BIC/ZZPDT
              FISCVARNT = g-FISCVARNT
              CALDAY = g-CALDAY
              FISCPER = g-FISCPER
              FISCYEAR = g-FISCYEAR
              .
            l_subrc = sy-subrc.
            IF l_subrc = 0.
              g_s_kbtbx = g_s_kb.
              c_wa_new = rs_c_false.
            ELSE.
              CLEAR g_s_kbtbx.
              CLEAR g_s_kb.
              g_s_kb-CREATEDON = g-CREATEDON.
              g_s_kb-CUST_SALES = g-CUST_SALES.
              g_s_kb-DEALTYPE = g-DEALTYPE.
              g_s_kb-DISTR_CHAN = g-DISTR_CHAN.
              g_s_kb-DIVISION = g-DIVISION.
              g_s_kb-DOC_TYPE = g-DOC_TYPE.
              g_s_kb-DSDEL_DATE = g-DSDEL_DATE.
              g_s_kb-ITEM_CATEG = g-ITEM_CATEG.
              g_s_kb-LOAD_DATE = g-LOAD_DATE.
              g_s_kb-MATAV_DATE = g-MATAV_DATE.
              g_s_kb-MATERIAL = g-MATERIAL.
              g_s_kb-ORD_TYPE = g-ORD_TYPE.
              g_s_kb-PLANT = g-PLANT.
              g_s_kb-REASON_REJ = g-REASON_REJ.
              g_s_kb-REQ_DATE = g-REQ_DATE.
              g_s_kb-SALESORG = g-SALESORG.
              g_s_kb-SALES_OFF = g-SALES_OFF.
              g_s_kb-SHIP_TO = g-SHIP_TO.
              g_s_kb-SOLD_TO = g-SOLD_TO.
              g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP.
              g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT.
              g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT.
              g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG.
              g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP.
              g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY.
              g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC.
              g_s_kb-DELIV_NUMB = g-DELIV_NUMB.
              g_s_kb-DOC_NUMBER = g-DOC_NUMBER.
              g_s_kb-MAT_PLANT = g-MAT_PLANT.
              g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC.
              g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN.
              g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM.
              g_s_kb-PO_NUMBER = g-PO_NUMBER.
              g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC.
              g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT.
              g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT.
              g_s_kb-FISCVARNT = g-FISCVARNT.
              g_s_kb-CALDAY = g-CALDAY.
              g_s_kb-FISCPER = g-FISCPER.
              g_s_kb-FISCYEAR = g-FISCYEAR.
              c_wa_new = rs_c_true.
            ENDIF.

            g_s_kb-/BIC/ZOPNORDLN = g_s_kb-/BIC/ZOPNORDLN + l_kyf.
            g_s_kb-fZOPNORDLN = rs_c_true.
            c_val_set = rs_c_true.
          ENDIF.
      CATCH cx_rsfo_skip_record.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'S' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_skip_record_as_error.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_abort_package.
*     In der Formel wurde ein Abbruch ausgelöst.
        sy-subrc = 24.
        c_abort = c_subrc = sy-subrc.
        PERFORM abort_message
         USING 'RSAR' 'E' '514' sy-msgv1
          sy-msgv2 sy-msgv3 sy-msgv4 0 rs_c_true.
        EXIT.

      CATCH cx_sy_arithmetic_error.
        sy-subrc = 10.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0019_ZOPNORDLN' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_sy_conversion_error.
        sy-subrc = 9.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0019_ZOPNORDLN' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_dynamic_check INTO g_error_in_formula.

        WHILE g_error_in_formula->previous IS BOUND.
          g_error_in_formula = g_error_in_formula->previous.
        ENDWHILE.
        CALL METHOD g_error_in_formula->get_text
          RECEIVING
            result = g_previous.

        sy-subrc = 20.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '194'
                'ZOPNORDLN' g_previous
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.
    ENDTRY.

*BEGIN TB09042002 HW510455
*END TB09042002

  ENDFORM.                    "r0019_ZOPNORDLN
************************************************************************
* update rule no...: 0020
* update infoobject: 0GROSS_WGT
* update field.....: GROSS_WGT
************************************************************************
FORM r0020_0GROSS_WGT
  CHANGING c_wa_new       TYPE rs_bool
           c_val_set      TYPE rs_bool
           c_t_idocstate  TYPE rsarr_t_idocstate
           c_subrc        LIKE sy-subrc
           c_abort        LIKE sy-subrc.

  DATA:
    l_kyf         TYPE g_s_hashed_cube-GROSS_WGT,
    l_fscvtval    TYPE rsau_s_updinfo-fscvtval,             "#EC *
    l_subrc       LIKE sy-subrc,                            "#EC *
    l_returncode  LIKE sy-subrc,                            "#EC *
    l_t_rsmondata LIKE rsmonview OCCURS 0 WITH HEADER LINE, "#EC *
    l_chavl       TYPE rsd_chavl,                           "#EC *
    l_date        TYPE dats,                                "#EC *
    l_timchavl    TYPE rsd_chavl,                           "#EC *
    l_timresult   TYPE rsd_chavl,                           "#EC *
    l_s_dep       TYPE rrsv_s_dep,                          "#EC *
    l_t_dep       TYPE rrsv_t_dep,                          "#EC *
    l_chavl_in    TYPE rschavl60,                             "#EC *
    l_chavl_out   TYPE rschavl60,                             "#EC *
    l_s_uom_val   TYPE rsuom_s_calc_runt_values,            "#EC *
    l_s_uom_prop  TYPE rsuom_s_calc_runt_prop_ext.          "#EC *

  IF g_break = rs_c_true. " any key figure
    BREAK-POINT.                                           "#EC NOBREAK
  ENDIF.
  c_abort = 0.
  TRY.                                                      "JD 180102
      CLEAR: g, g_error.


      IF g_record_no_cha_calculation <> record_no.
*   common characteristics calculation
        PERFORM cha_calculation
          CHANGING l_returncode c_t_idocstate c_subrc c_abort.
        IF l_returncode <> 0 OR c_subrc <> 0 OR c_abort <> 0.
          EXIT.
        ENDIF.
      ELSE.
*   don´t calculate, if same record
        g = g_s_cha_calculation.
      ENDIF.


      g-FISCPER = g_s_is-FISCPER.
      g-FISCYEAR = g_s_is-FISCYEAR.
      g-UNIT_OF_WT = g_s_is-UNIT_OF_WT.

        l_kyf = g_s_is-GROSS_WGT.


          IF NOT g_s_kb IS INITIAL
              AND   g_s_kb-DISTR_CHAN = g-DISTR_CHAN
              AND   g_s_kb-DIVISION = g-DIVISION
              AND   g_s_kb-DOC_TYPE = g-DOC_TYPE
              AND   g_s_kb-DSDEL_DATE = g-DSDEL_DATE
              AND   g_s_kb-MATAV_DATE = g-MATAV_DATE
              AND   g_s_kb-MATERIAL = g-MATERIAL
              AND   g_s_kb-ORD_TYPE = g-ORD_TYPE
              AND   g_s_kb-PLANT = g-PLANT
              AND   g_s_kb-REQ_DATE = g-REQ_DATE
              AND   g_s_kb-SALESORG = g-SALESORG
              AND   g_s_kb-SALES_OFF = g-SALES_OFF
              AND   g_s_kb-SOLD_TO = g-SOLD_TO
              AND   g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              AND   g_s_kb-LOAD_DATE = g-LOAD_DATE
              AND   g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              AND   g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              AND   g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP
              AND   g_s_kb-SHIP_TO = g-SHIP_TO
              AND   g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              AND   g_s_kb-REASON_REJ = g-REASON_REJ
              AND   g_s_kb-CREATEDON = g-CREATEDON
              AND   g_s_kb-CUST_SALES = g-CUST_SALES
              AND   g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              AND   g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC
              AND   g_s_kb-DEALTYPE = g-DEALTYPE
              AND   g_s_kb-ITEM_CATEG = g-ITEM_CATEG
              AND   g_s_kb-DELIV_NUMB = g-DELIV_NUMB
              AND   g_s_kb-DOC_NUMBER = g-DOC_NUMBER
              AND   g_s_kb-MAT_PLANT = g-MAT_PLANT
              AND   g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC
              AND   g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              AND   g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM
              AND   g_s_kb-PO_NUMBER = g-PO_NUMBER
              AND   g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              AND   g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT
              AND   g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT
              AND   g_s_kb-FISCVARNT = g-FISCVARNT
              AND   g_s_kb-CALDAY = g-CALDAY
              AND   g_s_kb-FISCPER = g-FISCPER
              AND   g_s_kb-FISCYEAR = g-FISCYEAR
              AND   g_s_kb-UNIT_OF_WT = g-UNIT_OF_WT
                  .
            g_s_kb-GROSS_WGT = g_s_kb-GROSS_WGT + l_kyf.
            g_s_kb-f0GROSS_WGT = rs_c_true.
            c_val_set = rs_c_true.
            g_s_kb-UNIT_OF_WT = g-UNIT_OF_WT.
          ELSE.
            IF c_val_set = rs_c_true.
              IF c_wa_new = rs_c_false.      "read from table !!!
                DELETE TABLE g_t_kb FROM g_s_kbtbx.
              ENDIF.
              INSERT g_s_kb INTO TABLE g_t_kb.
              c_val_set = rs_c_false.
              CLEAR g_s_kb.
            ENDIF.
            READ TABLE g_t_kb INTO g_s_kb WITH KEY
              DISTR_CHAN = g-DISTR_CHAN
              DIVISION = g-DIVISION
              DOC_TYPE = g-DOC_TYPE
              DSDEL_DATE = g-DSDEL_DATE
              MATAV_DATE = g-MATAV_DATE
              MATERIAL = g-MATERIAL
              ORD_TYPE = g-ORD_TYPE
              PLANT = g-PLANT
              REQ_DATE = g-REQ_DATE
              SALESORG = g-SALESORG
              SALES_OFF = g-SALES_OFF
              SOLD_TO = g-SOLD_TO
              /BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              LOAD_DATE = g-LOAD_DATE
              /BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              /BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              TCTVAPRCTP = g-TCTVAPRCTP
              SHIP_TO = g-SHIP_TO
              /BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              REASON_REJ = g-REASON_REJ
              CREATEDON = g-CREATEDON
              CUST_SALES = g-CUST_SALES
              /BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              /BIC/ZZORC = g-/BIC/ZZORC
              DEALTYPE = g-DEALTYPE
              ITEM_CATEG = g-ITEM_CATEG
              DELIV_NUMB = g-DELIV_NUMB
              DOC_NUMBER = g-DOC_NUMBER
              MAT_PLANT = g-MAT_PLANT
              /BIC/ZZREC = g-/BIC/ZZREC
              /BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              S_ORD_ITEM = g-S_ORD_ITEM
              PO_NUMBER = g-PO_NUMBER
              /BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              /BIC/ZZOPDT = g-/BIC/ZZOPDT
              /BIC/ZZPDT = g-/BIC/ZZPDT
              FISCVARNT = g-FISCVARNT
              CALDAY = g-CALDAY
              FISCPER = g-FISCPER
              FISCYEAR = g-FISCYEAR
              UNIT_OF_WT = g-UNIT_OF_WT
              .
            l_subrc = sy-subrc.
            IF l_subrc <> 0.
              READ TABLE g_t_kb INTO g_s_kb WITH KEY
                DISTR_CHAN = g-DISTR_CHAN
                DIVISION = g-DIVISION
                DOC_TYPE = g-DOC_TYPE
                DSDEL_DATE = g-DSDEL_DATE
                MATAV_DATE = g-MATAV_DATE
                MATERIAL = g-MATERIAL
                ORD_TYPE = g-ORD_TYPE
                PLANT = g-PLANT
                REQ_DATE = g-REQ_DATE
                SALESORG = g-SALESORG
                SALES_OFF = g-SALES_OFF
                SOLD_TO = g-SOLD_TO
                /BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
                LOAD_DATE = g-LOAD_DATE
                /BIC/ZCMRCAT = g-/BIC/ZCMRCAT
                /BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
                TCTVAPRCTP = g-TCTVAPRCTP
                SHIP_TO = g-SHIP_TO
                /BIC/ZWORKDAY = g-/BIC/ZWORKDAY
                REASON_REJ = g-REASON_REJ
                CREATEDON = g-CREATEDON
                CUST_SALES = g-CUST_SALES
                /BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
                /BIC/ZZORC = g-/BIC/ZZORC
                DEALTYPE = g-DEALTYPE
                ITEM_CATEG = g-ITEM_CATEG
                DELIV_NUMB = g-DELIV_NUMB
                DOC_NUMBER = g-DOC_NUMBER
                MAT_PLANT = g-MAT_PLANT
                /BIC/ZZREC = g-/BIC/ZZREC
                /BIC/ZZRECMAN = g-/BIC/ZZRECMAN
                S_ORD_ITEM = g-S_ORD_ITEM
                PO_NUMBER = g-PO_NUMBER
                /BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
                /BIC/ZZOPDT = g-/BIC/ZZOPDT
                /BIC/ZZPDT = g-/BIC/ZZPDT
                FISCVARNT = g-FISCVARNT
                CALDAY = g-CALDAY
                FISCPER = g-FISCPER
                FISCYEAR = g-FISCYEAR
                UNIT_OF_WT = space
                .
              l_subrc = sy-subrc.
            ENDIF.
            IF l_subrc = 0.
              g_s_kbtbx = g_s_kb.
              g_s_kb-UNIT_OF_WT = g-UNIT_OF_WT.
              c_wa_new = rs_c_false.
            ELSE.
              CLEAR g_s_kbtbx.
              CLEAR g_s_kb.
              g_s_kb-DISTR_CHAN = g-DISTR_CHAN.
              g_s_kb-DIVISION = g-DIVISION.
              g_s_kb-DOC_TYPE = g-DOC_TYPE.
              g_s_kb-DSDEL_DATE = g-DSDEL_DATE.
              g_s_kb-MATAV_DATE = g-MATAV_DATE.
              g_s_kb-MATERIAL = g-MATERIAL.
              g_s_kb-ORD_TYPE = g-ORD_TYPE.
              g_s_kb-PLANT = g-PLANT.
              g_s_kb-REQ_DATE = g-REQ_DATE.
              g_s_kb-SALESORG = g-SALESORG.
              g_s_kb-SALES_OFF = g-SALES_OFF.
              g_s_kb-SOLD_TO = g-SOLD_TO.
              g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT.
              g_s_kb-LOAD_DATE = g-LOAD_DATE.
              g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT.
              g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP.
              g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP.
              g_s_kb-SHIP_TO = g-SHIP_TO.
              g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY.
              g_s_kb-REASON_REJ = g-REASON_REJ.
              g_s_kb-CREATEDON = g-CREATEDON.
              g_s_kb-CUST_SALES = g-CUST_SALES.
              g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG.
              g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC.
              g_s_kb-DEALTYPE = g-DEALTYPE.
              g_s_kb-ITEM_CATEG = g-ITEM_CATEG.
              g_s_kb-DELIV_NUMB = g-DELIV_NUMB.
              g_s_kb-DOC_NUMBER = g-DOC_NUMBER.
              g_s_kb-MAT_PLANT = g-MAT_PLANT.
              g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC.
              g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN.
              g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM.
              g_s_kb-PO_NUMBER = g-PO_NUMBER.
              g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC.
              g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT.
              g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT.
              g_s_kb-FISCVARNT = g-FISCVARNT.
              g_s_kb-CALDAY = g-CALDAY.
              g_s_kb-FISCPER = g-FISCPER.
              g_s_kb-FISCYEAR = g-FISCYEAR.
              g_s_kb-UNIT_OF_WT = g-UNIT_OF_WT.
              c_wa_new = rs_c_true.
            ENDIF.

            g_s_kb-GROSS_WGT = g_s_kb-GROSS_WGT + l_kyf.
            g_s_kb-f0GROSS_WGT = rs_c_true.
            c_val_set = rs_c_true.
            g_s_kb-UNIT_OF_WT = g-UNIT_OF_WT.
          ENDIF.
      CATCH cx_rsfo_skip_record.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'S' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_skip_record_as_error.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_abort_package.
*     In der Formel wurde ein Abbruch ausgelöst.
        sy-subrc = 24.
        c_abort = c_subrc = sy-subrc.
        PERFORM abort_message
         USING 'RSAR' 'E' '514' sy-msgv1
          sy-msgv2 sy-msgv3 sy-msgv4 0 rs_c_true.
        EXIT.

      CATCH cx_sy_arithmetic_error.
        sy-subrc = 10.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0020_0GROSS_WGT' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_sy_conversion_error.
        sy-subrc = 9.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0020_0GROSS_WGT' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_dynamic_check INTO g_error_in_formula.

        WHILE g_error_in_formula->previous IS BOUND.
          g_error_in_formula = g_error_in_formula->previous.
        ENDWHILE.
        CALL METHOD g_error_in_formula->get_text
          RECEIVING
            result = g_previous.

        sy-subrc = 20.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '194'
                '0GROSS_WGT' g_previous
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.
    ENDTRY.

*BEGIN TB09042002 HW510455
*END TB09042002

  ENDFORM.                    "r0020_0GROSS_WGT
************************************************************************
* update rule no...: 0021
* update infoobject: ZNONCONF1
* update field.....: /BIC/ZNONCONF1
************************************************************************
FORM r0021_ZNONCONF1
  CHANGING c_wa_new       TYPE rs_bool
           c_val_set      TYPE rs_bool
           c_t_idocstate  TYPE rsarr_t_idocstate
           c_subrc        LIKE sy-subrc
           c_abort        LIKE sy-subrc.

  DATA:
    l_kyf         TYPE g_s_hashed_cube-/BIC/ZNONCONF1,
    l_fscvtval    TYPE rsau_s_updinfo-fscvtval,             "#EC *
    l_subrc       LIKE sy-subrc,                            "#EC *
    l_returncode  LIKE sy-subrc,                            "#EC *
    l_t_rsmondata LIKE rsmonview OCCURS 0 WITH HEADER LINE, "#EC *
    l_chavl       TYPE rsd_chavl,                           "#EC *
    l_date        TYPE dats,                                "#EC *
    l_timchavl    TYPE rsd_chavl,                           "#EC *
    l_timresult   TYPE rsd_chavl,                           "#EC *
    l_s_dep       TYPE rrsv_s_dep,                          "#EC *
    l_t_dep       TYPE rrsv_t_dep,                          "#EC *
    l_chavl_in    TYPE rschavl60,                             "#EC *
    l_chavl_out   TYPE rschavl60,                             "#EC *
    l_s_uom_val   TYPE rsuom_s_calc_runt_values,            "#EC *
    l_s_uom_prop  TYPE rsuom_s_calc_runt_prop_ext.          "#EC *

  IF g_break = rs_c_true. " any key figure
    BREAK-POINT.                                           "#EC NOBREAK
  ENDIF.
  c_abort = 0.
  TRY.                                                      "JD 180102
      CLEAR: g, g_error.


      IF g_record_no_cha_calculation <> record_no.
*   common characteristics calculation
        PERFORM cha_calculation
          CHANGING l_returncode c_t_idocstate c_subrc c_abort.
        IF l_returncode <> 0 OR c_subrc <> 0 OR c_abort <> 0.
          EXIT.
        ENDIF.
      ELSE.
*   don´t calculate, if same record
        g = g_s_cha_calculation.
      ENDIF.


      g-FISCPER = g_s_is-FISCPER.
      g-FISCYEAR = g_s_is-FISCYEAR.
      g-UNIT_OF_WT = g_s_is-UNIT_OF_WT.

        l_kyf = g_s_is-/BIC/ZNONCONF1.


          IF NOT g_s_kb IS INITIAL
              AND   g_s_kb-DISTR_CHAN = g-DISTR_CHAN
              AND   g_s_kb-DIVISION = g-DIVISION
              AND   g_s_kb-DOC_TYPE = g-DOC_TYPE
              AND   g_s_kb-DSDEL_DATE = g-DSDEL_DATE
              AND   g_s_kb-MATAV_DATE = g-MATAV_DATE
              AND   g_s_kb-MATERIAL = g-MATERIAL
              AND   g_s_kb-ORD_TYPE = g-ORD_TYPE
              AND   g_s_kb-PLANT = g-PLANT
              AND   g_s_kb-REQ_DATE = g-REQ_DATE
              AND   g_s_kb-SALESORG = g-SALESORG
              AND   g_s_kb-SALES_OFF = g-SALES_OFF
              AND   g_s_kb-SOLD_TO = g-SOLD_TO
              AND   g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              AND   g_s_kb-LOAD_DATE = g-LOAD_DATE
              AND   g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              AND   g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              AND   g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP
              AND   g_s_kb-SHIP_TO = g-SHIP_TO
              AND   g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              AND   g_s_kb-REASON_REJ = g-REASON_REJ
              AND   g_s_kb-CREATEDON = g-CREATEDON
              AND   g_s_kb-CUST_SALES = g-CUST_SALES
              AND   g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              AND   g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC
              AND   g_s_kb-DEALTYPE = g-DEALTYPE
              AND   g_s_kb-ITEM_CATEG = g-ITEM_CATEG
              AND   g_s_kb-DELIV_NUMB = g-DELIV_NUMB
              AND   g_s_kb-DOC_NUMBER = g-DOC_NUMBER
              AND   g_s_kb-MAT_PLANT = g-MAT_PLANT
              AND   g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC
              AND   g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              AND   g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM
              AND   g_s_kb-PO_NUMBER = g-PO_NUMBER
              AND   g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              AND   g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT
              AND   g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT
              AND   g_s_kb-FISCVARNT = g-FISCVARNT
              AND   g_s_kb-CALDAY = g-CALDAY
              AND   g_s_kb-FISCPER = g-FISCPER
              AND   g_s_kb-FISCYEAR = g-FISCYEAR
              AND   g_s_kb-UNIT_OF_WT = g-UNIT_OF_WT
                  .
            g_s_kb-/BIC/ZNONCONF1 = g_s_kb-/BIC/ZNONCONF1 + l_kyf.
            g_s_kb-fZNONCONF1 = rs_c_true.
            c_val_set = rs_c_true.
            g_s_kb-UNIT_OF_WT = g-UNIT_OF_WT.
          ELSE.
            IF c_val_set = rs_c_true.
              IF c_wa_new = rs_c_false.      "read from table !!!
                DELETE TABLE g_t_kb FROM g_s_kbtbx.
              ENDIF.
              INSERT g_s_kb INTO TABLE g_t_kb.
              c_val_set = rs_c_false.
              CLEAR g_s_kb.
            ENDIF.
            READ TABLE g_t_kb INTO g_s_kb WITH KEY
              DISTR_CHAN = g-DISTR_CHAN
              DIVISION = g-DIVISION
              DOC_TYPE = g-DOC_TYPE
              DSDEL_DATE = g-DSDEL_DATE
              MATAV_DATE = g-MATAV_DATE
              MATERIAL = g-MATERIAL
              ORD_TYPE = g-ORD_TYPE
              PLANT = g-PLANT
              REQ_DATE = g-REQ_DATE
              SALESORG = g-SALESORG
              SALES_OFF = g-SALES_OFF
              SOLD_TO = g-SOLD_TO
              /BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              LOAD_DATE = g-LOAD_DATE
              /BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              /BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              TCTVAPRCTP = g-TCTVAPRCTP
              SHIP_TO = g-SHIP_TO
              /BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              REASON_REJ = g-REASON_REJ
              CREATEDON = g-CREATEDON
              CUST_SALES = g-CUST_SALES
              /BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              /BIC/ZZORC = g-/BIC/ZZORC
              DEALTYPE = g-DEALTYPE
              ITEM_CATEG = g-ITEM_CATEG
              DELIV_NUMB = g-DELIV_NUMB
              DOC_NUMBER = g-DOC_NUMBER
              MAT_PLANT = g-MAT_PLANT
              /BIC/ZZREC = g-/BIC/ZZREC
              /BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              S_ORD_ITEM = g-S_ORD_ITEM
              PO_NUMBER = g-PO_NUMBER
              /BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              /BIC/ZZOPDT = g-/BIC/ZZOPDT
              /BIC/ZZPDT = g-/BIC/ZZPDT
              FISCVARNT = g-FISCVARNT
              CALDAY = g-CALDAY
              FISCPER = g-FISCPER
              FISCYEAR = g-FISCYEAR
              UNIT_OF_WT = g-UNIT_OF_WT
              .
            l_subrc = sy-subrc.
            IF l_subrc <> 0.
              READ TABLE g_t_kb INTO g_s_kb WITH KEY
                DISTR_CHAN = g-DISTR_CHAN
                DIVISION = g-DIVISION
                DOC_TYPE = g-DOC_TYPE
                DSDEL_DATE = g-DSDEL_DATE
                MATAV_DATE = g-MATAV_DATE
                MATERIAL = g-MATERIAL
                ORD_TYPE = g-ORD_TYPE
                PLANT = g-PLANT
                REQ_DATE = g-REQ_DATE
                SALESORG = g-SALESORG
                SALES_OFF = g-SALES_OFF
                SOLD_TO = g-SOLD_TO
                /BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
                LOAD_DATE = g-LOAD_DATE
                /BIC/ZCMRCAT = g-/BIC/ZCMRCAT
                /BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
                TCTVAPRCTP = g-TCTVAPRCTP
                SHIP_TO = g-SHIP_TO
                /BIC/ZWORKDAY = g-/BIC/ZWORKDAY
                REASON_REJ = g-REASON_REJ
                CREATEDON = g-CREATEDON
                CUST_SALES = g-CUST_SALES
                /BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
                /BIC/ZZORC = g-/BIC/ZZORC
                DEALTYPE = g-DEALTYPE
                ITEM_CATEG = g-ITEM_CATEG
                DELIV_NUMB = g-DELIV_NUMB
                DOC_NUMBER = g-DOC_NUMBER
                MAT_PLANT = g-MAT_PLANT
                /BIC/ZZREC = g-/BIC/ZZREC
                /BIC/ZZRECMAN = g-/BIC/ZZRECMAN
                S_ORD_ITEM = g-S_ORD_ITEM
                PO_NUMBER = g-PO_NUMBER
                /BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
                /BIC/ZZOPDT = g-/BIC/ZZOPDT
                /BIC/ZZPDT = g-/BIC/ZZPDT
                FISCVARNT = g-FISCVARNT
                CALDAY = g-CALDAY
                FISCPER = g-FISCPER
                FISCYEAR = g-FISCYEAR
                UNIT_OF_WT = space
                .
              l_subrc = sy-subrc.
            ENDIF.
            IF l_subrc = 0.
              g_s_kbtbx = g_s_kb.
              g_s_kb-UNIT_OF_WT = g-UNIT_OF_WT.
              c_wa_new = rs_c_false.
            ELSE.
              CLEAR g_s_kbtbx.
              CLEAR g_s_kb.
              g_s_kb-DISTR_CHAN = g-DISTR_CHAN.
              g_s_kb-DIVISION = g-DIVISION.
              g_s_kb-DOC_TYPE = g-DOC_TYPE.
              g_s_kb-DSDEL_DATE = g-DSDEL_DATE.
              g_s_kb-MATAV_DATE = g-MATAV_DATE.
              g_s_kb-MATERIAL = g-MATERIAL.
              g_s_kb-ORD_TYPE = g-ORD_TYPE.
              g_s_kb-PLANT = g-PLANT.
              g_s_kb-REQ_DATE = g-REQ_DATE.
              g_s_kb-SALESORG = g-SALESORG.
              g_s_kb-SALES_OFF = g-SALES_OFF.
              g_s_kb-SOLD_TO = g-SOLD_TO.
              g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT.
              g_s_kb-LOAD_DATE = g-LOAD_DATE.
              g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT.
              g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP.
              g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP.
              g_s_kb-SHIP_TO = g-SHIP_TO.
              g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY.
              g_s_kb-REASON_REJ = g-REASON_REJ.
              g_s_kb-CREATEDON = g-CREATEDON.
              g_s_kb-CUST_SALES = g-CUST_SALES.
              g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG.
              g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC.
              g_s_kb-DEALTYPE = g-DEALTYPE.
              g_s_kb-ITEM_CATEG = g-ITEM_CATEG.
              g_s_kb-DELIV_NUMB = g-DELIV_NUMB.
              g_s_kb-DOC_NUMBER = g-DOC_NUMBER.
              g_s_kb-MAT_PLANT = g-MAT_PLANT.
              g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC.
              g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN.
              g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM.
              g_s_kb-PO_NUMBER = g-PO_NUMBER.
              g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC.
              g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT.
              g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT.
              g_s_kb-FISCVARNT = g-FISCVARNT.
              g_s_kb-CALDAY = g-CALDAY.
              g_s_kb-FISCPER = g-FISCPER.
              g_s_kb-FISCYEAR = g-FISCYEAR.
              g_s_kb-UNIT_OF_WT = g-UNIT_OF_WT.
              c_wa_new = rs_c_true.
            ENDIF.

            g_s_kb-/BIC/ZNONCONF1 = g_s_kb-/BIC/ZNONCONF1 + l_kyf.
            g_s_kb-fZNONCONF1 = rs_c_true.
            c_val_set = rs_c_true.
            g_s_kb-UNIT_OF_WT = g-UNIT_OF_WT.
          ENDIF.
      CATCH cx_rsfo_skip_record.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'S' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_skip_record_as_error.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_abort_package.
*     In der Formel wurde ein Abbruch ausgelöst.
        sy-subrc = 24.
        c_abort = c_subrc = sy-subrc.
        PERFORM abort_message
         USING 'RSAR' 'E' '514' sy-msgv1
          sy-msgv2 sy-msgv3 sy-msgv4 0 rs_c_true.
        EXIT.

      CATCH cx_sy_arithmetic_error.
        sy-subrc = 10.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0021_ZNONCONF1' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_sy_conversion_error.
        sy-subrc = 9.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0021_ZNONCONF1' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_dynamic_check INTO g_error_in_formula.

        WHILE g_error_in_formula->previous IS BOUND.
          g_error_in_formula = g_error_in_formula->previous.
        ENDWHILE.
        CALL METHOD g_error_in_formula->get_text
          RECEIVING
            result = g_previous.

        sy-subrc = 20.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '194'
                'ZNONCONF1' g_previous
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.
    ENDTRY.

*BEGIN TB09042002 HW510455
*END TB09042002

  ENDFORM.                    "r0021_ZNONCONF1
************************************************************************
* update rule no...: 0022
* update infoobject: ZZPDTCNT
* update field.....: /BIC/ZZPDTCNT
************************************************************************
FORM r0022_ZZPDTCNT
  CHANGING c_wa_new       TYPE rs_bool
           c_val_set      TYPE rs_bool
           c_t_idocstate  TYPE rsarr_t_idocstate
           c_subrc        LIKE sy-subrc
           c_abort        LIKE sy-subrc.

  DATA:
    l_kyf         TYPE g_s_hashed_cube-/BIC/ZZPDTCNT,
    l_fscvtval    TYPE rsau_s_updinfo-fscvtval,             "#EC *
    l_subrc       LIKE sy-subrc,                            "#EC *
    l_returncode  LIKE sy-subrc,                            "#EC *
    l_t_rsmondata LIKE rsmonview OCCURS 0 WITH HEADER LINE, "#EC *
    l_chavl       TYPE rsd_chavl,                           "#EC *
    l_date        TYPE dats,                                "#EC *
    l_timchavl    TYPE rsd_chavl,                           "#EC *
    l_timresult   TYPE rsd_chavl,                           "#EC *
    l_s_dep       TYPE rrsv_s_dep,                          "#EC *
    l_t_dep       TYPE rrsv_t_dep,                          "#EC *
    l_chavl_in    TYPE rschavl60,                             "#EC *
    l_chavl_out   TYPE rschavl60,                             "#EC *
    l_s_uom_val   TYPE rsuom_s_calc_runt_values,            "#EC *
    l_s_uom_prop  TYPE rsuom_s_calc_runt_prop_ext.          "#EC *

  IF g_break = rs_c_true. " any key figure
    BREAK-POINT.                                           "#EC NOBREAK
  ENDIF.
  c_abort = 0.
  TRY.                                                      "JD 180102
      CLEAR: g, g_error.


      IF g_record_no_cha_calculation <> record_no.
*   common characteristics calculation
        PERFORM cha_calculation
          CHANGING l_returncode c_t_idocstate c_subrc c_abort.
        IF l_returncode <> 0 OR c_subrc <> 0 OR c_abort <> 0.
          EXIT.
        ENDIF.
      ELSE.
*   don´t calculate, if same record
        g = g_s_cha_calculation.
      ENDIF.


      g-FISCPER = g_s_is-FISCPER.
      g-FISCYEAR = g_s_is-FISCYEAR.

        l_kyf = g_s_is-/BIC/ZZPDTCNT.


          IF NOT g_s_kb IS INITIAL
              AND   g_s_kb-DISTR_CHAN = g-DISTR_CHAN
              AND   g_s_kb-DIVISION = g-DIVISION
              AND   g_s_kb-DOC_TYPE = g-DOC_TYPE
              AND   g_s_kb-DSDEL_DATE = g-DSDEL_DATE
              AND   g_s_kb-MATAV_DATE = g-MATAV_DATE
              AND   g_s_kb-MATERIAL = g-MATERIAL
              AND   g_s_kb-ORD_TYPE = g-ORD_TYPE
              AND   g_s_kb-PLANT = g-PLANT
              AND   g_s_kb-REQ_DATE = g-REQ_DATE
              AND   g_s_kb-SALESORG = g-SALESORG
              AND   g_s_kb-SALES_OFF = g-SALES_OFF
              AND   g_s_kb-SOLD_TO = g-SOLD_TO
              AND   g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              AND   g_s_kb-LOAD_DATE = g-LOAD_DATE
              AND   g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              AND   g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              AND   g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP
              AND   g_s_kb-SHIP_TO = g-SHIP_TO
              AND   g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              AND   g_s_kb-REASON_REJ = g-REASON_REJ
              AND   g_s_kb-CREATEDON = g-CREATEDON
              AND   g_s_kb-CUST_SALES = g-CUST_SALES
              AND   g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              AND   g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC
              AND   g_s_kb-DEALTYPE = g-DEALTYPE
              AND   g_s_kb-ITEM_CATEG = g-ITEM_CATEG
              AND   g_s_kb-DELIV_NUMB = g-DELIV_NUMB
              AND   g_s_kb-DOC_NUMBER = g-DOC_NUMBER
              AND   g_s_kb-MAT_PLANT = g-MAT_PLANT
              AND   g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC
              AND   g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              AND   g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM
              AND   g_s_kb-PO_NUMBER = g-PO_NUMBER
              AND   g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              AND   g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT
              AND   g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT
              AND   g_s_kb-FISCVARNT = g-FISCVARNT
              AND   g_s_kb-CALDAY = g-CALDAY
              AND   g_s_kb-FISCPER = g-FISCPER
              AND   g_s_kb-FISCYEAR = g-FISCYEAR
                  .
            g_s_kb-/BIC/ZZPDTCNT = g_s_kb-/BIC/ZZPDTCNT + l_kyf.
            g_s_kb-fZZPDTCNT = rs_c_true.
            c_val_set = rs_c_true.
          ELSE.
            IF c_val_set = rs_c_true.
              IF c_wa_new = rs_c_false.      "read from table !!!
                DELETE TABLE g_t_kb FROM g_s_kbtbx.
              ENDIF.
              INSERT g_s_kb INTO TABLE g_t_kb.
              c_val_set = rs_c_false.
              CLEAR g_s_kb.
            ENDIF.
            READ TABLE g_t_kb INTO g_s_kb WITH KEY
              DISTR_CHAN = g-DISTR_CHAN
              DIVISION = g-DIVISION
              DOC_TYPE = g-DOC_TYPE
              DSDEL_DATE = g-DSDEL_DATE
              MATAV_DATE = g-MATAV_DATE
              MATERIAL = g-MATERIAL
              ORD_TYPE = g-ORD_TYPE
              PLANT = g-PLANT
              REQ_DATE = g-REQ_DATE
              SALESORG = g-SALESORG
              SALES_OFF = g-SALES_OFF
              SOLD_TO = g-SOLD_TO
              /BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT
              LOAD_DATE = g-LOAD_DATE
              /BIC/ZCMRCAT = g-/BIC/ZCMRCAT
              /BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP
              TCTVAPRCTP = g-TCTVAPRCTP
              SHIP_TO = g-SHIP_TO
              /BIC/ZWORKDAY = g-/BIC/ZWORKDAY
              REASON_REJ = g-REASON_REJ
              CREATEDON = g-CREATEDON
              CUST_SALES = g-CUST_SALES
              /BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG
              /BIC/ZZORC = g-/BIC/ZZORC
              DEALTYPE = g-DEALTYPE
              ITEM_CATEG = g-ITEM_CATEG
              DELIV_NUMB = g-DELIV_NUMB
              DOC_NUMBER = g-DOC_NUMBER
              MAT_PLANT = g-MAT_PLANT
              /BIC/ZZREC = g-/BIC/ZZREC
              /BIC/ZZRECMAN = g-/BIC/ZZRECMAN
              S_ORD_ITEM = g-S_ORD_ITEM
              PO_NUMBER = g-PO_NUMBER
              /BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC
              /BIC/ZZOPDT = g-/BIC/ZZOPDT
              /BIC/ZZPDT = g-/BIC/ZZPDT
              FISCVARNT = g-FISCVARNT
              CALDAY = g-CALDAY
              FISCPER = g-FISCPER
              FISCYEAR = g-FISCYEAR
              .
            l_subrc = sy-subrc.
            IF l_subrc = 0.
              g_s_kbtbx = g_s_kb.
              c_wa_new = rs_c_false.
            ELSE.
              CLEAR g_s_kbtbx.
              CLEAR g_s_kb.
              g_s_kb-DISTR_CHAN = g-DISTR_CHAN.
              g_s_kb-DIVISION = g-DIVISION.
              g_s_kb-DOC_TYPE = g-DOC_TYPE.
              g_s_kb-DSDEL_DATE = g-DSDEL_DATE.
              g_s_kb-MATAV_DATE = g-MATAV_DATE.
              g_s_kb-MATERIAL = g-MATERIAL.
              g_s_kb-ORD_TYPE = g-ORD_TYPE.
              g_s_kb-PLANT = g-PLANT.
              g_s_kb-REQ_DATE = g-REQ_DATE.
              g_s_kb-SALESORG = g-SALESORG.
              g_s_kb-SALES_OFF = g-SALES_OFF.
              g_s_kb-SOLD_TO = g-SOLD_TO.
              g_s_kb-/BIC/ZMATAVDAT = g-/BIC/ZMATAVDAT.
              g_s_kb-LOAD_DATE = g-LOAD_DATE.
              g_s_kb-/BIC/ZCMRCAT = g-/BIC/ZCMRCAT.
              g_s_kb-/BIC/ZPTYP_GRP = g-/BIC/ZPTYP_GRP.
              g_s_kb-TCTVAPRCTP = g-TCTVAPRCTP.
              g_s_kb-SHIP_TO = g-SHIP_TO.
              g_s_kb-/BIC/ZWORKDAY = g-/BIC/ZWORKDAY.
              g_s_kb-REASON_REJ = g-REASON_REJ.
              g_s_kb-CREATEDON = g-CREATEDON.
              g_s_kb-CUST_SALES = g-CUST_SALES.
              g_s_kb-/BIC/ZMAT_PTG = g-/BIC/ZMAT_PTG.
              g_s_kb-/BIC/ZZORC = g-/BIC/ZZORC.
              g_s_kb-DEALTYPE = g-DEALTYPE.
              g_s_kb-ITEM_CATEG = g-ITEM_CATEG.
              g_s_kb-DELIV_NUMB = g-DELIV_NUMB.
              g_s_kb-DOC_NUMBER = g-DOC_NUMBER.
              g_s_kb-MAT_PLANT = g-MAT_PLANT.
              g_s_kb-/BIC/ZZREC = g-/BIC/ZZREC.
              g_s_kb-/BIC/ZZRECMAN = g-/BIC/ZZRECMAN.
              g_s_kb-S_ORD_ITEM = g-S_ORD_ITEM.
              g_s_kb-PO_NUMBER = g-PO_NUMBER.
              g_s_kb-/BIC/ZZCRSDRC = g-/BIC/ZZCRSDRC.
              g_s_kb-/BIC/ZZOPDT = g-/BIC/ZZOPDT.
              g_s_kb-/BIC/ZZPDT = g-/BIC/ZZPDT.
              g_s_kb-FISCVARNT = g-FISCVARNT.
              g_s_kb-CALDAY = g-CALDAY.
              g_s_kb-FISCPER = g-FISCPER.
              g_s_kb-FISCYEAR = g-FISCYEAR.
              c_wa_new = rs_c_true.
            ENDIF.

            g_s_kb-/BIC/ZZPDTCNT = g_s_kb-/BIC/ZZPDTCNT + l_kyf.
            g_s_kb-fZZPDTCNT = rs_c_true.
            c_val_set = rs_c_true.
          ENDIF.
      CATCH cx_rsfo_skip_record.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'S' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_skip_record_as_error.
*     Der Satz wurde in der Formel bewußt übersprungen.
        sy-subrc = 23.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '513' "<<<<<
                g_s_is-recno rs_c_false
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_rsfo_abort_package.
*     In der Formel wurde ein Abbruch ausgelöst.
        sy-subrc = 24.
        c_abort = c_subrc = sy-subrc.
        PERFORM abort_message
         USING 'RSAR' 'E' '514' sy-msgv1
          sy-msgv2 sy-msgv3 sy-msgv4 0 rs_c_true.
        EXIT.

      CATCH cx_sy_arithmetic_error.
        sy-subrc = 10.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0022_ZZPDTCNT' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_sy_conversion_error.
        sy-subrc = 9.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAU' 'E' '487'
                'R0022_ZZPDTCNT' g_s_is-recno
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.

      CATCH cx_dynamic_check INTO g_error_in_formula.

        WHILE g_error_in_formula->previous IS BOUND.
          g_error_in_formula = g_error_in_formula->previous.
        ENDWHILE.
        CALL METHOD g_error_in_formula->get_text
          RECEIVING
            result = g_previous.

        sy-subrc = 20.
        c_subrc = sy-subrc.
        PERFORM error_message USING 'RSAR' 'E' '194'
                'ZZPDTCNT' g_previous
                rs_c_false rs_c_false g_s_is-recno
                CHANGING c_abort.
        EXIT.
    ENDTRY.

*BEGIN TB09042002 HW510455
*END TB09042002

  ENDFORM.                    "r0022_ZZPDTCNT


************************************************************************
* time_conversion
************************************************************************
FORM time_conversion
               USING i_timnm_from TYPE rsiobjnm
                     i_timnm_to   TYPE rsiobjnm
                     i_timvl
                     i_fiscvarnt  TYPE t009b-periv
               CHANGING e_timvl
                        c_t_idocstate  TYPE rsarr_t_idocstate
                        c_subrc   LIKE sy-subrc
                        c_abort   LIKE sy-subrc.            "#EC *
  DATA: l_timvl  TYPE rsd_chavl,
        l_result TYPE rsd_chavl.
  IF i_timvl CO ' 0'.
    CLEAR e_timvl.
    EXIT.
  ENDIF.
  l_timvl = i_timvl.
  CALL FUNCTION 'RST_TOBJ_TO_DERIVED_TOBJ'
    EXPORTING
      i_timnm_from       = i_timnm_from
      i_timnm_to         = i_timnm_to
      i_timvl            = l_timvl
      i_fiscvarnt        = i_fiscvarnt
      i_buffer           = rs_c_true
    IMPORTING
      e_timvl            = l_result
    EXCEPTIONS
      incompatible_tobjs = 1
      no_input_value     = 2
      fiscvarnt_missing  = 3
      input_not_numeric  = 4
      wrong_date         = 5
      wrong_fiscper      = 6
      x_message          = 7
      OTHERS             = 8.
  IF sy-subrc <> 0.
    c_subrc = sy-subrc.
    PERFORM error_message
          USING 'RSAU' 'E' '896'
                i_timnm_from i_timnm_to i_fiscvarnt l_timvl
                g_s_is-recno
          CHANGING c_abort.
  ENDIF.
  e_timvl = l_result.
ENDFORM.                  "TIME_CONVERSION

************************************************************************
* msg_to_handler
************************************************************************
FORM msg_to_handler USING i_msgid     LIKE sy-msgid
                          i_msgty     LIKE sy-msgty
                          i_msgno     LIKE sy-msgno
                          i_msgv1
                          i_msgv2
                          i_msgv3
                          i_msgv4
                          i_record_no LIKE sy-tabix
                    CHANGING
                          e_subrc     LIKE sy-subrc.        "#EC *

  DATA: l_t_log TYPE rssm_t_errorlog_int,
        l_s_log TYPE rssm_s_errorlog_int.
  l_s_log-record  = i_record_no.
  l_s_log-msgid   = i_msgid.
  l_s_log-msgty   = i_msgty.
  l_s_log-msgno   = i_msgno.
  l_s_log-msgv1   = i_msgv1.
  l_s_log-msgv2   = i_msgv2.
  l_s_log-msgv3   = i_msgv3.
  l_s_log-msgv4   = i_msgv4.
  APPEND l_s_log TO l_t_log.
  IF g_s_minfo-crt_package = 'X'.
    LOOP AT l_t_log INTO l_s_log.
      CALL METHOD g_s_minfo-crt_dta_object->ur_msg
        EXPORTING
          i_datapakid = g_s_minfo-datapakid
          i_msgno     = l_s_log-msgno
          i_msgid     = l_s_log-msgid
          i_msgty     = l_s_log-msgty
          i_msgv1     = l_s_log-msgv1
          i_msgv2     = l_s_log-msgv2
          i_msgv3     = l_s_log-msgv3
          i_msgv4     = l_s_log-msgv4.
    ENDLOOP.
  ELSE.
    CALL METHOD g_r_error_handler->add_protocol
      EXPORTING
        i_t_errorlog     = l_t_log
        i_use_crosstab   = rs_c_true
*       I_NO_PSA         = RS_C_FALSE
      EXCEPTIONS
        not_in_cross_tab = 1
        too_many_errors  = 2
        OTHERS           = 3
          .
    IF sy-subrc <> 0.
      e_subrc = sy-subrc.
      CALL METHOD g_r_error_handler->commit
        EXPORTING
          i_aufrufer = '59'
          i_abort    = rs_c_false.
    ENDIF.
  ENDIF.

ENDFORM.                "MSG_TO_HANDLER

************************************************************************
* abort_message
************************************************************************
FORM abort_message  USING i_msgid        LIKE sy-msgid
                          i_msgty        LIKE sy-msgty
                          i_msgno        LIKE sy-msgno
                          i_msgv1
                          i_msgv2
                          i_msgv3
                          i_msgv4
                          i_record_no    TYPE i
                          i_with_handler TYPE rs_bool.      "#EC *

  DATA: l_subrc LIKE sy-subrc.
  IF i_with_handler = rs_c_true.
    PERFORM msg_to_handler
       USING i_msgid i_msgty i_msgno i_msgv1
             i_msgv2 i_msgv3 i_msgv4 i_record_no
       CHANGING l_subrc.
  ENDIF.
  IF NOT g_s_minfo-crt_package = 'X'.
    CALL METHOD g_r_error_handler->commit
      EXPORTING
        i_aufrufer = '59'
        i_abort    = rs_c_true.
  ENDIF.
ENDFORM.                "ABORT_MESSAGE

************************************************************************
* error_message
************************************************************************
FORM error_message  USING i_msgid        LIKE sy-msgid
                          i_msgty        LIKE sy-msgty
                          i_msgno        LIKE sy-msgno
                          i_msgv1
                          i_msgv2
                          i_msgv3
                          i_msgv4
                          i_record_no    TYPE i
                    CHANGING
                          c_abort       LIKE sy-subrc.      "#EC *

  DATA: l_subrc LIKE sy-subrc,
        l_subrc_23 LIKE sy-subrc.

  l_subrc_23 = sy-subrc.

  PERFORM msg_to_handler_2
       USING i_msgid i_msgty i_msgno i_msgv1
             i_msgv2 i_msgv3 i_msgv4 i_record_no
       CHANGING l_subrc.
  IF l_subrc = 0 AND g_continue_at_err = rs_c_false AND
     NOT g_s_minfo-crt_package = 'X'.
*   no commit set in msg_to_handler
*SK768911
*    CALL METHOD g_r_error_handler->commit
*      EXPORTING
*        i_aufrufer = '59'
*        i_abort    = rs_c_false.
  ENDIF.
  IF l_subrc_23 = 23.
    c_abort = 0.
  ELSE.
    IF l_subrc <> 0 OR g_continue_at_err = rs_c_false.
      c_abort = 1.
    ELSE.
      c_abort = 0.
    ENDIF.
  ENDIF.
ENDFORM.                "ERROR_MESSAGE

************************************************************************
* MSG_TO_MONITOR
************************************************************************
FORM msg_to_monitor USING i_msgid     LIKE sy-msgid
                          i_msgty     LIKE sy-msgty
                          i_msgno     LIKE sy-msgno
                          i_msgv1
                          i_msgv2
                          i_msgv3
                          i_msgv4.                          "#EC *

  DATA: l_s_rsmondata TYPE rsmonview,
        l_t_rsmondata TYPE rsmonview OCCURS 0.

  l_s_rsmondata-msgid     = i_msgid.
  l_s_rsmondata-msgty     = i_msgty.
  l_s_rsmondata-msgno     = i_msgno.
  l_s_rsmondata-msgv1     = i_msgv1.
  l_s_rsmondata-msgv2     = i_msgv2.
  l_s_rsmondata-msgv3     = i_msgv3.
  l_s_rsmondata-msgv4     = i_msgv4.
  l_s_rsmondata-rnr       = i_requnr.
  l_s_rsmondata-datapakid = i_datap.
  l_s_rsmondata-aufrufer  = '5E'.
  IF i_msgty = 'E'
  OR i_msgty = 'A'
  OR i_msgty = 'X'.
    l_s_rsmondata-error   = rs_c_true.
  ELSE.
    l_s_rsmondata-error   = rs_c_false.
  ENDIF.
  APPEND l_s_rsmondata TO l_t_rsmondata.

* user defined messages monitor entries
  IF g_s_minfo-crt_package = 'X'.
    LOOP AT l_t_rsmondata INTO l_s_rsmondata.
      CALL METHOD g_s_minfo-crt_dta_object->ur_msg
        EXPORTING
          i_datapakid = g_s_minfo-datapakid
          i_msgno     = l_s_rsmondata-msgno
          i_msgid     = l_s_rsmondata-msgid
          i_msgty     = l_s_rsmondata-msgty
          i_msgv1     = l_s_rsmondata-msgv1
          i_msgv2     = l_s_rsmondata-msgv2
          i_msgv3     = l_s_rsmondata-msgv3
          i_msgv4     = l_s_rsmondata-msgv4.

    ENDLOOP.
  ELSE.
    CALL FUNCTION 'RSSM_MON_WRITE_MONITOR_IC'
      EXPORTING
        aggregate = 'N'
      TABLES
        data      = l_t_rsmondata.
  ENDIF.

ENDFORM.               "MSG_TO_MONITOR
************************************************************************
* REQUEST_ERROR
************************************************************************
FORM request_error USING i_msgid     LIKE sy-msgid
                         i_msgty     LIKE sy-msgty
                         i_msgno     LIKE sy-msgno
                         i_msgv1
                         i_msgv2
                         i_msgv3
                         i_msgv4
                         i_routid    TYPE c
                   CHANGING c_t_idocstate TYPE rsarr_t_idocstate."#EC *
  DATA: l_s_idocstate TYPE rsarr_s_idocstate.

  l_s_idocstate-msgid  = i_msgid.
  l_s_idocstate-msgty  = i_msgty.
  l_s_idocstate-msgno  = i_msgno.
  l_s_idocstate-msgv1  = i_msgv1.
  l_s_idocstate-msgv2  = i_msgv2.
  l_s_idocstate-msgv3  = i_msgv3.
  l_s_idocstate-msgv4  = i_msgv4.

  l_s_idocstate-status = rsarr_c_idocstate_error.
  l_s_idocstate-uname  = sy-uname.
  l_s_idocstate-repid  = sy-repid.
  l_s_idocstate-routid = i_routid.
  APPEND l_s_idocstate TO c_t_idocstate.

  PERFORM msg_to_monitor USING i_msgid i_msgty i_msgno
                               i_msgv1 i_msgv2 i_msgv3 i_msgv4.
ENDFORM.     "REQUEST_ERROR





* for special logic
FORM get_ref_to_commstru CHANGING r_s_comm TYPE REF TO data
                                  r_t_comm TYPE REF TO data."#EC CALLED
  CREATE DATA r_s_comm TYPE g_s_comstru.
  CREATE DATA r_t_comm TYPE STANDARD TABLE OF g_s_comstru.
ENDFORM.                    "get_ref_to_commstru
************************************************************************
* msg_to_handler_2
************************************************************************
FORM msg_to_handler_2 USING i_msgid     LIKE sy-msgid
                            i_msgty     LIKE sy-msgty
                            i_msgno     LIKE sy-msgno
                            i_msgv1
                            i_msgv2
                            i_msgv3
                            i_msgv4
                            i_record_no LIKE sy-tabix
                      CHANGING
                            e_subrc     LIKE sy-subrc.      "#EC *

  DATA: l_t_log TYPE rssm_t_errorlog_int,
        l_s_log TYPE rssm_s_errorlog_int.
  l_s_log-record  = i_record_no.
  l_s_log-msgid   = i_msgid.
  l_s_log-msgty   = i_msgty.
  IF l_s_log-msgty = 'E'
  OR l_s_log-msgty = 'A'
  OR l_s_log-msgty = 'X'.
    g_count_error = g_count_error + 1.
    g_error       = rs_c_true.
  ENDIF.
  l_s_log-msgno   = i_msgno.
  l_s_log-msgv1   = i_msgv1.
  l_s_log-msgv2   = i_msgv2.
  l_s_log-msgv3   = i_msgv3.
  l_s_log-msgv4   = i_msgv4.
  APPEND l_s_log TO l_t_log.
  IF g_s_minfo-crt_package = 'X'.
    LOOP AT l_t_log INTO l_s_log.
      CALL METHOD g_s_minfo-crt_dta_object->ur_msg
        EXPORTING
          i_datapakid = g_s_minfo-datapakid
          i_msgno     = l_s_log-msgno
          i_msgid     = l_s_log-msgid
          i_msgty     = l_s_log-msgty
          i_msgv1     = l_s_log-msgv1
          i_msgv2     = l_s_log-msgv2
          i_msgv3     = l_s_log-msgv3
          i_msgv4     = l_s_log-msgv4.
    ENDLOOP.
  ELSE.
    CALL METHOD g_r_error_handler->add_protocol
      EXPORTING
        i_t_errorlog     = l_t_log
        i_use_crosstab   = rs_c_false
*       I_NO_PSA         = RS_C_FALSE
      EXCEPTIONS
        not_in_cross_tab = 1
        too_many_errors  = 2
        OTHERS           = 3
          .
    IF sy-subrc <> 0.
      e_subrc = sy-subrc.
      CALL METHOD g_r_error_handler->commit
        EXPORTING
          i_aufrufer = '59'
          i_abort    = rs_c_false.
    ENDIF.
  ENDIF.

ENDFORM.                "MSG_TO_HANDLER_2
************************************************************************
* MSG_TO_MONITOR_2
************************************************************************
FORM MSG_TO_MONITOR_2 USING
            i_t_monitor    like monitor[].


DATA: l_s_rsmondata type rsmonview,
      l_t_rsmondata type rsmonview occurs 0.

field-symbols: <l_s_monitor> like line of monitor[].


  Loop at i_t_monitor assigning <l_s_monitor>.
    l_s_rsmondata-msgid     = <l_s_monitor>-msgid.
    l_s_rsmondata-msgty     = <l_s_monitor>-msgty.
    l_s_rsmondata-msgno     = <l_s_monitor>-msgno.
    l_s_rsmondata-msgv1     = <l_s_monitor>-msgv1.
    l_s_rsmondata-msgv2     = <l_s_monitor>-msgv2.
    l_s_rsmondata-msgv3     = <l_s_monitor>-msgv3.
    l_s_rsmondata-msgv4     = <l_s_monitor>-msgv4.
    l_s_rsmondata-rnr       = g_s_minfo-requnr.
    l_s_rsmondata-datapakid = g_s_minfo-datapakid.
    l_s_rsmondata-aufrufer  = '5E'.
    if <l_s_monitor>-msgty = 'E'
    or <l_s_monitor>-msgty = 'A'
    or <l_s_monitor>-msgty = 'X'.
      g_count_error = g_count_error + 1.
      l_s_rsmondata-error   = rs_c_true.
    else.
      l_s_rsmondata-error   = rs_c_false.
    endif.
    append l_s_rsmondata to l_t_rsmondata.
  ENDLOOP.

* user defined messages monitor entries
  if g_s_minfo-crt_package = 'X'.
    loop at l_t_rsmondata into l_s_rsmondata.
      call method g_s_minfo-crt_dta_object->ur_msg
        exporting
          i_datapakid = g_s_minfo-datapakid
          i_msgno     = l_s_rsmondata-msgno
          i_msgid     = l_s_rsmondata-msgid
          i_msgty     = l_s_rsmondata-msgty
          i_msgv1     = l_s_rsmondata-msgv1
          i_msgv2     = l_s_rsmondata-msgv2
          i_msgv3     = l_s_rsmondata-msgv3
          i_msgv4     = l_s_rsmondata-msgv4.

    endloop.
  else.
    call function 'RSSM_MON_WRITE_MONITOR_IC'
         exporting
              aggregate = 'N'
         tables
              data      = l_t_rsmondata.
  endif.

ENDFORM.

*.. The following subroutine is for internal (FuGr RSSG) use only! ..*


FORM _RSSG_PROGRAM_LOADCHECK
     CHANGING E_S_PDIR TYPE RSSGTPDIR
              E_FOUND  TYPE FLAG. "#EC CALLED
CONSTANTS:
BEGIN OF _RSSG_C_PCLA,
  PROGCLASS TYPE  RSSGTPCLA-PROGCLASS
    VALUE 'RSAUTMPLUR',
  CLIDEP TYPE  RSSGTPCLA-CLIDEP
    VALUE ' ',
  GENFLAG TYPE  RSSGTPCLA-GENFLAG
    VALUE 'X',
  SUBC TYPE  RSSGTPCLA-SUBC
    VALUE '1',
  TEMPLATE TYPE  RSSGTPCLA-TEMPLATE
    VALUE 'RSTMPLUR',
END   OF _RSSG_C_PCLA,

BEGIN OF _RSSG_C_PDIR,
  UNI_IDC25 TYPE  RSSGTPDIR-UNI_IDC25
    VALUE '3Y9DFGB7DROX3F8WGYMIY8U81',
  CLIENT TYPE  RSSGTPDIR-CLIENT
    VALUE ' ',
  PROGCLASS TYPE  RSSGTPDIR-PROGCLASS
    VALUE 'RSAUTMPLUR',
  PROGNAME TYPE  RSSGTPDIR-PROGNAME
    VALUE 'GP3Y9DFGB7DROX3F8WGYMIY8U81',
  CREUSER TYPE  RSSGTPDIR-CREUSER
    VALUE 'DDIC',
  CRETSTMP TYPE  RSSGTPDIR-CRETSTMP
    VALUE 20151220110936,
  GENUSER TYPE  RSSGTPDIR-GENUSER
    VALUE 'CYBAGENT',
  GENTSTMP TYPE  RSSGTPDIR-GENTSTMP
    VALUE 20230602060105,
  GENRELEASE TYPE  RSSGTPDIR-GENRELEASE
    VALUE '750',
  GENSTATUS TYPE  RSSGTPDIR-GENSTATUS
    VALUE '00',
  TEMPLATE TYPE  RSSGTPDIR-TEMPLATE
    VALUE 'RSTMPLUR',
  UPDUSERTPL TYPE  RSSGTPDIR-UPDUSERTPL
    VALUE 'SAP',
  UPDDATETPL TYPE  RSSGTPDIR-UPDDATETPL
    VALUE '20210515',
  UPDTIMETPL TYPE  RSSGTPDIR-UPDTIMETPL
    VALUE '145701',
  SUBC TYPE  RSSGTPDIR-SUBC
    VALUE '1',
  WITH_SUBTEMPL TYPE  RSSGTPDIR-WITH_SUBTEMPL
    VALUE ' ',
END   OF _RSSG_C_PDIR.

  MOVE-CORRESPONDING _RSSG_C_PDIR TO E_S_PDIR.
  E_FOUND = 'X'.
ENDFORM.
