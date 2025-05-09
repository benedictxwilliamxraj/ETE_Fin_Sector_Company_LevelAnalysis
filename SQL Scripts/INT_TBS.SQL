CREATE MATERIALIZED VIEW INT_TBS.MV_FUNDA
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT
    gvkey,
    fyear         AS year,
  datadate      AS report_date,
  revt          AS revenue,
  ni            AS net_income,
  at            AS assets,
  seq           AS equity,
  dltt          AS total_debt,
  oiadp         AS operating_income,
  xint          AS interest_expense,
  ebit,
  xrd           AS rd_expense,
  capx          AS capex,
  csho          AS shares_outstanding,
  prcc_f        AS price
FROM RAW_TBS.COMP_NA_FUND_A_RAW
WHERE indfmt  IN ('INDL')
  AND datafmt = 'STD'
  AND popsrc  = 'D'
  AND consol  = 'C'
  AND fyear > 2020
;

CREATE  MATERIALIZED VIEW INT_TBS.MV_GICS
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT
    gvkey,
    gsector
FROM RAW_TBS.COMPANY_LIST_RAW;

CREATE MATERIALIZED VIEW INT_TBS.MV_MSF
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT
    PERMNO,
    CUSIP,
    TO_DATE("DATE",'YYYY-MM-DD') as "DATE",
    RET,
    PRC,
    ALTPRC,
    SHROUT,
    prc * shrout * 1000 AS me
FROM RAW_TBS.CRSP_A_STOCK_MSF_RAW
WHERE "DATE" > TO_DATE('2020-01-01','YYYY-MM-DD')
  AND RET IS NOT NULL
;

CREATE MATERIALIZED VIEW INT_TBS.MV_IBES_FULL
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT
    permno,
    PUBLIC_DATE,
    gvkey,
    capei,
    bm,
    evm,
    pe_op_basic,
    pe_op_dil,
    pe_exi,
    pe_inc,
    ps,
    pcf,
    dpr,
    ptb,
    peg_trailing,
    divyield,
    peg_1yrforward,
    peg_ltgforward,
    efftax,
    gprof,
    aftret_eq,
    aftret_equity,
    aftret_invcapx,
    gpm,
    npm,
    opmad,
    opmbd,
    pretret_earnat,
    pretret_noa,
    ptpm,
    roa,
    roce,
    roe,
    capital_ratio,
    equity_invcap,
    debt_invcap,
    totdebt_invcap,
    invt_act,
    rect_act,
    fcf_ocf,
    ocf_lct,
    cash_debt,
    cash_lt,
    cfm,
    short_debt,
    profit_lct,
    curr_debt,
    debt_ebitda,
    dltt_be,
    int_debt,
    int_totdebt,
    lt_debt,
    lt_ppent,
    de_ratio,
    debt_assets,
    debt_at,
    debt_capital,
    intcov,
    intcov_ratio,
    cash_conversion,
    cash_ratio,
    curr_ratio,
    quick_ratio,
    at_turn,
    inv_turn,
    pay_turn,
    rect_turn,
    sale_equity,
    sale_invcap,
    sale_nwc,
    accrual,
    rd_sale,
    adv_sale,
    staff_sale
FROM RAW_TBS.FIN_RATIOS_IBES_RAW
WHERE PUBLIC_DATE > DATE '2020-01-01'
;


CREATE MATERIALIZED VIEW INT_TBS.MV_FUNDQ
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT
    gvkey,
    datadate as REPORT_DATE,
    fyearq    AS QYEAR,
    fqtr      AS quarter,
    niq        AS net_income,
    revtq      AS revenue,
    atq        AS assets,
    seqq       AS equity,
    dlttq      AS total_debt,
    NVL(oiadpq, oibdpq) AS ebit,      -- whichever your source uses
    xintq      AS interest_expense,
    xrdq       AS rd_expense,
    CAPXY      AS capex,              -- or capxq if that’s the name
    cshoq      AS shares_outstanding,
    prccq      AS price
FROM RAW_TBS.COMP_NA_FUND_Q_RAW
WHERE indfmt IN ('INDL')
  AND datafmt = 'STD'
  AND popsrc = 'D'
  AND consol  = 'C'
  AND REGEXP_LIKE(datadate, '^\d{2}-[A-Z]{3}-\d{2}$');


################## JOBS #####################


BEGIN
  DBMS_SCHEDULER.CREATE_JOB (
    job_name        => 'REFRESH_MV_FUNDQ',
    job_type        => 'PLSQL_BLOCK',
    job_action      => 'BEGIN DBMS_MVIEW.REFRESH(''INT_TBS.MV_FUNDQ'', ''C''); END;',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=MONTHLY; BYMONTHDAY=1; BYHOUR=7; BYMINUTE=0; BYSECOND=0',
    enabled         => TRUE,
    comments        => 'Refresh MV_FUNDQ on 1st of every month at 2AM'
  );
END;