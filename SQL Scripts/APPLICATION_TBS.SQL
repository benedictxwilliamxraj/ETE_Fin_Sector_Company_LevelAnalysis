CREATE MATERIALIZED VIEW APPLICATION_TBS.FACT_COMPANY_ANNUAL_MV
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
WITH
    -- A) Annual Compustat fundamentals
    stg_comp_a AS (
        SELECT
            gvkey,
            "YEAR",
            net_income,
            revenue,
            assets,
            equity,
            total_debt,
            operating_income,
            interest_expense,
            ebit,
            rd_expense,
            capex,
            shares_outstanding,
            price        AS price_close
        FROM INT_TBS.MV_FUNDA
    ),

    -- B) CRSP monthly → annual aggregates
  stg_crsp AS (
    SELECT
      permno,
      cusip,
      ret,
      me               AS market_equity,
      EXTRACT(YEAR FROM "DATE")  AS year
    FROM INT_TBS.MV_MSF
    WHERE "DATE" >= DATE '2020-01-01'
      AND ret IS NOT NULL
  ),

  crsp_ann AS (
    SELECT
      permno,
      year,
      EXP(SUM(LN(1 + NVL(ret,0)))) - 1  AS annual_return,
      AVG(market_equity)                AS avg_mktcap_crsp
    FROM stg_crsp
    GROUP BY permno, year
  ),

    -- C) IBES ratios (for permno & fallback ratios)
    stg_ibes AS (
                    SELECT
                    gvkey,
                    permno,
                    EXTRACT(YEAR FROM TO_DATE(public_date,'YY-MM-DD')) AS year,
    pe_exi,
    ptb
    FROM INT_TBS.MV_IBES_FULL
    ),

    -- D) GICS sectors from company list
    stg_gics AS (
                    SELECT gvkey, gsector
                    FROM INT_TBS.MV_GICS
                ),

    -- E) Bring it all together
    comp_base AS (
                     SELECT
                     a.gvkey,
                     i.permno,
    -- COALESCE(g.gsector, i.gsector)   AS gsector,
                     g.gsector,
                     a."YEAR",
                     a.net_income,
                     a.revenue,
                     a.assets,
                     a.equity,
                     a.total_debt,
                     a.shares_outstanding,
                     a.price_close,
                     a.rd_expense,
                     a.capex,
                     a.operating_income,
                     a.interest_expense,
                     a.ebit,
                     i.pe_exi                         AS ibes_pe,
                     i.ptb                            AS ibes_pb,
                     cr.annual_return,
                     cr.avg_mktcap_crsp
                     FROM stg_comp_a a
                     LEFT JOIN stg_ibes    i ON a.gvkey = i.gvkey AND a."YEAR" = i."YEAR"
                     LEFT JOIN stg_gics    g ON a.gvkey = g.gvkey
                     LEFT JOIN crsp_ann    cr ON i.permno = cr.permno AND a."YEAR" = cr."YEAR"
                 ),

    -- F) Compute the 15 KPIs + CRSP metrics
    fact_cte AS (
                    SELECT
                    gvkey,
                    permno,
                    gsector,
                    "YEAR",

    -- Profitability & Efficiency
                    CASE
                    WHEN equity = 0 THEN NULL                -- or 0, or -1, or 'N/A'
                    ELSE net_income / equity
                    END AS roe,
                    CASE
                    WHEN assets = 0 THEN NULL                -- or 0, or -1, or 'N/A'
                    ELSE net_income / assets
                    END AS roa,
                    CASE
                    WHEN revenue = 0 THEN NULL                -- or 0, or -1, or 'N/A'
                    ELSE operating_income / revenue
                    END AS op_margin,

    --   net_income   / assets                    AS roa,
    --   operating_income / revenue               AS op_margin,
                    CASE
                    WHEN revenue = 0 THEN NULL                -- or 0, or -1, or 'N/A'
                    ELSE net_income   / revenue
                    END AS net_margin,
    --   net_income   / revenue                   AS net_margin,
                    CASE
                    WHEN assets = 0 THEN NULL                -- or 0, or -1, or 'N/A'
                    ELSE revenue/ assets
                    END AS asset_turn,
    --   revenue      / assets                    AS asset_turn,

--       -- Growth (YoY)
    -- revenue growth, safely avoiding zero or NULL denominators
    (revenue - LAG(revenue) OVER (PARTITION BY gvkey ORDER BY year))
    / NULLIF(LAG(revenue)   OVER (PARTITION BY gvkey ORDER BY year), 0)
    AS revenue_growth,

(net_income - LAG(net_income) OVER (PARTITION BY gvkey ORDER BY year))
    / NULLIF(LAG(net_income) OVER (PARTITION BY gvkey ORDER BY year), 0)
    AS netincome_growth,



--     --   -- EPS
    CASE
    WHEN shares_outstanding = 0 THEN NULL                -- or 0, or -1, or 'N/A'
    ELSE net_income   / shares_outstanding
    END AS eps,
    --     --   net_income   / shares_outstanding          AS eps,

--     --   -- Leverage & Risk
--     --   total_debt   / equity                      AS debt_equity,
    CASE
    WHEN equity = 0 THEN NULL                -- or 0, or -1, or 'N/A'
    ELSE total_debt   / equity
    END AS debt_equity,
--     --   ebit         / NULLIF(interest_expense,0)  AS interest_coverage,
    CASE
    WHEN NULLIF(interest_expense,0) = 0 THEN NULL                -- or 0, or -1, or 'N/A'
    ELSE  ebit  / NULLIF(interest_expense,0)
    END AS interest_coverage,
--       assets       / equity                      AS financial_leverage,
    CASE
    WHEN equity = 0 THEN NULL                -- or 0, or -1, or 'N/A'
    ELSE  assets  / equity
    END AS financial_leverage,
    --   -- Market & Valuation
    price_close  * shares_outstanding          AS market_cap_cstra,

    --   price_close  / NULLIF(net_income/shares_outstanding,0) AS pe_ratio,
    CASE
    WHEN shares_outstanding = 0 THEN NULL                -- or 0, or -1, or 'N/A'
    ELSE  price_close  / NULLIF(net_income/shares_outstanding,0)
    END AS pe_ratio,

    --   price_close  / NULLIF(equity/shares_outstanding,0)     AS pb_ratio,
    CASE
    WHEN shares_outstanding = 0 THEN NULL                -- or 0, or -1, or 'N/A'
    ELSE  price_close  / NULLIF(equity/shares_outstanding,0)
    END AS pb_ratio,

    --   -- Innovation & Investment
    --   rd_expense   / revenue * 100               AS rd_pct,
    CASE
    WHEN revenue = 0 THEN NULL                -- or 0, or -1, or 'N/A'
    ELSE  rd_expense   / revenue * 100
    END AS rd_pct,

    --   capex        / revenue * 100               AS capex_pct,
    CASE
    WHEN revenue = 0 THEN NULL                -- or 0, or -1, or 'N/A'
    ELSE  capex   / revenue * 100
    END AS capex_pct,

    --   -- CRSP‐derived
    annual_return,
    avg_mktcap_crsp

    FROM comp_base
    )

SELECT * FROM fact_cte
ORDER BY gvkey, "YEAR";



################################################################
CREATE MATERIALIZED VIEW APPLICATION_TBS.DIM_COMPANY_MV
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT DISTINCT
    cl.GVKEY,
    ib.PERMNO,
    -- ms.CUSIP,
    cl.CONML       AS COMPANY_NAME,
    cl.ADD1,
    cl.ADD2,
    cl.ADD3,
    cl.ADD4,
    cl.ADDZIP,
    cl.BUSDESC,
    cl.CIK,
    cl.CITY,
    cl.COSTAT,
    cl.COUNTY,
    cl.DLRSN,
    cl.EIN,
    cl.FAX,
    cl.FIC,
    cl.FYRC,
    cl.GGROUP,
    cl.GIND,
    cl.GSECTOR,
    cl.GSUBIND,
    cl.IDBFLAG,
    cl.INCORP,
    cl.LOC,
    cl.NAICS,
    cl.PHONE,
    cl.PRICAN,
    cl.PRIROW,
    cl.PRIUSA,
    cl.SIC,
    cl.SPCINDCD,
    cl.SPCSECCD,
    cl.SPCSRC,
    cl.STATE,
    cl.STKO,
    cl.WEBURL,
    -- convert TEXT IPODATE if necessary; assuming it's already DATE
    cl.IPODATE,
    cl.CURR_SP500_FLAG
FROM INT_TBS.MV_COMPANY_LIST cl
         -- map to the latest IBES permno per gvkey
         LEFT JOIN (
    SELECT /*+ materialize */
        gvkey, permno
    FROM (
             SELECT
                 gvkey,
                 permno,
                 ROW_NUMBER() OVER (
          PARTITION BY gvkey
          ORDER BY PUBLIC_DATE DESC
        ) AS rn
             FROM INT_TBS.MV_IBES_FULL
         )
    WHERE rn = 1
) ib
                   ON cl.GVKEY = ib.GVKEY

    -- grab a representative CUSIP per permno
         LEFT JOIN (
    SELECT
        permno,
        MIN(CUSIP) AS CUSIP
    FROM INT_TBS.MV_MSF
    GROUP BY permno
) ms
                   ON ib.PERMNO = ms.PERMNO


#######################
CREATE MATERIALIZED VIEW APPLICATION_TBS.MV_DIM_DATE
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
WITH month_range AS (
  SELECT ADD_MONTHS(DATE '2015-01-01', LEVEL - 1) AS first_of_month
  FROM dual
  CONNECT BY LEVEL <= 12 * 30  -- 30 years of months
)
SELECT
    first_of_month,
    EXTRACT(YEAR FROM first_of_month)    AS year,
  EXTRACT(MONTH FROM first_of_month)   AS month,
  'Q' || TO_CHAR(first_of_month, 'Q')  AS quarter
FROM month_range;





###########################
CREATE MATERIALIZED VIEW APPLICATION_TBS.DIM_SECTOR_MV
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT DISTINCT
    ib.gsector    AS sector_code,
    ib.gicdesc    AS sector_name
FROM RAW_TBS.FIN_RATIOS_IBES_RAW ib
WHERE ib.gsector IS NOT NULL
UNION ALL
SELECT
    '1'            AS sector_code,
    'Other'        AS sector_name
FROM   dual
;

####
CREATE MATERIALIZED VIEW  APPLICATION_TBS.FACT_SECTOR_ANNUAL_MV
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT
    /* replace any null sector_code with '1' */
    NVL(f.gsector, '1')                            AS sector_code,
    /* bring in the sector_name from DIM_SECTOR */
    ds.sector_name                                 AS sector_name,
    f.year                                         AS year_key,

    -- Profitability & Efficiency (sector averages)
    AVG(f.roe)                                     AS avg_roe,
    AVG(f.roa)                                     AS avg_roa,
    AVG(f.op_margin)                               AS avg_op_margin,
    AVG(f.net_margin)                              AS avg_net_margin,
    AVG(f.asset_turn)                              AS avg_asset_turn,

    -- Growth (sector averages)
    AVG(f.revenue_growth)                          AS avg_revenue_growth,
    AVG(f.netincome_growth)                        AS avg_netincome_growth,

    -- EPS (sector average)
    AVG(f.eps)                                     AS avg_eps,

    -- Leverage & Risk (sector averages)
    AVG(f.debt_equity)                             AS avg_debt_equity,
    AVG(f.interest_coverage)                       AS avg_interest_coverage,
    AVG(f.financial_leverage)                      AS avg_financial_leverage,

    -- Market & Valuation
    AVG(f.pe_ratio)                                AS avg_pe_ratio,
    AVG(f.pb_ratio)                                AS avg_pb_ratio,

    -- Innovation & Investment (sector averages)
    AVG(f.rd_pct)                                  AS avg_rd_pct,
    AVG(f.capex_pct)                               AS avg_capex_pct,

    -- Market metrics
    AVG(f.annual_return)                           AS avg_annual_return,
    SUM(f.avg_mktcap_crsp)                         AS total_mktcap

FROM APPLICATION_TBS.FACT_COMPANY_ANNUAL_MV f
         /* join to DIM_SECTOR on the replaced code */
         LEFT JOIN APPLICATION_TBS.DIM_SECTOR_MV ds
                   ON ds.sector_code = NVL(f.gsector, '1')
WHERE F.GSECTOR <> '1'
GROUP BY
    NVL(f.gsector, '1'),
    ds.sector_name,
    f.year

####################

CREATE MATERIALIZED VIEW  APPLICATION_TBS.FINAL_RANK_MV
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
WITH sector_summ AS (
  SELECT
    f. sector_code                     AS sector_code,
    ds.sector_name                  AS sector_name,
    EXP(
      SUM(LN(1 + COALESCE(f.avg_annual_return,0))) / 5
    ) - 1                           AS cagr_return,
    AVG(f.avg_roe)                  AS avg_roe,
    AVG(f.avg_revenue_growth)       AS avg_rev_growth,
    AVG(f.avg_pe_ratio)             AS avg_pe
  FROM FACT_SECTOR_ANNUAL_MV f
  LEFT JOIN application_tbs.DIM_SECTOR ds
    ON f.sector_code = ds.sector_code
  WHERE f.year_key BETWEEN 2020 AND 2024
  GROUP BY f.sector_code, ds.sector_name
),

--------------------------------------------------------------------------------
-- 2) Normalize each metric 0–1
--------------------------------------------------------------------------------
norm AS (
  SELECT
    sector_code,
    sector_name,
    cagr_return,
    avg_roe,
    avg_rev_growth,
    avg_pe,

    (cagr_return - MIN(cagr_return) OVER()) /
    (MAX(cagr_return) OVER() - MIN(cagr_return) OVER())
      AS pr_return,

    (avg_roe - MIN(avg_roe) OVER()) /
    (MAX(avg_roe) OVER() - MIN(avg_roe) OVER())
      AS pr_roe,

    (avg_rev_growth - MIN(avg_rev_growth) OVER()) /
    (MAX(avg_rev_growth) OVER() - MIN(avg_rev_growth) OVER())
      AS pr_rev,

    1 - (
      (avg_pe - MIN(avg_pe) OVER()) /
      (MAX(avg_pe) OVER() - MIN(avg_pe) OVER())
    )                                 AS pr_pe_inv

  FROM sector_summ
),

--------------------------------------------------------------------------------
-- 3) Compute composite score (40% return, 20% ROE, 20% growth, 20% inv. P/E)
--------------------------------------------------------------------------------
composite AS (
  SELECT
    sector_code,
    sector_name,
    cagr_return,
    avg_roe,
    avg_rev_growth,
    avg_pe,
    pr_return,
    pr_roe,
    pr_rev,
    pr_pe_inv,

    0.40 * pr_return
  + 0.20 * pr_roe
  + 0.20 * pr_rev
  + 0.20 * pr_pe_inv                 AS composite_score
  FROM norm
),

--------------------------------------------------------------------------------
-- 4) Compute both composite rank and Borda‐count rank,
--    making sure to carry forward the normalized cols
--------------------------------------------------------------------------------
borda_rank AS (
  SELECT
    sector_code,
    sector_name,
    cagr_return,
    avg_roe,
    avg_rev_growth,
    avg_pe,
    pr_return,
    pr_roe,
    pr_rev,
    pr_pe_inv,
    composite_score,
    RANK() OVER (ORDER BY composite_score DESC)                AS rank_composite,
    RANK() OVER (ORDER BY cagr_return DESC)                    AS r_return,
    RANK() OVER (ORDER BY avg_roe DESC)                        AS r_roe,
    RANK() OVER (ORDER BY avg_rev_growth DESC)                 AS r_rev,
    RANK() OVER (ORDER BY avg_pe ASC)                          AS r_pe
  FROM composite
),
final AS (
  SELECT
    sector_code,
    sector_name,
    ROUND(cagr_return,4)    AS cagr_return,
    ROUND(avg_roe,4)        AS avg_roe,
    ROUND(avg_rev_growth,4) AS avg_revenue_growth,
    ROUND(avg_pe,4)         AS avg_pe_ratio,
    ROUND(pr_return,4)      AS norm_return,
    ROUND(pr_roe,4)         AS norm_roe,
    ROUND(pr_rev,4)         AS norm_rev_growth,
    ROUND(pr_pe_inv,4)      AS norm_pe_inv,
    ROUND(composite_score,4)AS composite_score,
    rank_composite          AS composite_rank,
    (r_return + r_roe + r_rev + r_pe)              AS borda_sum,
    DENSE_RANK() OVER (ORDER BY r_return + r_roe + r_rev + r_pe) AS borda_rank
  FROM borda_rank
)
SELECT *
FROM final
ORDER BY composite_score DESC
;

#############################
CREATE MATERIALIZED VIEW APPLICATION_TBS.MV_TOP_SECTOR_COMPOSITE
  BUILD IMMEDIATE
  REFRESH COMPLETE ON DEMAND
AS
WITH fact_cte AS (
  SELECT *
    FROM APPLICATION_TBS.FACT_SECTOR_ANNUAL_MV
),
ranked AS (
  SELECT
    sector_code,
    YEAR_KEY,
    sector_name,
    -- raw KPIs
    avg_roe,
    avg_roa,
    avg_op_margin,
    avg_net_margin,
    avg_asset_turn,
    avg_debt_equity,
    AVG_EPS,
    AVG_RD_PCT       AS rd_pct,
    AVG_CAPEX_PCT    AS capex_pct,
    AVG_ANNUAL_RETURN,
    total_mktcap       AS avg_mktcap_crsp,

    -- count of sectors per year
    COUNT(*) OVER (PARTITION BY YEAR_KEY) AS cnt,

    -- DENSE_RANK() for each KPI (1 = best)
    DENSE_RANK() OVER (PARTITION BY YEAR_KEY ORDER BY avg_roe         DESC) AS r_roe,
    DENSE_RANK() OVER (PARTITION BY YEAR_KEY ORDER BY avg_roa         DESC) AS r_roa,
    DENSE_RANK() OVER (PARTITION BY YEAR_KEY ORDER BY avg_op_margin   DESC) AS r_op_margin,
    DENSE_RANK() OVER (PARTITION BY YEAR_KEY ORDER BY avg_net_margin  DESC) AS r_net_margin,
    DENSE_RANK() OVER (PARTITION BY YEAR_KEY ORDER BY avg_asset_turn  DESC) AS r_asset_turn,
    DENSE_RANK() OVER (PARTITION BY YEAR_KEY ORDER BY avg_debt_equity DESC) AS r_debt_equity,
    DENSE_RANK() OVER (PARTITION BY YEAR_KEY ORDER BY AVG_EPS         DESC) AS r_eps,
    DENSE_RANK() OVER (PARTITION BY YEAR_KEY ORDER BY AVG_RD_PCT  DESC) AS r_rd_pct,
    DENSE_RANK() OVER (PARTITION BY YEAR_KEY ORDER BY AVG_CAPEX_PCT DESC) AS r_capex_pct,
    DENSE_RANK() OVER (PARTITION BY YEAR_KEY ORDER BY AVG_ANNUAL_RETURN DESC) AS r_annual_return,
    DENSE_RANK() OVER (PARTITION BY YEAR_KEY ORDER BY total_mktcap    DESC) AS r_avg_mktcap

  FROM fact_cte
),
normalized AS (
  SELECT
    sector_code,
    sector_name,
    YEAR_KEY,

    -- normalize each rank to [0..1] (best→1, worst→0)
    (cnt - r_roe       ) / NULLIF(cnt-1,0) AS n_roe,
    (cnt - r_roa       ) / NULLIF(cnt-1,0) AS n_roa,
    (cnt - r_op_margin) / NULLIF(cnt-1,0) AS n_op_margin,
    (cnt - r_net_margin) / NULLIF(cnt-1,0) AS n_net_margin,
    (cnt - r_asset_turn) / NULLIF(cnt-1,0) AS n_asset_turn,
    (cnt - r_debt_equity) / NULLIF(cnt-1,0) AS n_debt_equity,
    (cnt - r_eps       ) / NULLIF(cnt-1,0) AS n_eps,
    (cnt - r_rd_pct    ) / NULLIF(cnt-1,0) AS n_rd_pct,
    (cnt - r_capex_pct ) / NULLIF(cnt-1,0) AS n_capex_pct,
    (cnt - r_annual_return) / NULLIF(cnt-1,0) AS n_annual_return,
    (cnt - r_avg_mktcap) / NULLIF(cnt-1,0) AS n_avg_mktcap_crsp,

    -- composite = average of all 11 normalized metrics
    (
      (cnt - r_roe       ) / NULLIF(cnt-1,0) +
      (cnt - r_roa       ) / NULLIF(cnt-1,0) +
      (cnt - r_op_margin) / NULLIF(cnt-1,0) +
      (cnt - r_net_margin) / NULLIF(cnt-1,0) +
      (cnt - r_asset_turn) / NULLIF(cnt-1,0) +
      (cnt - r_debt_equity) / NULLIF(cnt-1,0) +
      (cnt - r_eps       ) / NULLIF(cnt-1,0) +
      (cnt - r_rd_pct    ) / NULLIF(cnt-1,0) +
      (cnt - r_capex_pct ) / NULLIF(cnt-1,0) +
      (cnt - r_annual_return) / NULLIF(cnt-1,0) +
      (cnt - r_avg_mktcap) / NULLIF(cnt-1,0)
    ) / 11 AS composite_score,

    -- assign sector rank by descending composite_score
    ROW_NUMBER() OVER (
      PARTITION BY YEAR_KEY
      ORDER BY
        (
          (cnt - r_roe       ) / NULLIF(cnt-1,0) +
          (cnt - r_roa       ) / NULLIF(cnt-1,0) +
          (cnt - r_op_margin) / NULLIF(cnt-1,0) +
          (cnt - r_net_margin) / NULLIF(cnt-1,0) +
          (cnt - r_asset_turn) / NULLIF(cnt-1,0) +
          (cnt - r_debt_equity) / NULLIF(cnt-1,0) +
          (cnt - r_eps       ) / NULLIF(cnt-1,0) +
          (cnt - r_rd_pct    ) / NULLIF(cnt-1,0) +
          (cnt - r_capex_pct ) / NULLIF(cnt-1,0) +
          (cnt - r_annual_return) / NULLIF(cnt-1,0) +
          (cnt - r_avg_mktcap) / NULLIF(cnt-1,0)
        ) DESC
    ) AS comp_rank

  FROM ranked
)
SELECT
    sector_code,
    sector_name,
    YEAR_KEY,
    composite_score,
    comp_rank
FROM normalized
WHERE comp_rank = 1;

##################################

CREATE MATERIALIZED VIEW APPLICATION_TBS.MV_TOP_COMPANY_COMPOSITE_RANKNORM
  BUILD IMMEDIATE
  REFRESH COMPLETE ON DEMAND
AS
WITH fact_cte AS (
  SELECT *
    FROM APPLICATION_TBS.FACT_COMPANY_ANNUAL_MV
),
ranked AS (
  SELECT
    gvkey,
    gsector,
    "YEAR",
    roe, roa, op_margin, net_margin, asset_turn,
    revenue_growth, netincome_growth, eps,
    debt_equity, interest_coverage, financial_leverage,
    market_cap_cstra, pe_ratio, pb_ratio,
    rd_pct, capex_pct, annual_return, avg_mktcap_crsp,
    COUNT(*) OVER (PARTITION BY gsector,"YEAR")             AS cnt,

    -- DENSE_RANK() for each KPI (1 = best)
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY roe         DESC) AS r_roe,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY roa         DESC) AS r_roa,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY op_margin   DESC) AS r_op_margin,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY net_margin  DESC) AS r_net_margin,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY asset_turn  DESC) AS r_asset_turn,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY revenue_growth    DESC) AS r_rev_growth,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY netincome_growth DESC) AS r_netinc_growth,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY eps         DESC) AS r_eps,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY debt_equity DESC) AS r_debt_eq,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY interest_coverage DESC) AS r_int_cov,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY financial_leverage DESC) AS r_fin_lev,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY market_cap_cstra DESC) AS r_mktcap_cstra,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY pe_ratio     ASC) AS r_pe,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY pb_ratio     ASC) AS r_pb,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY rd_pct       DESC) AS r_rd_pct,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY capex_pct    DESC) AS r_capex_pct,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY annual_return DESC) AS r_ann_ret,
    DENSE_RANK() OVER (PARTITION BY gsector,"YEAR" ORDER BY avg_mktcap_crsp DESC) AS r_avg_mktcap
  FROM fact_cte
),
normalized AS (
  SELECT
    gvkey,
    gsector,
    "YEAR",

    /* normalize each rank to [0..1]: best→1, worst→0 */
    (cnt - r_roe)        / NULLIF(cnt-1,0) AS n_roe,
    (cnt - r_roa)        / NULLIF(cnt-1,0) AS n_roa,
    (cnt - r_op_margin)  / NULLIF(cnt-1,0) AS n_op_margin,
    (cnt - r_net_margin) / NULLIF(cnt-1,0) AS n_net_margin,
    (cnt - r_asset_turn) / NULLIF(cnt-1,0) AS n_asset_turn,
    (cnt - r_rev_growth) / NULLIF(cnt-1,0) AS n_revenue_growth,
    (cnt - r_netinc_growth) / NULLIF(cnt-1,0) AS n_netincome_growth,
    (cnt - r_eps)        / NULLIF(cnt-1,0) AS n_eps,
    (cnt - r_debt_eq)    / NULLIF(cnt-1,0) AS n_debt_equity,
    (cnt - r_int_cov)    / NULLIF(cnt-1,0) AS n_interest_coverage,
    (cnt - r_fin_lev)    / NULLIF(cnt-1,0) AS n_financial_leverage,
    (cnt - r_mktcap_cstra)/NULLIF(cnt-1,0) AS n_market_cap_cstra,
    (cnt - r_pe)         / NULLIF(cnt-1,0) AS n_pe_ratio,
    (cnt - r_pb)         / NULLIF(cnt-1,0) AS n_pb_ratio,
    (cnt - r_rd_pct)     / NULLIF(cnt-1,0) AS n_rd_pct,
    (cnt - r_capex_pct)  / NULLIF(cnt-1,0) AS n_capex_pct,
    (cnt - r_ann_ret)    / NULLIF(cnt-1,0) AS n_annual_return,
    (cnt - r_avg_mktcap) / NULLIF(cnt-1,0) AS n_avg_mktcap_crsp,

    /* compute composite */
    (
      (cnt - r_roe)        / NULLIF(cnt-1,0)
    + (cnt - r_roa)        / NULLIF(cnt-1,0)
    + (cnt - r_op_margin)  / NULLIF(cnt-1,0)
    + (cnt - r_net_margin) / NULLIF(cnt-1,0)
    + (cnt - r_asset_turn) / NULLIF(cnt-1,0)
    + (cnt - r_rev_growth) / NULLIF(cnt-1,0)
    + (cnt - r_netinc_growth)/NULLIF(cnt-1,0)
    + (cnt - r_eps)        / NULLIF(cnt-1,0)
    + (cnt - r_debt_eq)    / NULLIF(cnt-1,0)
    + (cnt - r_int_cov)    / NULLIF(cnt-1,0)
    + (cnt - r_fin_lev)    / NULLIF(cnt-1,0)
    + (cnt - r_mktcap_cstra)/NULLIF(cnt-1,0)
    + (cnt - r_pe)         / NULLIF(cnt-1,0)
    + (cnt - r_pb)         / NULLIF(cnt-1,0)
    + (cnt - r_rd_pct)     / NULLIF(cnt-1,0)
    + (cnt - r_capex_pct)  / NULLIF(cnt-1,0)
    + (cnt - r_ann_ret)    / NULLIF(cnt-1,0)
    + (cnt - r_avg_mktcap) / NULLIF(cnt-1,0)
    ) / 18 AS composite_score,

    /* preserve the rank position */
    ROW_NUMBER() OVER (
      PARTITION BY gsector,"YEAR"
      ORDER BY
        (cnt - r_roe)        / NULLIF(cnt-1,0)
      + (cnt - r_roa)        / NULLIF(cnt-1,0)
      + (cnt - r_op_margin)  / NULLIF(cnt-1,0)
      + (cnt - r_net_margin) / NULLIF(cnt-1,0)
      + (cnt - r_asset_turn) / NULLIF(cnt-1,0)
      + (cnt - r_rev_growth) / NULLIF(cnt-1,0)
      + (cnt - r_netinc_growth)/NULLIF(cnt-1,0)
      + (cnt - r_eps)        / NULLIF(cnt-1,0)
      + (cnt - r_debt_eq)    / NULLIF(cnt-1,0)
      + (cnt - r_int_cov)    / NULLIF(cnt-1,0)
      + (cnt - r_fin_lev)    / NULLIF(cnt-1,0)
      + (cnt - r_mktcap_cstra)/NULLIF(cnt-1,0)
      + (cnt - r_pe)         / NULLIF(cnt-1,0)
      + (cnt - r_pb)         / NULLIF(cnt-1,0)
      + (cnt - r_rd_pct)     / NULLIF(cnt-1,0)
      + (cnt - r_capex_pct)  / NULLIF(cnt-1,0)
      + (cnt - r_ann_ret)    / NULLIF(cnt-1,0)
      + (cnt - r_avg_mktcap) / NULLIF(cnt-1,0)
      DESC
    ) AS comp_rank

  FROM ranked
)
SELECT
    gvkey,
    gsector,
    "YEAR",
    composite_score,
    comp_rank
FROM normalized
WHERE comp_rank = 1;
