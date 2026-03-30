WITH usage_data AS (
  SELECT
    u.user_name,
    u.full_name,
    u.department,
    u.office,
    pl.job_type,
    pl.total_color_pages,
    (pl.total_pages - pl.total_color_pages) AS bw_pages,
    pl.usage_day::date AS usage_date
  FROM tbl_user AS u
  JOIN tbl_printer_usage_log AS pl
    ON pl.used_by_user_id = u.user_id
  WHERE u.deleted   = 'N'
    AND pl.printed   = 'Y'
    AND pl.cancelled = 'N'
--     AND pl.usage_day::date > DATE '2023-09-30'
--     AND pl.usage_day::date <> CURRENT_DATE
)
SELECT
  user_name,
  full_name,
  department,
  office,
  job_type,
  SUM(total_color_pages)                 AS total_color_pages,
  SUM(bw_pages)                          AS total_grayscale_pages,
  SUM(total_color_pages + bw_pages)      AS total_pages,
  SUM(total_color_pages) * 4.82          AS total_cost_color_pages,
  SUM(bw_pages)          * 0.48          AS total_cost_grayscale_pages,
  SUM(total_color_pages) * 4.82
    + SUM(bw_pages)      * 0.48          AS total_cost_pages,
  EXTRACT(YEAR  FROM usage_date)         AS usage_calendar_year,
  EXTRACT(MONTH FROM usage_date)         AS usage_calendar_month,
  CASE
    WHEN EXTRACT(MONTH FROM usage_date) >= 10
      THEN EXTRACT(YEAR FROM usage_date) + 1
    ELSE EXTRACT(YEAR FROM usage_date)
  END AS usage_budget_year,
  CASE
    WHEN EXTRACT(MONTH FROM usage_date) >= 10
      THEN EXTRACT(MONTH FROM usage_date) - 9
    ELSE EXTRACT(MONTH FROM usage_date) + 3
  END AS usage_budget_month_order
FROM usage_data
GROUP BY
  user_name, full_name, department, office, job_type,
  usage_calendar_year, usage_calendar_month,
  usage_budget_year, usage_budget_month_order
ORDER BY
  usage_calendar_year,
  usage_calendar_month,
  user_name