# This file contain all my views 

#  view the sum of male names grouped by name
CREATE OR REPLACE VIEW `PROJECT_NAME.view_usanames.males_names`(name, number) AS (
  SELECT
    name,
    SUM(number) as nb
  FROM
    `PROJECT_NAME.dwh_usnames.usanames_raw`
  WHERE
    gender = 'M'
  GROUP BY
    name
  ORDER BY
    nb DESC
);

#  view the sum of female names grouped by name
CREATE OR REPLACE VIEW `PROJECT_NAME.view_usanames.males_names`(name, number) AS (
  SELECT
    name,
    SUM(number) as nb
  FROM
    `PROJECT_NAME.dwh_usnames.usanames_raw`
  WHERE
    gender = 'F'
  GROUP BY
    name
  ORDER BY
    nb DESC
);


# Top 3  female by year query :
SELECT
  finish_rank,
  year,
  gender,
  name,
  sum_by_year
FROM (
  SELECT
    gender,
    year,
    name,
    SUM(number) AS sum_by_year,
    RANK() OVER (PARTITION BY year ORDER BY SUM(number) DESC) AS finish_rank
  FROM
    `PROJECT_NAME.dwh_usnames.usanames_raw`
  WHERE
    gender = 'F'
  GROUP BY
    gender,
    year,
    name
  ORDER BY
    year,
    sum_by_year DESC)
GROUP BY
  finish_rank,
  gender,
  year,
  name,
  sum_by_year
HAVING
  finish_rank<4
ORDER BY
  year,
  sum_by_year DESC
