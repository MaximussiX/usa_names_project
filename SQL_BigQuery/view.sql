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
