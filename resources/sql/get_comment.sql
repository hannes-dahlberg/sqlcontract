SELECT
  value
FROM fn_listextendedproperty(default, default, default, default, default, default, default)
WHERE
  objtype IS NULL
  AND objname IS NULL
  AND name = '%COMMENT_NAME%'