IF EXISTS(SELECT 1 FROM fn_listextendedproperty(default, default, default, default, default, default, default) WHERE objtype IS NULL AND objname IS NULL AND name = '%COMMENT_NAME%')
BEGIN
  EXEC sp_updateextendedproperty '%COMMENT_NAME%', '%COMMENT_VALUE%'
END
ELSE
BEGIN
  EXEC sp_addextendedproperty '%COMMENT_NAME%', '%COMMENT_VALUE%'
END