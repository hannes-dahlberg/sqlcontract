IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = '%TYPE%' AND CONCAT('[',ROUTINE_SCHEMA , '].[', ROUTINE_NAME, ']') = '%ROUTINE%')
BEGIN
  EXEC sp_executesql N'DROP %TYPE% %ROUTINE%'
END

