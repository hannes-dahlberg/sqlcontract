IF OBJECT_ID('tempdb..#%DDL_PROCEDURE_NAME%') IS NOT NULL
BEGIN
    EXEC #%DDL_PROCEDURE_NAME% '%OBJECT_NAME%'
    RETURN
END

SELECT 'NO' AS [exists]