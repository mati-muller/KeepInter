-- =====================================================
-- ELIMINAR DUPLICADOS DE NVNUMERO Y PROCESO
-- =====================================================

-- 1. Primero eliminar duplicados de HISTORIAL (tiene FK a procesos)
DELETE FROM REPORTES.dbo.HISTORIAL
WHERE ID_PROCESO IN (
    SELECT p.ID
    FROM REPORTES.dbo.procesos p
    WHERE p.ID IN (
        SELECT ID
        FROM (
            SELECT 
                ID,
                ROW_NUMBER() OVER (PARTITION BY NVNUMERO, PROCESO ORDER BY ID) as rn
            FROM REPORTES.dbo.procesos
        ) ranked
        WHERE rn > 1
    )
);

-- 2. Eliminar duplicados de procesos2 (tiene FK a procesos)
DELETE FROM REPORTES.dbo.procesos2
WHERE ID IN (
    SELECT ID
    FROM (
        SELECT 
            ID,
            ROW_NUMBER() OVER (PARTITION BY NVNUMERO, PROCESO ORDER BY ID) as rn
        FROM REPORTES.dbo.procesos
    ) ranked
    WHERE rn > 1
);

-- 3. Finalmente eliminar duplicados de procesos (mantener el primero de cada grupo)
DELETE FROM REPORTES.dbo.procesos
WHERE ID IN (
    SELECT ID
    FROM (
        SELECT 
            ID,
            ROW_NUMBER() OVER (PARTITION BY NVNUMERO, PROCESO ORDER BY ID) as rn
        FROM REPORTES.dbo.procesos
    ) ranked
    WHERE rn > 1
);

-- =====================================================
-- VERIFICAR RESULTADOS
-- =====================================================

-- Ver registros duplicados que se eliminarían (antes de ejecutar delete)
SELECT 
    NVNUMERO,
    PROCESO,
    COUNT(*) as cantidad_duplicados,
    STUFF((
        SELECT ', ' + CAST(ID AS VARCHAR)
        FROM REPORTES.dbo.procesos p2
        WHERE p2.NVNUMERO = p.NVNUMERO AND p2.PROCESO = p.PROCESO
        FOR XML PATH('')
    ), 1, 2, '') as IDs
FROM REPORTES.dbo.procesos p
GROUP BY NVNUMERO, PROCESO
HAVING COUNT(*) > 1
ORDER BY NVNUMERO, PROCESO;

-- Ver el estado final (después de eliminar)
SELECT 
    NVNUMERO,
    PROCESO,
    COUNT(*) as cantidad
FROM REPORTES.dbo.procesos
GROUP BY NVNUMERO, PROCESO
ORDER BY NVNUMERO, PROCESO;
