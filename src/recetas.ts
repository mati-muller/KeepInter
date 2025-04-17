import sql from 'mssql';
import { dbConfig1, dbConfig2 } from './config';

export async function keepAliveRecetas() {
    let connection1, connection2;

    try {
        // Conexión a SQL Server
        connection1 = await sql.connect(dbConfig1);

        // Ejecutar consulta principal
        const result = await connection1.request().query(`
            SELECT DISTINCT
                t1.CodProd,
                CASE 
                    WHEN t1.CodMat = 'PLA21001730004C20 ' THEN 'PLA21001730004C20'
                    WHEN t1.CodMat = 'PL189021450007D94 ' THEN 'PLA189021450007D94'
                    WHEN t1.CodMat = 'PLA204021450007D94' THEN 'PLA220021450007D94'
                    WHEN t1.CodMat = 'PLA09500765000309B' THEN 'PLA09500780000309B'
                    WHEN t1.CodMat = 'PLA11000765000309B' THEN 'PLA11000780000309B'
                    WHEN t1.CodMat = 'PLA11000795000309B' THEN 'PLA11000780000309B'
                    WHEN t1.CodMat = 'PLA12000765000309B' THEN 'PLA11000780000309B'
                    WHEN t1.CodMat = 'PLA09500765000210E' THEN 'PLA09500780000210E'
                    WHEN t1.CodMat = 'PLA11000700000210E' THEN 'PLA11000780000210E'
                    WHEN t1.CodMat = 'PLA210021600004C20' THEN 'PLA210021450004C20'
                    WHEN t1.CodMat = 'PLA270015700000C20' THEN 'PLA270015700004C20'
                    WHEN t1.CodMat = 'PLA280021600004C20' THEN 'PLA280021450004C20'
                    WHEN t1.CodMat = 'PLA220021600007D92' THEN 'PLA220021450007D92'
                    WHEN t1.CodMat = 'PLA235021450007D94' THEN 'PLA189021450007D94'
                    WHEN t1.CodMat = 'PLA186021450007D98' THEN 'PLA186011650007D98'
                    WHEN t1.CodMat = 'PLA233021450007D98' THEN 'PLA233011650007D98'
                    WHEN t1.CodMat = 'PLA24001720004C20' THEN 'PLA250017300004C20'
                    ELSE t1.CodMat
                END AS CodMat,
                t2.DesProd,
                t1.CantMat
            FROM 
                softland.dwrecmat t1
            INNER JOIN 
                softland.iw_tprod t2
            ON 
                t1.CodMat = t2.CodProd
            WHERE 
                t2.DesProd LIKE '%PLACA%'
                AND t2.DesProd NOT LIKE '%ADHESIVO%';
        `);

        const rows = result.recordset;

        // Conexión a SQL Server (segunda base de datos)
        connection2 = await sql.connect(dbConfig2);

        // Crear la tabla REPORTES.recetas si no existe
        await connection2.request().query(`
            IF OBJECT_ID('REPORTES.dbo.recetas', 'U') IS NULL
            BEGIN
                CREATE TABLE REPORTES.dbo.recetas (
                    CodProd NVARCHAR(255),
                    CodMat NVARCHAR(MAX) DEFAULT NULL,
                    DesProd NVARCHAR(MAX) DEFAULT NULL,
                    CantMat DECIMAL(10,3) DEFAULT NULL
                );
            END
        `);

        // Obtener datos existentes de la tabla recetas
        const existingRecords = await connection2.request().query(`
            SELECT CodProd, CodMat, DesProd, CantMat FROM REPORTES.dbo.recetas
        `);

        const existingData = new Map(
            existingRecords.recordset.map(record => [record.CodProd, record])
        );

        // Usar transacción para operaciones masivas
        const transaction = new sql.Transaction(connection2);
        await transaction.begin();

        for (const row of rows) {
            const existingRecord = existingData.get(row.CodProd);

            // Crear un nuevo objeto request para cada operación
            const request = transaction.request();

            if (existingRecord) {
                // Actualizar solo si hay cambios
                if (
                    existingRecord.CodMat !== row.CodMat ||
                    existingRecord.DesProd !== row.DesProd ||
                    existingRecord.CantMat !== row.CantMat
                ) {
                    await request.input('CodProd', sql.NVarChar, row.CodProd)
                        .input('CodMat', sql.NVarChar, row.CodMat)
                        .input('DesProd', sql.NVarChar, row.DesProd)
                        .input('CantMat', sql.Decimal(10, 3), row.CantMat)
                        .query(`
                            UPDATE REPORTES.dbo.recetas
                            SET CodMat = @CodMat, DesProd = @DesProd, CantMat = @CantMat
                            WHERE CodProd = @CodProd
                        `);
                }
            } else {
                // Insertar nuevo registro
                await request.input('CodProd', sql.NVarChar, row.CodProd)
                    .input('CodMat', sql.NVarChar, row.CodMat)
                    .input('DesProd', sql.NVarChar, row.DesProd)
                    .input('CantMat', sql.Decimal(10, 3), row.CantMat)
                    .query(`
                        INSERT INTO REPORTES.dbo.recetas (CodProd, CodMat, DesProd, CantMat)
                        VALUES (@CodProd, @CodMat, @DesProd, @CantMat)
                    `);
            }
        }

        await transaction.commit();
        console.log('Registros procesados correctamente.');

        // Eliminar duplicados en la tabla REPORTES.dbo.recetas
        await connection2.request().query(`
            WITH CTE AS (
                SELECT 
                    CodProd, 
                    CodMat, 
                    DesProd, 
                    CantMat,
                    ROW_NUMBER() OVER (
                        PARTITION BY CodProd, CodMat, DesProd, CantMat 
                        ORDER BY (SELECT NULL)
                    ) AS RowNum
                FROM REPORTES.dbo.recetas
            )
            DELETE FROM CTE WHERE RowNum > 1;
        `);

        console.log('Duplicados eliminados correctamente.');

    } catch (err) {
        console.error('Error en proceso:', err);
    } finally {
        if (connection1?.connected) await connection1.close();
        if (connection2?.connected) await connection2.close();
    }

    setTimeout(keepAliveRecetas, 60000);
}

