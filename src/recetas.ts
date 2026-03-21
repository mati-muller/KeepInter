import sql from 'mssql';
import { dbConfig1, dbConfig2 } from './config';

let isRunning = false;

export async function keepAliveRecetas() {
    if (isRunning) return;
    isRunning = true;
    let connection1, connection2;

    try {
        // Conexión a SQL Server
        connection1 = await sql.connect(dbConfig1);        // Ejecutar consulta principal (sin DISTINCT para obtener todas las placas)
        const result = await connection1.request().query(`
            SELECT
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
        connection2 = await sql.connect(dbConfig2);        // Crear la tabla REPORTES.recetas si no existe
        await connection2.request().query(`
            IF OBJECT_ID('REPORTES.dbo.recetas', 'U') IS NULL
            BEGIN
                CREATE TABLE REPORTES.dbo.recetas (
                    CodProd NVARCHAR(255) NOT NULL,
                    CodMat NVARCHAR(255) NOT NULL,
                    DesProd NVARCHAR(MAX) DEFAULT NULL,
                    CantMat DECIMAL(10,3) DEFAULT NULL,
                    PRIMARY KEY (CodProd, CodMat)
                );
            END
        `);        // Obtener datos existentes de la tabla recetas
        const existingRecords = await connection2.request().query(`
            SELECT CodProd, CodMat, DesProd, CantMat FROM REPORTES.dbo.recetas
        `);

        // Crear un Map con clave compuesta (CodProd:CodMat) para evitar duplicados
        const existingData = new Map(
            existingRecords.recordset.map(record => [
                `${record.CodProd}:${record.CodMat}`,
                record
            ])
        );        // Eliminar duplicados en los datos nuevos usando clave compuesta
        const uniqueRows = new Map();
        for (const row of rows) {
            const key = `${row.CodProd}:${row.CodMat}`;
            uniqueRows.set(key, row);
        }

        // Crear un mapa de CodProd -> array de CodMat nuevos
        const newMatsByCodProd = new Map();
        for (const row of rows) {
            if (!newMatsByCodProd.has(row.CodProd)) {
                newMatsByCodProd.set(row.CodProd, []);
            }
            newMatsByCodProd.get(row.CodProd).push(row.CodMat);
        }

        // Usar transacción para operaciones masivas
        const transaction = new sql.Transaction(connection2);
        await transaction.begin();

        let insertCount = 0;
        let updateCount = 0;
        let deleteCount = 0;

        // Primero: Eliminar placas antiguas que ya no existen para cada CodProd
        for (const [existingKey, existingRecord] of existingData) {
            const [codProd, codMat] = existingKey.split(':');
            const newMatsForThisProd = newMatsByCodProd.get(codProd) || [];
            
            // Si este CodMat no está en los datos nuevos para este CodProd, eliminarlo
            if (!newMatsForThisProd.includes(codMat)) {
                console.log(`DELETE: CodProd=${codProd}, CodMat=${codMat} (placa antigua eliminada)`);
                const deleteRequest = transaction.request();
                await deleteRequest.input('CodProd', sql.NVarChar, codProd)
                    .input('CodMat', sql.NVarChar, codMat)
                    .query(`
                        DELETE FROM REPORTES.dbo.recetas
                        WHERE CodProd = @CodProd AND CodMat = @CodMat
                    `);
                deleteCount++;
            }
        }

        // Segundo: Insertar o actualizar registros nuevos
        for (const [key, row] of uniqueRows) {
            const existingRecord = existingData.get(key);

            // Crear un nuevo objeto request para cada operación
            const request = transaction.request();
            if (existingRecord) {
                // Actualizar solo si hay cambios en DesProd o CantMat
                const desProdChanged = String(existingRecord.DesProd).trim() !== String(row.DesProd).trim();
                const cantMatChanged = parseFloat(existingRecord.CantMat) !== parseFloat(row.CantMat);
                
                if (desProdChanged || cantMatChanged) {
                    console.log(`UPDATE: CodProd=${row.CodProd}, CodMat=${row.CodMat}, DesProd=${row.DesProd}, CantMat=${row.CantMat} (DesProdChanged=${desProdChanged}, CantMatChanged=${cantMatChanged})`);
                    await request.input('CodProd', sql.NVarChar, row.CodProd)
                        .input('CodMat', sql.NVarChar, row.CodMat)
                        .input('DesProd', sql.NVarChar, row.DesProd)
                        .input('CantMat', sql.Decimal(10, 3), row.CantMat)
                        .query(`
                            UPDATE REPORTES.dbo.recetas
                            SET DesProd = @DesProd, CantMat = @CantMat
                            WHERE CodProd = @CodProd AND CodMat = @CodMat
                        `);
                    updateCount++;
                }
            } else {
                // Insertar nuevo registro
                console.log(`INSERT: CodProd=${row.CodProd}, CodMat=${row.CodMat}, DesProd=${row.DesProd}, CantMat=${row.CantMat}`);
                await request.input('CodProd', sql.NVarChar, row.CodProd)
                    .input('CodMat', sql.NVarChar, row.CodMat)
                    .input('DesProd', sql.NVarChar, row.DesProd)
                    .input('CantMat', sql.Decimal(10, 3), row.CantMat)
                    .query(`
                        INSERT INTO REPORTES.dbo.recetas (CodProd, CodMat, DesProd, CantMat)
                        VALUES (@CodProd, @CodMat, @DesProd, @CantMat)
                    `);
                insertCount++;
            }
        }        await transaction.commit();
        console.log(`Registros procesados: ${insertCount} insertados, ${updateCount} actualizados, ${deleteCount} eliminados.`);

    } catch (err) {
        console.error('Error en proceso:', err);
    } finally {
        if (connection1?.connected) await connection1.close();
        if (connection2?.connected) await connection2.close();
        isRunning = false;
    }
}

// Ejecutar cada 10 minutos para reducir carga
setInterval(keepAliveRecetas, 10 * 60 * 1000);

