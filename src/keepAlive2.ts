import sql from 'mssql';
import { dbConfig2 } from './config';

let isRunning = false;

export async function keepAlive2() {
    if (isRunning) return;
    isRunning = true;
    let connection;
    let transactionActive = false;

    try {
        // Conexión a SQL Server (segunda base de datos)
        connection = await sql.connect(dbConfig2);

        // Iniciar transacción
        const transaction = new sql.Transaction(connection);
        await transaction.begin();
        transactionActive = true;        try {
            // Primero eliminar registros de HISTORIAL que estén relacionados
            const historialDeleteResult = await transaction.request().query(`
                DELETE FROM REPORTES.dbo.HISTORIAL
                WHERE ID_PROCESO IN (
                    SELECT p.ID
                    FROM REPORTES.dbo.procesos p
                    LEFT JOIN REPORTES.dbo.procesos2 p2 ON p.ID = p2.ID
                    WHERE (p2.ESTADO_PROC = 'LISTO' OR p2.ID IS NULL)
                    AND CONVERT(DATE, p.FECHA_ENTREGA, 120) < CONVERT(DATE, DATEADD(MONTH, -6, GETDATE()), 120)
                );
            `);
            console.log(`Registros de HISTORIAL eliminados: ${historialDeleteResult.rowsAffected[0]}`);

            // Eliminar registros de procesos2
            const procesos2DeleteResult = await transaction.request().query(`
                DELETE FROM REPORTES.dbo.procesos2
                WHERE ID IN (
                    SELECT p2.ID
                    FROM REPORTES.dbo.procesos2 p2
                    JOIN REPORTES.dbo.procesos p ON p2.ID = p.ID
                    WHERE p2.ESTADO_PROC = 'LISTO'
                    AND CONVERT(DATE, p.FECHA_ENTREGA, 120) < CONVERT(DATE, DATEADD(MONTH, -6, GETDATE()), 120)
                );
            `);
            console.log(`Registros de procesos2 eliminados: ${procesos2DeleteResult.rowsAffected[0]}`);

            // Eliminar registros de procesos
            const procesosDeleteResult = await transaction.request().query(`
                DELETE FROM REPORTES.dbo.procesos
                WHERE ID NOT IN (SELECT ID FROM REPORTES.dbo.procesos2)
                AND CONVERT(DATE, FECHA_ENTREGA, 120) < CONVERT(DATE, DATEADD(MONTH, -6, GETDATE()), 120);
            `);
            console.log(`Registros de procesos eliminados: ${procesosDeleteResult.rowsAffected[0]}`);

            await transaction.commit();
            transactionActive = false;
            console.log('Registros eliminados correctamente');

        } catch (deleteError) {
            if (transactionActive) {
                await transaction.rollback();
                transactionActive = false;
            }
            console.error('Error en eliminación:', deleteError);
        }

    } catch (err) {
        console.error('Error en proceso:', err);

    } finally {
        if (connection?.connected) await connection.close();
        isRunning = false;
    }
}

// Ejecutar cada 24 horas como antes
setInterval(keepAlive2, 1000 * 60 * 60 * 24);
