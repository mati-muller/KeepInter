import sql from 'mssql';
import { dbConfig2 } from './config';

export async function keepAlive2() {
    let connection;
    let transactionActive = false;

    try {
        // Conexión a SQL Server (segunda base de datos)
        connection = await sql.connect(dbConfig2);

        // Iniciar transacción
        const transaction = new sql.Transaction(connection);
        await transaction.begin();
        transactionActive = true;

        try {
            // Eliminar registros de procesos2
            await transaction.request().query(`
                DELETE FROM REPORTES.dbo.procesos2
                WHERE ID IN (
                    SELECT p2.ID
                    FROM REPORTES.dbo.procesos2 p2
                    JOIN REPORTES.dbo.procesos p ON p2.ID = p.ID
                    WHERE p2.ESTADO_PROC = 'LISTO'
                    AND CONVERT(DATE, p.FECHA_ENTREGA, 120) < CONVERT(DATE, DATEADD(MONTH, -6, GETDATE()), 120)
                );
            `);

            // Eliminar registros de procesos
            await transaction.request().query(`
                DELETE FROM REPORTES.dbo.procesos
                WHERE ID NOT IN (SELECT ID FROM REPORTES.dbo.procesos2)
                AND CONVERT(DATE, FECHA_ENTREGA, 120) < CONVERT(DATE, DATEADD(MONTH, -6, GETDATE()), 120);
            `);

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
    }

    setTimeout(keepAlive2, 1000 * 60 * 60 * 24); // Cada 24 horas
}
