import sql from 'mssql';
import fs from 'fs';
import path from 'path';
import { dbConfig1, dbConfig2 } from './config';

export async function generateNVReport() {
    let connection1, connection2;

    try {
        console.log('\n=== GENERANDO REPORTE DE NV ===');

        // 1. Obtener NV pendientes desde la fuente original (solo lectura)
        connection1 = await sql.connect(dbConfig1);
        const pendingResult = await connection1.request().query(`
            SELECT DISTINCT det.NVNumero
            FROM PANELSA2017.softland.nw_detnv det
            LEFT JOIN PANELSA2017.softland.nw_nventa ven 
                ON det.NVNumero = ven.NVNumero
            LEFT JOIN (
                SELECT gs.nvnumero, gm.CodProd, SUM(gm.cantfacturada) AS cant
                FROM PANELSA2017.softland.iw_gsaen gs
                LEFT JOIN PANELSA2017.softland.iw_gmovi gm ON gs.NroInt = gm.NroInt
                WHERE gs.Tipo IN ('F','N')
                GROUP BY gs.nvnumero, gm.CodProd
            ) fact 
                ON fact.nvnumero = det.NVNumero AND fact.CodProd = det.CodProd
            WHERE ven.nvEstado = 'A'
              AND det.NVNumero >= 13215
              AND det.NVNumero NOT IN (13388,13344,13433,13427)
              AND (det.nvCant - ISNULL(fact.cant, 0)) > 0
        `);

        const pendingNVSet = new Set(
            pendingResult.recordset.map(r => Number(r.NVNumero))
        );
        console.log(`NV pendientes en fuente: ${pendingNVSet.size}`);        // 2. Obtener todos los registros de procesos (solo lectura)
        connection2 = await sql.connect(dbConfig2);        const procesosResult = await connection2.request().query(`
            SELECT 
                p.NVNUMERO,
                p.CODPROD,
                p.DETPROD,
                p.PROCESO
            FROM REPORTES.dbo.procesos p
            ORDER BY p.NVNUMERO, p.CODPROD
        `);// 3. Construir filas del CSV
        const csvRows: string[] = [
            'NOTA_VENTA,PRODUCTO,DESCRIPCION,PROCESO,ESTADO'
        ];

        for (const record of procesosResult.recordset) {
            const nvNum = Number(record.NVNUMERO);
            const isPending = pendingNVSet.has(nvNum);
            const estado = isPending ? 'PENDIENTE' : 'LISTA';

            // Escapar campos que puedan tener comas
            const desc = `"${String(record.DETPROD || '').replace(/"/g, '""')}"`;

            csvRows.push(
                `${record.NVNUMERO},${record.CODPROD},${desc},${record.PROCESO},${estado}`
            );
        }

        // 4. Guardar CSV en carpeta descargas/
        const downloadsDir = path.join(process.cwd(), 'descargas');
        if (!fs.existsSync(downloadsDir)) {
            fs.mkdirSync(downloadsDir, { recursive: true });
        }

        const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
        const filename = `ReporteNV_${timestamp}.csv`;
        const filepath = path.join(downloadsDir, filename);

        fs.writeFileSync(filepath, csvRows.join('\n'), 'utf8');

        console.log(`✓ CSV generado en: ${filepath}`);
        console.log(`  Total de registros: ${csvRows.length - 1}`);
        console.log(`  NV pendientes: ${[...new Set(procesosResult.recordset.filter(r => pendingNVSet.has(Number(r.NVNUMERO))).map(r => r.NVNUMERO))].length}`);
        console.log(`  NV listas:     ${[...new Set(procesosResult.recordset.filter(r => !pendingNVSet.has(Number(r.NVNUMERO))).map(r => r.NVNUMERO))].length}`);

    } catch (err) {
        console.error('Error generando reporte:', err);
    } finally {
        if (connection1?.connected) await connection1.close();
        if (connection2?.connected) await connection2.close();
    }
}

// Ejecutar inmediatamente al llamar el script directamente
generateNVReport();
