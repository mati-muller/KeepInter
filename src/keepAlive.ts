import sql from 'mssql';
import { dbConfig1, dbConfig2 } from './config';

let isRunning = false;

export async function keepAlive() {
    if (isRunning) return;
    isRunning = true;
    let connection1, connection2;
    let transactionActive = false;
    
    try {
        // Conexión a SQL Server
        connection1 = await sql.connect(dbConfig1);
        
        // Ejecutar consulta principal
        const result = await connection1.request().query(`
            SELECT
                det.NVNumero,
                ven.nvEstado,
                ven.nvFem AS fecha_nv,
                ven.nvFeEnt AS fecha_entrega,
                ven.ConcAuto,
                det.CodProd,
                det.nvCant AS cant_vendida,
                fact.cant AS cant_facturada,
                (det.nvCant - ISNULL(fact.cant, 0)) AS dif_fact,
                det.nvPrecio,
                det.DetProd,
                auxi.NomAux,

                -- Procesos reales sin agrupar
                P.CodProc,
                CASE 
                    WHEN P.DescProc LIKE '%CALA%' THEN 'CALADO'
                    WHEN P.DescProc LIKE '%EMPLA%' THEN 'EMPLACADO'
                    WHEN P.DescProc LIKE '%ENCOL%' THEN 'ENCOLADO'
                    WHEN P.DescProc LIKE '%IMPR%' THEN 'IMPRESION'
                    WHEN P.DescProc LIKE '%MULTI%' THEN 'MULTIPLE'
                    WHEN P.DescProc LIKE '%PEGA%' THEN 'PEGADO'
                    WHEN P.DescProc LIKE '%PLIZ%' THEN 'PLIZADO'
                    WHEN P.DescProc LIKE '%EMBAL%' THEN 'OTRO'
                    WHEN P.DescProc LIKE '%TROQUE%' THEN 'TROQUELADO'
                    WHEN P.DescProc LIKE '%TROZ%' THEN 'TROZADO'
                    ELSE 'OTRO'
                END AS PROCESO,
                P.DescProc,
                P.tiempo

            FROM PANELSA2017.softland.nw_detnv det
            LEFT JOIN PANELSA2017.softland.nw_nventa ven 
                ON det.NVNumero = ven.NVNumero
            LEFT JOIN PANELSA2017.softland.cwtauxi auxi 
                ON ven.CodAux = auxi.CodAux

            -- Procesos (SIN GROUP BY, una fila por proceso)
            LEFT JOIN (
                SELECT 
                    prod.CodProd, 
                    prod.CodProc,
                    procesos.DescProc, 
                    procesos.TpoEjecPro AS tiempo
                FROM PANELSA2017.softland.dworproprod prod
                LEFT JOIN PANELSA2017.softland.dwprocesos procesos 
                    ON procesos.CodProc = prod.CodProc
            ) P 
                ON det.CodProd = P.CodProd

            -- Facturación por producto
            LEFT JOIN (
                SELECT 
                    gs.nvnumero, 
                    gm.CodProd, 
                    SUM(gm.cantfacturada) AS cant
                FROM PANELSA2017.softland.iw_gsaen gs
                LEFT JOIN PANELSA2017.softland.iw_gmovi gm 
                    ON gs.NroInt = gm.NroInt
                WHERE gs.Tipo IN ('F','N')  -- Factura o Nota de Crédito/Débito
                GROUP BY gs.nvnumero, gm.CodProd
            ) fact 
                ON fact.nvnumero = det.NVNumero
                AND fact.CodProd = det.CodProd

            WHERE ven.nvEstado = 'A'
              AND det.NVNumero >= 13215
              AND det.NVNumero NOT IN (13388,13344,13433,13427)
              AND (det.nvCant - ISNULL(fact.cant, 0)) > 0  -- SOLO productos pendientes

            ORDER BY det.NVNumero, det.CodProd, P.CodProc;
        `);
        
        const rows = result.recordset;
        
        // Conexión a SQL Server (segunda base de datos)
        connection2 = await sql.connect(dbConfig2);
        
        // Crear tablas si no existen
        await connection2.request().query(`
            IF NOT EXISTS (SELECT * FROM REPORTES.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'procesos')
            CREATE TABLE REPORTES.dbo.procesos (
                ID INT PRIMARY KEY IDENTITY(1,1),
                NVNUMERO INT NOT NULL,
                NVESTADO NVARCHAR(MAX) DEFAULT NULL,
                FECHA_NV NVARCHAR(MAX) DEFAULT NULL,
                FECHA_ENTREGA NVARCHAR(MAX) DEFAULT NULL,
                CONCAUTO NVARCHAR(MAX) DEFAULT NULL,
                CODPROD NVARCHAR(MAX) DEFAULT NULL,
                NVCANT INT DEFAULT NULL,
                CANT_FACT INT DEFAULT NULL,
                DIF_FACT INT DEFAULT NULL,
                NVPRECIO DECIMAL(10,2) DEFAULT NULL,
                DETPROD NVARCHAR(MAX) DEFAULT NULL,
                NOMAUX NVARCHAR(MAX) DEFAULT NULL,
                CODPROC NVARCHAR(MAX) DEFAULT NULL,
                PROCESO NVARCHAR(MAX) DEFAULT NULL,
                DESCPROC NVARCHAR(MAX) DEFAULT NULL,
                TIEMPO INT DEFAULT NULL
            );
            
            IF NOT EXISTS (SELECT * FROM REPORTES.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'procesos2')
            CREATE TABLE REPORTES.dbo.procesos2 (
                ID INT PRIMARY KEY,
                ESTADO_PROC NVARCHAR(MAX) DEFAULT 'PENDIENTE',
                CANT_PROD INT DEFAULT NULL,
                CANT_A_PROD INT DEFAULT NULL,
                PLACA NVARCHAR(MAX) DEFAULT NULL,
                PLACAS_USADAS INT DEFAULT NULL,
                TIEMPO_TOTAL DECIMAL(10,2) DEFAULT NULL,
                OBSERVACIONES NVARCHAR(MAX) DEFAULT NULL,
                [USER] NVARCHAR(MAX) DEFAULT NULL,
                FOREIGN KEY (ID) REFERENCES REPORTES.dbo.procesos(ID)
            );
        `);        // Iniciar transacción
        const transaction = new sql.Transaction(connection2);
        await transaction.begin();
        transactionActive = true;
          try {
            // 1. Obtener llaves únicas existentes (NVNUMERO + PROCESO)
            const existingKeys = new Set();
            const existingRecords = await transaction.request().query(`
                SELECT NVNUMERO, COALESCE(PROCESO, '') AS PROCESO
                FROM REPORTES.dbo.procesos;
            `);
            
            existingRecords.recordset.forEach((record) => {
                existingKeys.add(`${record.NVNUMERO}-${record.PROCESO}`);
            });

            // 2. Preparar statement de inserción
            const insertColumns = [
                'NVNUMERO', 'NVESTADO', 'FECHA_NV', 'FECHA_ENTREGA',
                'CONCAUTO', 'CODPROD', 'NVCANT', 'CANT_FACT', 'DIF_FACT',
                'NVPRECIO', 'DETPROD', 'NOMAUX', 'CODPROC', 'PROCESO',
                'DESCPROC', 'TIEMPO'
            ];
              const insertStatement = `
                INSERT INTO REPORTES.dbo.procesos (${insertColumns.join(', ')})
                VALUES (${insertColumns.map((col) => `@${col}`).join(', ')})
            `;            // 3. Procesar registros with conversion of types
            let insertedCount = 0;
            let skippedCount = 0;
            let skippedByEmbalado = 0;
            let skippedByDuplicate = 0;
            let skippedByNullCodProd = 0;
            let totalProcessed = 0;
            
            console.log(`\n=== INICIANDO PROCESAMIENTO ===`);
            console.log(`Total de registros obtenidos de la query: ${rows.length}`);
            console.log(`Total de claves existentes en BD: ${existingKeys.size}`);
            
            // Agrupar por NV para mostrar estadísticas
            const nvGroups = new Map();
            rows.forEach(row => {
                const nvNumber = Number(row.NVNumero) || 0;
                if (!nvGroups.has(nvNumber)) {
                    nvGroups.set(nvNumber, { productos: new Set(), procesos: 0 });
                }
                nvGroups.get(nvNumber).productos.add(String(row.CodProd || ''));
                nvGroups.get(nvNumber).procesos++;
            });
            
            console.log(`\nNotas de venta a procesar: ${nvGroups.size}`);

            console.log(`\n--- PROCESANDO REGISTROS ---`); 

            for (const row of rows) {
                totalProcessed++;                // Conversión de tipos y manejo de valores
                const nvNumber = Number(row.NVNumero) || 0;
                const codProd = String(row.CodProd || '').trim();
                const codProc = String(row.CodProc || '').trim();
                const proceso = String(row.PROCESO || '').trim();
                const uniqueKey = `${nvNumber}-${proceso}`;



                if (proceso.toUpperCase() === 'EMBALADO') {
                    console.log('  → Omitido por proceso EMBALADO');
                    skippedByEmbalado++;
                    skippedCount++;
                    continue;
                }

                if (existingKeys.has(uniqueKey)) {
                    skippedByDuplicate++;
                    skippedCount++;
                    continue;
                }// Convertir todos los valores a tipos compatibles con MSSQL
                const params = {
                    NVNUMERO: nvNumber,
                    NVESTADO: String(row.nvEstado || ''),
                    FECHA_NV: new Date(row.fecha_nv).toISOString().split('T')[0] || '1970-01-01',
                    FECHA_ENTREGA: new Date(row.fecha_entrega).toISOString().split('T')[0] || '1970-01-01',
                    CONCAUTO: String(row.ConcAuto || ''),
                    CODPROD: String(row.CodProd || ''),
                    NVCANT: Number(row.cant_vendida) || 0,
                    CANT_FACT: Number(row.cant_fact) || 0,
                    DIF_FACT: Number(row.dif_fact) || 0,
                    NVPRECIO: Number(row.nvPrecio) || 0,
                    DETPROD: String(row.DetProd || ''),
                    NOMAUX: String(row.NomAux || ''),
                    CODPROC: codProc,
                    PROCESO: String(row.PROCESO || ''),
                    DESCPROC: String(row.DescProc || '').trim() || null,
                    TIEMPO: Number(row.tiempo) || 0
                };

                // Mostrar registro que se va a insertar
                console.log('  → Insertando registro con clave:', uniqueKey);

                try {
                    await transaction.request()
                        .input('NVNUMERO', sql.Int, params.NVNUMERO)
                        .input('NVESTADO', sql.NVarChar, params.NVESTADO)
                        .input('FECHA_NV', sql.NVarChar, params.FECHA_NV)
                        .input('FECHA_ENTREGA', sql.NVarChar, params.FECHA_ENTREGA)
                        .input('CONCAUTO', sql.NVarChar, params.CONCAUTO)
                        .input('CODPROD', sql.NVarChar, params.CODPROD)
                        .input('NVCANT', sql.Int, params.NVCANT)
                        .input('CANT_FACT', sql.Int, params.CANT_FACT)
                        .input('DIF_FACT', sql.Int, params.DIF_FACT)
                        .input('NVPRECIO', sql.Decimal, params.NVPRECIO)
                        .input('DETPROD', sql.NVarChar, params.DETPROD)                        .input('NOMAUX', sql.NVarChar, params.NOMAUX)
                        .input('CODPROC', sql.NVarChar, params.CODPROC)
                        .input('PROCESO', sql.NVarChar, params.PROCESO)
                        .input('DESCPROC', sql.NVarChar(sql.MAX), params.DESCPROC)
                        .input('TIEMPO', sql.Int, params.TIEMPO)
                        .query(insertStatement);                    existingKeys.add(uniqueKey);
                    insertedCount++;
                    console.log('  ✓ Registro insertado exitosamente');
                } catch (insertError: any) {
                    console.error(`  ✗ Error insertando registro ${uniqueKey}:`, insertError.message);
                    skippedCount++;
                    continue;
                }
            }

            await transaction.commit();
            transactionActive = false;            console.log(`\n=== RESUMEN DE PROCESAMIENTO ===`);
            console.log(`Total registros procesados: ${totalProcessed}`);
            console.log(`Registros insertados: ${insertedCount}`);
            console.log(`Registros omitidos: ${skippedCount}`);
            console.log(`  - Por CODPROD nulo/0/vacío: ${skippedByNullCodProd}`);
            console.log(`  - Por proceso EMBALADO: ${skippedByEmbalado}`);
            console.log(`  - Por clave duplicada: ${skippedByDuplicate}`);
            console.log(`Porcentaje de inserción: ${((insertedCount / totalProcessed) * 100).toFixed(2)}%`);

            // Mostrar estadísticas adicionales
            console.log(`\nRegistros insertados exitosamente: ${insertedCount}`);

            // Insert into procesos2
            const procesosRows = await connection2.request().query(`
                SELECT ID, NVCANT FROM REPORTES.dbo.procesos
            `);

            const insertProcesos2Statement = `
                MERGE REPORTES.dbo.procesos2 AS target
                USING (SELECT @ID AS ID, @CANT_PROD AS CANT_PROD, @CANT_A_PROD AS CANT_A_PROD) AS source
                ON (target.ID = source.ID)
                WHEN MATCHED AND (target.CANT_A_PROD IS NULL OR target.CANT_PROD IS NULL) THEN 
                    UPDATE SET CANT_A_PROD = source.CANT_A_PROD, CANT_PROD = source.CANT_PROD
                WHEN NOT MATCHED THEN
                    INSERT (ID, CANT_PROD, CANT_A_PROD) VALUES (source.ID, source.CANT_PROD, source.CANT_A_PROD);
            `;

            for (const row of procesosRows.recordset) {
                await connection2.request()
                    .input('ID', sql.Int, row.ID)
                    .input('CANT_PROD', sql.Int, 0)
                    .input('CANT_A_PROD', sql.Int, row.NVCANT)
                    .query(insertProcesos2Statement);
            }

        } catch (insertError) {
            if (transactionActive) {
                await transaction.rollback();
                transactionActive = false;
            }
            console.error('Error en inserción:', insertError);
        }

    } catch (err) {
        console.error('Error en proceso:', err);
        
    } finally {
        if (connection1?.connected) await connection1.close();
        if (connection2?.connected) await connection2.close();
        isRunning = false;
    }
}

// Ejecutar cada 5 minutos para reducir carga
setInterval(keepAlive, 2* 5 * 60 * 1000);
