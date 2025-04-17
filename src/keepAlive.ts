import sql from 'mssql';
import { dbConfig1, dbConfig2 } from './config';

export async function keepAlive() {
    let connection1, connection2;
    let transactionActive = false;
    
    try {
        // Conexión a SQL Server
        connection1 = await sql.connect(dbConfig1);
        
        // Ejecutar consulta principal
        const result = await connection1.request().query(`
            Select * from (
                -- Notas de venta
                Select det.NVNumero, ven.nvEstado, ven.nvFem fecha_nv, ven.nvFeEnt fecha_entrega, ven.ConcAuto , det.CodProd, 
                det.nvCant, cant cant_fact, 
                case when cant is null then det.nvcant else det.nvcant-cant end dif_fact, 
                det.nvPrecio, det.DetProd, auxi.NomAux,
                P.codProc,
                CASE 
                WHEN P.DescProc LIKE '%CALA%' THEN 'CALADO'
                WHEN P.DescProc LIKE '%EMPLA%' THEN 'EMPLACADO'
                WHEN P.DescProc LIKE '%ENCOL%' THEN 'ENCOLADO'
                WHEN P.DescProc LIKE '%IMPR%' THEN 'IMPRESION'
                WHEN P.DescProc LIKE '%MULTI%' THEN 'MULTIPLE'
                WHEN P.DescProc LIKE '%PEGA%' THEN 'PEGADO'
                WHEN P.DescProc LIKE '%PLIZ%' THEN 'PLIZADO'
                WHEN P.DescProc LIKE '%TROQUE%' THEN 'TROQUELADO'
                WHEN P.DescProc LIKE '%TROZ%' THEN 'TROZADO'
                ELSE 'OTRO' END PROCESO,
                P.DescProc, P.tiempo,P.CantProd
                from PANELSA2017.softland.nw_detnv det
                left join PANELSA2017.softland.nw_nventa ven on det.NVNumero=ven.NVNumero 
                left join PANELSA2017.softland.cwtauxi auxi on ven.CodAux=auxi.CodAux 
                left JOIN 
                (
                -- prod con proceso
                Select prod.CodProd, prod.CodProc, DescProc, tiempo, CantProd from PANELSA2017.softland.dworproprod as prod
                Left join (
                -- Procesos
                Select CodProc, DescProc, TpoEjecPro tiempo, CantProd  from PANELSA2017.softland.dwprocesos
                ) as procesos on procesos.codproc=prod.codproc
                group by CodProd, prod.CodProc, DescProc, tiempo, CantProd
                ) P on det.codprod=P.codprod
                left join (
                -- facturas y notas de venta
                SELECT gs.nvnumero, gm.CodProd, sum(cantfacturada) cant
                FROM [PANELSA2017].[softland].[iw_gsaen] gs
                left join [PANELSA2017].[softland].[iw_gmovi] gm on gs.NroInt=gm.NroInt 
                where gs.Tipo in ('F','N')
                group by gs.nvnumero, gm.codprod) fact on fact.nvnumero=det.NVNumero and fact.codprod=det.codprod
                where nvEstado='A'
                and det.NVNumero >=13215
                and det.NVNumero not in (13388,13344,13433,13427) --cancelada
            ) as subc
            where dif_fact>0
            ORDER BY NVNUMERO ASC
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
                TIEMPO INT DEFAULT NULL,
                CANTPROD INT DEFAULT NULL
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
        `);

        // Iniciar transacción
        const transaction = new sql.Transaction(connection2);
        await transaction.begin();
        transactionActive = true;
        
        try {
            // 1. Obtener llaves únicas existentes
            const existingKeys = new Set();
            const existingRecords = await transaction.request().query(`
                SELECT NVNUMERO, COALESCE(CODPROC, '') AS CODPROC 
                FROM REPORTES.dbo.procesos;
            `);
            
            existingRecords.recordset.forEach((record) => {
                existingKeys.add(`${record.NVNUMERO}-${record.CODPROC}`);
            });

            // 2. Preparar statement de inserción
            const insertColumns = [
                'NVNUMERO', 'NVESTADO', 'FECHA_NV', 'FECHA_ENTREGA',
                'CONCAUTO', 'CODPROD', 'NVCANT', 'CANT_FACT', 'DIF_FACT',
                'NVPRECIO', 'DETPROD', 'NOMAUX', 'CODPROC', 'PROCESO',
                'DESCPROC', 'TIEMPO', 'CANTPROD'
            ];
            
            const insertStatement = `
                INSERT INTO REPORTES.dbo.procesos (${insertColumns.join(', ')})
                VALUES (${insertColumns.map((col) => `@${col}`).join(', ')})
            `;

            // 3. Procesar registros con conversión de tipos
            let insertedCount = 0;
            for (const row of rows) {
                // Conversión de tipos y manejo de valores
                const nvNumber = Number(row.NVNumero) || 0;
                const codProc = String(row.codProc || '').trim();
                const uniqueKey = `${nvNumber}-${codProc}`;

                if (existingKeys.has(uniqueKey)) continue;

                // Verificar si NVNUMERO está en la tabla nv_hechas
                const nvHechasCheck = await transaction.request().query(`
                    SELECT 1 FROM REPORTES.dbo.nv_hechas WHERE NVENTA = ${nvNumber}
                `);
                if (nvHechasCheck.recordset.length > 0) continue;

                // Convertir todos los valores a tipos compatibles con MSSQL
                const params = {
                    NVNUMERO: nvNumber,
                    NVESTADO: String(row.nvEstado || ''),
                    FECHA_NV: new Date(row.fecha_nv).toISOString().split('T')[0] || '1970-01-01',
                    FECHA_ENTREGA: new Date(row.fecha_entrega).toISOString().split('T')[0] || '1970-01-01',
                    CONCAUTO: String(row.ConcAuto || ''),
                    CODPROD: String(row.CodProd || ''),
                    NVCANT: Number(row.nvCant) || 0,
                    CANT_FACT: Number(row.cant_fact) || 0,
                    DIF_FACT: Number(row.dif_fact) || 0,
                    NVPRECIO: Number(row.nvPrecio) || 0,
                    DETPROD: String(row.DetProd || ''),
                    NOMAUX: String(row.NomAux || ''),
                    CODPROC: codProc,
                    PROCESO: String(row.PROCESO || ''),
                    DESCPROC: String(row.DescProc || ''),
                    TIEMPO: Number(row.tiempo) || 0,
                    CANTPROD: Number(row.CantProd) || 0
                };

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
                    .input('DETPROD', sql.NVarChar, params.DETPROD)
                    .input('NOMAUX', sql.NVarChar, params.NOMAUX)
                    .input('CODPROC', sql.NVarChar, params.CODPROC)
                    .input('PROCESO', sql.NVarChar, params.PROCESO)
                    .input('DESCPROC', sql.NVarChar, params.DESCPROC)
                    .input('TIEMPO', sql.Int, params.TIEMPO)
                    .input('CANTPROD', sql.Int, params.CANTPROD)
                    .query(insertStatement);

                existingKeys.add(uniqueKey);
                insertedCount++;
            }

            await transaction.commit();
            transactionActive = false;
            console.log(`Insertados ${insertedCount} registros nuevos`);

            // Insert into procesos2
            const procesosRows = await connection2.request().query(`
                SELECT ID, CANTPROD FROM REPORTES.dbo.procesos
            `);

            const insertProcesos2Statement = `
                MERGE REPORTES.dbo.procesos2 AS target
                USING (SELECT @ID AS ID, @CANT_A_PROD AS CANT_A_PROD) AS source
                ON (target.ID = source.ID)
                WHEN MATCHED AND target.CANT_A_PROD IS NULL THEN 
                    UPDATE SET CANT_A_PROD = source.CANT_A_PROD
                WHEN NOT MATCHED THEN
                    INSERT (ID, CANT_A_PROD) VALUES (source.ID, source.CANT_A_PROD);
            `;

            for (const row of procesosRows.recordset) {
                await connection2.request()
                    .input('ID', sql.Int, row.ID)
                    .input('CANT_A_PROD', sql.Int, row.CANTPROD)
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
    }

    setTimeout(keepAlive, 60000 ); 
}
