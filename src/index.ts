import express, { Request, Response } from 'express';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import { keepAlive } from './keepAlive';
import { keepAlive2 } from './keepAlive2';
import { keepAliveRecetas } from './recetas';
import cors from 'cors';
const app = express();
const PORT = 3001;

app.use(helmet());
app.use(cors())
const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 1000 // limit each IP to 100 requests per windowMs
});

app.use(limiter);

async function executeKeepAlives() {
    await keepAlive();
    await keepAlive2();
    await keepAliveRecetas();
}

executeKeepAlives();

app.get('/', (req: Request, res: Response) => {
    res.send('Vamos CTM');
});

app.listen(PORT, () => {
    console.log(`Servidor corriendo en http://localhost:${PORT}`);
});
