import dotenv from 'dotenv';

dotenv.config();

const SQL_SERVER = process.env.SQL_SERVER || 'default_server';
const SQL_USER = process.env.SQL_USER || 'default_user';
const SQL_PASSWORD = process.env.SQL_PASSWORD || 'default_password';
const SQL_DATABASE = process.env.SQL_DATABASE || 'default_database';
const SQL_DATABASE2 = process.env.SQL_DATABASE2 || 'default_database2';
export const dbConfig1 = {
    user: SQL_USER,
    password: SQL_PASSWORD,
    server: SQL_SERVER,
    database: SQL_DATABASE,
    options: {
        encrypt: false,
        enableArithAbort: false
    }
};

export const dbConfig2 = {
    user: SQL_USER,
    password: SQL_PASSWORD,
    server: SQL_SERVER,
    database: SQL_DATABASE2,
    options: {
        encrypt: false,
        enableArithAbort: false
    }
};
