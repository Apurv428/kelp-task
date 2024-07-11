const express = require('express');
const fs = require('fs');
const { Pool } = require('pg');
const dotenv = require('dotenv');
const path = require('path');

dotenv.config();

const app = express();
const port = 3000;

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
});

app.get('/', async (req, res) => {
    const csvFilePath = path.resolve(process.env.CSV_FILE_PATH);
    try {
        const data = await readCSV(csvFilePath);
        await insertData(data);
        const ageDistribution = await calculateAgeDistribution();

        let htmlResponse = '<html><head><title>Age Distribution</title></head><body>';
        htmlResponse += '<h1>Age Distribution</h1>';
        htmlResponse += '<table border="1"><tr><th>Age-Group</th><th>Distribution</th></tr>';

        for (const range in ageDistribution) {
            htmlResponse += <tr><td>${range}</td><td>${ageDistribution[range]}</td></tr>;
        }

        htmlResponse += '</table>';
        htmlResponse += '</body></html>';

        res.send(htmlResponse);
    } catch (error) {
        console.error('Error:', error);
        if (error.code === 'ENOENT') {
            res.status(404).send(<html><head><title>File Not Found</title></head><body><h1>File Not Found</h1><p>${error.message}</p></body></html>);
        } else {
            res.status(500).send(<html><head><title>Error</title></head><body><h1>Error</h1><p>An error occurred: ${error.message}</p></body></html>);
        }
    }
});

const readCSV = (filePath) => {
    return new Promise((resolve, reject) => {
        fs.readFile(filePath, 'utf8', (err, data) => {
            if (err) {
                return reject(err);
            }
            const lines = data.split('\n');
            const headers = lines[0].split(',').map(header => header.trim());
            const jsonData = lines.slice(1).map(line => {
                const values = line.split(',').map(value => value.trim());
                let row = {};
                headers.forEach((header, index) => {
                    row[header] = values[index];
                });
                return row;
            });
            resolve(jsonData);
        });
    });
};

const insertData = async (data) => {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        for (const row of data) {
            const { 'name.firstName': firstName, 'name.lastName': lastName, age, ...additionalInfo } = row;
            const name = `${firstName} ${lastName}`;
            const address = {};

            Object.entries(additionalInfo).forEach(([key, value]) => {
                if (key.startsWith('address.')) {
                    const keys = key.split('.');
                    let current = address;

                    for (let i = 0; i < keys.length; i++) {
                        const k = keys[i];

                        if (i === keys.length - 1) {
                            current[k] = value;
                        } else {
                            current[k] = current[k] || {};
                            current = current[k];
                        }
                    }
                }
            });

            const filteredAdditionalInfo = {};
            for (const key in additionalInfo) {
                if (!key.startsWith('address.')) {
                    filteredAdditionalInfo[key] = additionalInfo[key];
                }
            }

            await client.query(
                'INSERT INTO public.users (name, age, address, additional_info) VALUES ($1, $2, $3, $4)',
                [name, age, address, filteredAdditionalInfo]
            );
        }
        await client.query('COMMIT');
    } catch (error) {
        await client.query('ROLLBACK');
        throw error;
    } finally {
        client.release();
    }
};

const calculateAgeDistribution = async () => {
    const client = await pool.connect();
    try {
        const res = await client.query('SELECT age, COUNT(*) FROM public.users GROUP BY age');

        let distribution = {
            '< 20': 0,
            '20 to 40': 0,
            '40 to 60': 0,
            '> 60': 0
        };

        res.rows.forEach(row => {
            const age = parseInt(row.age);
            const count = parseInt(row.count);

            if (age < 20) {
                distribution['< 20'] += count;
            } else if (age >= 20 && age <= 40) {
                distribution['20 to 40'] += count;
            } else if (age > 40 && age <= 60) {
                distribution['40 to 60'] += count;
            } else if (age > 60) {
                distribution['> 60'] += count;
            }
        });

        console.log('Age-Group\tDistribution');
        console.log('< 20\t\t', distribution['< 20']);
        console.log('20 to 40\t', distribution['20 to 40']);
        console.log('40 to 60\t', distribution['40 to 60']);
        console.log('> 60\t\t', distribution['> 60']);

        return distribution;
    } finally {
        client.release();
    }
};

app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});