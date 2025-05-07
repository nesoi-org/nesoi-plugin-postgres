import postgres from 'postgres';
import * as fs from 'fs';
import { Log } from 'nesoi/lib/engine/util/log';

export class CSV {
    
    /**
     * Import a csv table into the database
     */
    static import(sql: postgres.Sql<any>, tableName: string, csvpath: string) {
        
        Log.info('csv' as any, 'import', `Importing objects from csv file '${csvpath}' into table '${tableName}'`);

        const lines = fs.readFileSync(csvpath).toString().split('\n');
        const keys = lines[0]
            .split(',')
            .map(v => v.match(/"(.*)"/)?.[1] || v);

        const objs = lines.slice(1).map(line => {
            const rows = line
                .split(',')
                .map(v => v.match(/"(.*)"/)?.[1] || v);
            const obj: Record<string, any> = {};
            for (let i = 0; i < keys.length; i++) {
                obj[keys[i]] = rows[i];
            }
            return obj;
        });

        return sql.begin(async sql => {
            for (const obj of objs) {
                Log.info('csv' as any, 'import', `Inserting object ${obj.id}`, obj);
    
                await sql`
                    INSERT INTO ${sql(tableName)}
                    ${ sql(obj, keys) }
                `;
            }
        }).catch((e: unknown) => {
            Log.error('csv' as any, 'import', 'CSV Import failed, rolling back changes', e as any);
        });

    }

}