import * as fs from 'fs';
import * as path from 'path';
import postgres from 'postgres';
import { colored } from 'nesoi/lib/engine/util/string';
import { Log } from 'nesoi/lib/engine/util/log';
import { NesoiDatetime } from 'nesoi/lib/engine/data/datetime';
import { AnyDaemon, Daemon } from 'nesoi/lib/engine/daemon';
import { Trx } from 'nesoi/lib/engine/transaction/trx';
import { MigrationRunnerStatus } from './status';
import { MigrationFile, MigrationRoutine, MigrationRow } from '..';
import UI from 'nesoi/lib/engine/cli/ui';
import { $Migration } from '../generator/migration';
import { PostgresBucketAdapter } from '../../postgres.bucket_adapter';
import { PostgresService } from '../../postgres.service';
import { TrxEngine } from 'nesoi/lib/engine/transaction/trx_engine';
import { Module } from 'nesoi/lib/engine/module';

export class MigrationRunner {

    public static MIGRATION_TABLE_NAME = '__nesoi_migrations';
    
    // Scan (to generate status)

    private static async scanFiles(daemon: AnyDaemon, service: PostgresService, migrations_dir: string) {

        const modules = Daemon.getModules(daemon);
        
        // Read migration files of each module
        const files: Omit<MigrationFile, 'routine'>[] = [];
        for (const module of modules) {
            const modulepath = path.join('modules', ...module.subdir, module.name, migrations_dir);
            
            if (!fs.existsSync(modulepath)) continue;
            fs.readdirSync(modulepath, { withFileTypes: true })
                .forEach(node => {
                    const nodePath = path.resolve(modulepath, node.name);
                    if (nodePath.endsWith('.d.ts')) {
                        return;
                    }
                    files.push({
                        service: service.name,
                        module: module.name,
                        name: node.name.replace(/\.[j|t]s$/, ''),
                        path: nodePath
                    });
                });
        }

        // Extract migration routine of each file

        const migrationFiles: MigrationFile[] = [];
        const sortedFiles = files.sort((a,b) => a.name.localeCompare(b.name));
        for (const file of sortedFiles) {
            const { default: routine } = await import(file.path);
            if (routine instanceof MigrationRoutine) {
                const name = file.name.replace(/'.[t|j]s'/,'');
                migrationFiles.push({
                    ...file,
                    name,
                    routine
                });
            }
            else {
                Log.warn('migrator' as any, 'scan', `File at ${file.path} doesn't appear to be a migration. Skipping it.`);
            }
        }
        return migrationFiles;
    }
    
    private static async scanRows(
        sql: postgres.Sql<any>
    ) {
        const db = await sql<MigrationRow[]>`
            SELECT * FROM ${sql(MigrationRunner.MIGRATION_TABLE_NAME)}
            ORDER BY id
        `;
        return db;
    }

    public static async status(
        daemon: AnyDaemon,
        service: PostgresService,
        migrations_dir: string
    ) {
        const migrationFiles = await MigrationRunner.scanFiles(daemon, service, migrations_dir);
        const migrationRows = await MigrationRunner.scanRows(service.sql);
        return new MigrationRunnerStatus(daemon, migrationFiles, migrationRows);
    }

    // Public Up / Down

    public static async up(
        daemon: AnyDaemon,
        service: PostgresService,
        mode: 'one' | 'batch' = 'one',
        dirpath: string = 'migrations',
        interactive = false
    ) {
        let status = await MigrationRunner.status(daemon, service, dirpath);
        Log.info('migration_runner' as any, 'up', status.describe());

        await this.migrateTrash(daemon, service, status);

        const pending = status.items.filter(item => item.state === 'pending');
        if (!pending.length) {
            Log.info('migrator' as any, 'up', 'No migrations to run.');
            return;
        }

        if (interactive) {
            const n = (mode === 'one' ? 1 : pending.length).toString();
            const confirm = await UI.yesOrNo(`Run ${colored(n, 'green')} migration(s) ${colored('▲ UP', 'lightgreen')}?`);
            if (!confirm) {
                return;
            }
        }
        
        await service.sql.begin(async sql => {
            if (mode === 'one') {
                const migration = pending[0];
                await this.migrateUp(daemon, service.name, sql, migration, status.batch + 1);
            }
            else {
                for (const migration of pending) {
                    await this.migrateUp(daemon, service.name, sql, migration, status.batch + 1);
                }
            }
        });

        status = await MigrationRunner.status(daemon, service, dirpath);
        Log.info('migration_runner' as any, 'up', status.describe());
    }

    public static async down(
        daemon: AnyDaemon,
        service: PostgresService,
        mode: 'one' | 'batch' = 'one',
        dirpath: string = 'migrations'
    ) {
        let status = await MigrationRunner.status(daemon, service, dirpath);
        Log.info('migration_runner' as any, 'up', status.describe());

        const lastBatch = status.items.filter(item => item.batch === status.batch);
        if (!lastBatch.length) {
            Log.info('migrator' as any, 'down', 'No migrations to rollback.');
            return;
        }

        const n = mode === 'one' ? 'one' : 'last batch of';
        const confirm = await UI.yesOrNo(`Rollback ${colored(n, 'green')} migration(s) ${colored('▼ DOWN', 'red')}?`);
        if (!confirm) {
            return;
        }
        
        await service.sql.begin(async sql => {
            if (mode === 'one') {
                const migration = lastBatch.at(-1)!;
                await this.migrateDown(daemon, service.name, sql, migration);
            }
            else {
                const revLastBatch = [...lastBatch].reverse();
                for (const migration of revLastBatch) {
                    await this.migrateDown(daemon, service.name, sql, migration);
                }
            }
        });

        status = await MigrationRunner.status(daemon, service, dirpath);
        Log.info('migration_runner' as any, 'up', status.describe());
    }

    public static fromSchema = {

        up: async (
            daemon: AnyDaemon,
            service: PostgresService,
            migration: $Migration,
            dirpath: string = 'migrations'
        ) => {
            let status = await MigrationRunner.status(daemon, service, dirpath);
            Log.info('migration_runner' as any, 'up', status.describe());
    
            const routine = new MigrationRoutine({
                service: service.name,
                description: migration.description,
                up: async($: { sql: postgres.Sql<any> }) => {
                    for (const sql of migration.sqlUp()) {
                        await $.sql.unsafe(sql);
                    }
                },
                down: async($: { sql: postgres.Sql<any> }) => {
                    for (const sql of migration.sqlDown()) {
                        await $.sql.unsafe(sql);
                    }
                }
            });
    
            const mig: MigrationRunnerStatus['items'][number] = {
                // eslint-disable-next-line @typescript-eslint/no-misused-spread
                ...migration,
                module: migration.module.name,
                state: 'pending',
                hash: routine.hash,
                routine
            };
            await service.sql.begin(async sql => {
                await this.migrateUp(daemon, service.name, sql, mig, status.batch + 1);
            });
    
            status = await MigrationRunner.status(daemon, service, dirpath);
            Log.info('migration_runner' as any, 'up', status.describe());
        }

    };

    public static internal = {

        up: async (
            daemon: AnyDaemon,
            service: PostgresService,
            module: string,
            name: string,
            routine: MigrationRoutine,
            dirpath: string = 'migrations'
        ) => {
            let status: MigrationRunnerStatus|undefined = undefined;

            if (name !== 'migrations:v1') {
                status = await MigrationRunner.status(daemon, service, dirpath);
                Log.info('migration_runner' as any, 'up', status.describe());
            }
    
            const mig: MigrationRunnerStatus['items'][number] = {
                service: service.name,
                module: module,
                name,
                state: 'pending',
                hash: routine.hash,
                routine
            };
            await service.sql.begin(async sql => {
                await this.migrateUp(daemon, service.name, sql, mig, -1);
            });
            
            status = await MigrationRunner.status(daemon, service, dirpath);
            Log.info('migration_runner' as any, 'up', status.describe());
        }

    };

    // Trash

    private static async migrateTrash(
        daemon: AnyDaemon,
        service: PostgresService,
        status: MigrationRunnerStatus
    ) {
        const modules = Daemon.getModules(daemon);
        const trashTables = new Set<string>();

        for (const module of modules) {

            if (module.trash) {
                // Avoid non-postgres trash buckets
                const adapter = module.trash.adapter;
                if (!(adapter instanceof PostgresBucketAdapter)) continue;
                
                // Avoid trash buckets from other postgres services
                // (Migrator is run from the CLI for a specific PostgresService)
                if (adapter.service != service) continue;

                trashTables.add(adapter.tableName);
            }
        }

        for (const tableName of trashTables) {
            const done = status.items.filter(item =>
                item.module === '__nesoi_postgres'
                && item.name.startsWith(`trash:${tableName}:`)
            ).map(item => item.name);
            const routines: [string, MigrationRoutine][] = [
                [`trash:${tableName}:v1`, (await import('../../migrations/__nesoi_trash_v1')).default(service.name, tableName)]
            ];
            for (const routine of routines) {
                if (done.includes(routine[0])) continue;
                await MigrationRunner.internal.up(daemon, service, '__nesoi_postgres', routine[0], routine[1]);
            }
        }
    }

    // Implementation Up/Down

    private static async migrateUp(
        daemon: AnyDaemon,
        serviceName: string,
        sql: postgres.Sql,
        migration: MigrationRunnerStatus['items'][number],
        batch: number
    ) {
        Log.info('migrator' as any, 'up', `Running migration ${colored('▲ UP', 'lightgreen')} ${colored(migration.name, 'lightblue')}`);
        
        if (migration.module === '__nesoi_postgres') {
            const module = new Module('__nesoi_postgres', { builders: [] });
            const trxEngines = (daemon as any).trxEngines as AnyDaemon['trxEngines'];
            trxEngines['__nesoi_postgres'] = new TrxEngine('plugin:postgres', module, {}, {}, {});
        }
        
        const status = await daemon.trx(migration.module)

            // We don't want the trx to call sql.begin, given it was called
            // by the migrator itself.
            .idempotent
            
            .run(async trx => {
                Trx.set(trx, serviceName+'.sql', sql);
                await migration.routine!.up({
                    sql,
                    trx
                });
            });
        if (status.state !== 'ok') {
            throw new Error('Migration failed. Rolling back all batch changes.');
        }
        const row = {
            service: migration.service,
            module: migration.module,
            name: migration.name,
            description: migration.routine!.description,
            batch,
            timestamp: NesoiDatetime.now(),
            hash: migration.hash || null
        } as Omit<MigrationRow, 'id'|'timestamp'>;
        if (migration.description) {
            row.description = migration.description;
        }
        if (!row.description) delete row['description'];
        await sql`
            INSERT INTO ${sql(MigrationRunner.MIGRATION_TABLE_NAME)}
            ${ sql(row) }
        `;
    }

    private static async migrateDown(
        daemon: AnyDaemon,
        serviceName: string,
        sql: postgres.Sql<any>,
        migration: MigrationRunnerStatus['items'][number]
    ) {
        const name = colored(migration.name, 'lightblue');
        Log.info('migrator' as any, 'up', `Running migration ${colored('▼ DOWN', 'yellow')} ${name}`);

        if (migration.state === 'lost') {
            const del = await UI.yesOrNo(`The migration ${name} is ${colored('lost', 'red')}, skip and delete it? ${colored('Warning: this might cause inconsistencies', 'red')}.`);
            if (!del) {
                throw new Error(`Migration ${migration.name} was lost, unable to migrate down.`);
            }
        }
        else {
            const status = await daemon.trx(migration.module)
                .run(async trx => {
                    Trx.set(trx, serviceName+'.sql', sql);
                    await migration.routine!.down({
                        sql,
                        trx
                    });
                });
            if (status.state !== 'ok') {
                throw new Error('Migration failed. Rolling back all batch changes.');
            }
        }

        await sql`
            DELETE FROM ${sql(MigrationRunner.MIGRATION_TABLE_NAME)}
            WHERE id = ${ migration.id! }
        `;
    }

}