import postgres from 'postgres';
import { Log } from 'nesoi/lib/engine/util/log';
import { $Bucket, $Space } from 'nesoi/lib/elements';
import { MigrationRunner } from '../runner/runner';
import { AnyDaemon, Daemon } from 'nesoi/lib/engine/daemon';
import { PostgresBucketAdapter } from '../../postgres.bucket_adapter';
import { colored } from 'nesoi/lib/engine/util/string';
import { MigrationRunnerStatus } from '../runner/status';
import { $Migration } from './migration';
import { MigrationGenerator } from './generator';
import { PostgresService } from '../../postgres.service';
import { MigrationRoutine } from '..';

export type MigratorConfig = {
    dirpath?: string
    postgres?: postgres.Options<any>
}

export class MigrationProvider<
    S extends $Space
> {
    public status!: MigrationRunnerStatus;
    
    private constructor(
        protected daemon: AnyDaemon,
        private service: PostgresService<string>,
        public dirpath = './migrations'
    ) {}

    static async create(
        daemon: AnyDaemon,
        service: PostgresService<string>
    ) {
        const provider = new MigrationProvider(daemon, service);

        const oldTable = await provider.service.sql`
            SELECT * FROM pg_catalog.pg_tables WHERE tablename = ${ MigrationRunner.MIGRATION_TABLE_NAME };
        `;

        const baseMigrations: [string, MigrationRoutine][] = [
            ['migrations:v1', (await import('../../migrations/__nesoi_migrations_v1')).default(service.name)]
        ];
        if (!oldTable.length) {
            for (const baseMigration of baseMigrations) {
                await MigrationRunner.internal.up(daemon, service, '__nesoi_postgres', baseMigration[0], baseMigration[1]);
            }
        }

        provider.status = await MigrationRunner.status(daemon, provider.service, provider.dirpath);

        // FUTURE: When migrations:v2, v3, etc is available, here we should
        // check for the ones which didn't run

        return provider;
    }   

    async generate() {
        const modules = Daemon.getModules(this.daemon);

        const migrations: $Migration[] = [];

        for (const module of modules) {
            const buckets = Daemon.getModule(this.daemon, module.name).buckets;

            for (const bucket in buckets) {
                const schema: $Bucket = buckets[bucket].schema;

                // Avoid external buckets
                if (schema.module !== module.name) continue;
                
                // Avoid non-postgres buckets
                const adapter = buckets[bucket].adapter;
                if (!(adapter instanceof PostgresBucketAdapter)) continue;
                
                // Avoid buckets from other postgres services
                // (Migrator is run from the CLI for a specific PostgresService)
                if (adapter.service != this.service) continue;

                const migration = await this.generateForBucket(module.name, bucket, adapter.tableName, true);
                if (migration) {
                    migrations.push(migration);
                }
            }
        }

        return migrations;
    }

    public async generateForBucket<
        ModuleName extends keyof S['modules']
    >(
        module: ModuleName,
        bucket: keyof S['modules'][ModuleName]['buckets'],
        tableName: string,
        interactive = false
    ) {
        const generator = MigrationGenerator.fromModule(this.daemon, this.service, module as string, bucket as string, tableName);
        const migration = await generator.generate(interactive);
        const tag = colored(`${module as string}::bucket:${bucket as string}`, 'lightcyan');
        if (!migration) {
            Log.info('migrator' as any, 'bucket', `No migrations for ${tag}.`);
            return undefined;
        }
        
        const hash = migration.hash();
        const alreadyExists = this.status.items.find(item => item.hash === hash);
        if (alreadyExists && alreadyExists.state === 'pending') {
            Log.warn('migrator' as any, 'bucket', `A similar migration for ${tag} was found pending, ignoring this one.`);
            return undefined;
        }

        return migration;
    }

}