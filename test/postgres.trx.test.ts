import { BucketBuilder } from 'nesoi/lib/elements/entities/bucket/bucket.builder';
import { Log } from 'nesoi/lib/engine/util/log';
import { InlineApp } from 'nesoi/lib/engine/app/inline.app';
import { PostgresService } from '../src/postgres.service';
import { PostgresBucketAdapter } from '../src/postgres.bucket_adapter';
import { PostgresConfig } from '../src/postgres.config';
import { AnyDaemon } from 'nesoi/lib/engine/daemon';
import { MigrationProvider } from '../src/migrator/generator/provider';
import { MigrationRunner } from '../src/migrator/runner/runner';
import { Database } from '../src/migrator/database';

Log.level = 'off';

// TODO: read this from env
const PostgresConfig = (): PostgresConfig => ({
    meta: {
        created_at: 'created_at',
        created_by: 'created_by',
        updated_at: 'updated_at',
        updated_by: 'updated_by',
    },
    connection: {
        host: 'localhost',
        port: 5432,
        user: 'postgres',
        pass: 'postgres',
        db: 'NESOI_NQL_TEST',
    }
});

let daemon: AnyDaemon;

async function setup() {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (daemon) {
        return daemon;
    }
    
    // Build buckets used for test

    const tagBucket = new BucketBuilder('MODULE', 'tag')
        .model($ => ({
            id: $.string,
            scope: $.string
        }));

    const colorBucket = new BucketBuilder('MODULE', 'color')
        .model($ => ({
            id: $.string,
            name: $.string
        }));

    const pg = new PostgresService(PostgresConfig);
    
    // Build test app
    const app = new InlineApp('RUNTIME', [
        tagBucket,
        colorBucket,
    ])
        .service(pg)
        .config.module('MODULE', {
            buckets: {
                'tag': {
                    adapter: ($, {pg}) => new PostgresBucketAdapter($, pg, 'tags'),
                },
                'color': {
                    adapter: ($, {pg}) => new PostgresBucketAdapter($, pg, 'colors'),
                },
            },
            trx: {
                wrap: [PostgresService.wrap('pg')]
            }
        });
        
    // Run test daemon
    daemon = await app.daemon();
    
    // Prepare database using daemon
    // TODO: encapsulate this

    await Database.createDatabase('NESOI_NQL_TEST', PostgresConfig().connection, { if_exists: 'delete' });

    const migrator = await MigrationProvider.create(daemon, pg);
    for (const bucket of ['tag', 'color']) {
        const migration = await migrator.generateForBucket('MODULE', bucket, bucket+'s');
        if (migration) {
            migration.name = 'postgres.nql.'+bucket;
            await MigrationRunner.fromSchema.up(daemon, pg, migration);
        }
    }
    
    return daemon;
}

/* Generic Test */

describe('PostgreSQL Transactions', () => {

    beforeAll(async () => {
        await setup();
    }, 30000);
    afterAll(async () => {
        await daemon.destroy();
    }, 30000);

    describe('Atomicity', () => {

        it('should rollback changes on error', async () => {
            let tags1;
            const result1 = await daemon.trx('MODULE').run(async trx => {
                await trx.bucket('tag').create({
                    id: 'TAG1',
                    scope: 'test'
                });
                await trx.bucket('tag').create({
                    id: 'TAG2',
                    scope: 'test'
                });
                await trx.bucket('tag').create({
                    id: 'TAG3',
                    scope: 'test'
                });
                
                tags1 = await trx.bucket('tag').readAll();
            });
            expect(tags1).toHaveLength(3);
            expect(result1.state).toEqual('ok');            
            
            let tags2;
            const result2 = await daemon.trx('MODULE').run(async trx => {                
                tags2 = await trx.bucket('tag').readAll();
            });
            expect(tags2).toHaveLength(3);
            expect(result2.state).toEqual('ok');
        });

        it('should rollback changes on error', async () => {
            let tags1;
            const result1 = await daemon.trx('MODULE').run(async trx => {
                await trx.bucket('tag').create({
                    id: 'TAG4',
                    scope: 'test'
                });
                await trx.bucket('tag').create({
                    id: 'TAG5',
                    scope: 'test'
                });
                await trx.bucket('tag').create({
                    id: 'TAG6',
                    scope: 'test'
                });
                
                tags1 = await trx.bucket('tag').readAll();
                throw new Error('Simulated error, used to trigger a rollback on test.');
            });
            expect(tags1).toHaveLength(6);
            expect(result1.state).toEqual('error');
            
            let tags2;
            const result2 = await daemon.trx('MODULE').run(async trx => {                
                tags2 = await trx.bucket('tag').readAll();
            });
            expect(tags2).toHaveLength(3);
            expect(result2.state).toEqual('ok');
        });

        it('should rollback multiple changes on error', async () => {
            let tags1;
            let colors1;
            const result1 = await daemon.trx('MODULE').run(async trx => {
                await trx.bucket('tag').create({
                    id: 'TAG4',
                    scope: 'test'
                });
                await trx.bucket('tag').create({
                    id: 'TAG5',
                    scope: 'test'
                });
                await trx.bucket('tag').create({
                    id: 'TAG6',
                    scope: 'test'
                });
                tags1 = await trx.bucket('tag').readAll();
                
                await trx.bucket('color').create({
                    id: 'COLOR4',
                    name: 'red'
                });
                await trx.bucket('color').create({
                    id: 'COLOR5',
                    name: 'green'
                });
                await trx.bucket('color').create({
                    id: 'COLOR6',
                    name: 'blue'
                });
                colors1 = await trx.bucket('color').readAll();

                throw new Error('Simulated error, used to trigger a rollback on test.');
            });
            expect(tags1).toHaveLength(6);
            expect(colors1).toHaveLength(3);
            expect(result1.state).toEqual('error');
            
            let tags2;
            let colors2;
            const result2 = await daemon.trx('MODULE').run(async trx => {                
                tags2 = await trx.bucket('tag').readAll();
                colors2 = await trx.bucket('color').readAll();
            });
            expect(tags2).toHaveLength(3);
            expect(colors2).toHaveLength(0);
            expect(result2.state).toEqual('ok');
        });
        

    });
});