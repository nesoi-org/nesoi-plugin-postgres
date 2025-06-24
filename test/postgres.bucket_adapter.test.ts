
import { Mock } from 'nesoi/tools/joaquin/mock';
import { BucketBuilder } from 'nesoi/lib/elements/entities/bucket/bucket.builder';
import { Log } from 'nesoi/lib/engine/util/log';
import { InlineApp } from 'nesoi/lib/engine/apps/inline.app';
import { PostgresService } from '../src/postgres.service';
import { PostgresBucketAdapter } from '../src/postgres.bucket_adapter';
import { PostgresConfig } from '../src/postgres.config';
import { AnyDaemon } from 'nesoi/lib/engine/daemon';
import { MigrationProvider } from '../src/migrator/generator/provider';
import { MigrationRunner } from '../src/migrator/runner/runner';
import { Database } from '../src/migrator/database';
import { NesoiDatetime } from 'nesoi/lib/engine/data/datetime';

Log.level = 'warn';

// TODO: read this from env
const PostgresConfig: PostgresConfig = {
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
        db: 'NESOI_TEST',
    }
};

let daemon: AnyDaemon;
async function setup() {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (daemon) {
        return daemon;
    }

    const pg = new PostgresService(PostgresConfig);
    
    // Build bucket used for test
    const bucket = new BucketBuilder('MODULE', 'BUCKET')
        .model($ => ({
            id: $.int,
            p_boolean: $.boolean,
            p_date: $.date,
            p_datetime: $.datetime,
            p_decimal: $.decimal(),
            p_enum: $.enum(['a','b','c']),
            p_int: $.int,
            p_float: $.float,
            p_string: $.string,
            p_obj: $.obj({
                a: $.int,
                b: $.string
            }),
            p_dict: $.dict($.boolean)
        }));

    // Build test app
    const app = new InlineApp('RUNTIME', [
        bucket
    ])
        .service(pg)
        .config.buckets({
            'MODULE': {
                'BUCKET': {
                    adapter: ($, {pg}) => new PostgresBucketAdapter($, pg, 'nesoi_test_table')
                }
            }
        })
        .config.trx({
            'MODULE': {
                wrap: PostgresService.wrap('pg')
            }
        });
    
    // Run test daemon
    daemon = await app.daemon();

    // Prepare database using daemon
    // TODO: encapsulate this

    await Database.createDatabase('NESOI_TEST', PostgresConfig.connection, { if_exists: 'delete' });

    const migrator = await MigrationProvider.create(daemon, pg);
    const migration = await migrator.generateForBucket('MODULE', 'BUCKET', 'nesoi_test_table');
    if (migration) {
        migration.name = 'postgres.bucket_adapter.test';
        await MigrationRunner.fromSchema.up(daemon, pg, migration);
    }
        
    return daemon;
}

const mock = new Mock();
beforeAll(async () => {
    await setup();
}, 30000);
afterAll(async () => {
    await daemon.destroy();
}, 30000);

describe('Postgres Bucket Adapter', () => {

    describe('CRUD', () => {
        it('create should return unmodified object', async() => {
            const response = await daemon.trx('MODULE').run(async trx => {
                // given
                const BUCKET = trx.bucket('BUCKET');
                const input = mock.bucket('MODULE', 'BUCKET')
                    .obj({ id: undefined }).raw(daemon);
                
                // when
                const created = await BUCKET.create(input as any);
                return { input, created };
            });

            const { input, created: { id, ...obj } } = response.output!;

            // then
            expect(id).toBeTruthy();
            expect({
                ...obj,
                // The returned timezone doesn't necessarily matches the input
                p_datetime: new NesoiDatetime((obj).p_datetime.epoch, 'Z'),
            }).toEqual({
                ...input,
                created_at: expect.any(NesoiDatetime),
                updated_at: expect.any(NesoiDatetime),
                created_by: null,
                updated_by: null
            });
        });
    
        it('read should return unmodified object after create', async() => {
            const response = await daemon.trx('MODULE').run(async trx => {
                // given
                const BUCKET = trx.bucket('BUCKET');
                const input = mock.bucket('MODULE', 'BUCKET')
                    .obj({ id: undefined }).raw(daemon);
                
                // when
                const created = await BUCKET.create(input as any);
                const updated = await BUCKET.readOneOrFail(created.id);
                return { input, updated };
            });

            const { input, updated: { id, ...obj } } = response.output!;

            // then
            expect(id).toBeTruthy();
            expect({
                ...obj,
                // The returned timezone doesn't necessarily matches the input
                p_datetime: new NesoiDatetime(obj.p_datetime.epoch, 'Z'),
            }).toEqual({
                ...input,
                created_at: expect.any(NesoiDatetime),
                updated_at: expect.any(NesoiDatetime),
                created_by: null,
                updated_by: null
            });
        });
    
        it('update should modify object after insert', async() => {
            const response = await daemon.trx('MODULE').run(async trx => {
                // given
                const BUCKET = trx.bucket('BUCKET');
                const input1 = mock.bucket('MODULE', 'BUCKET')
                    .obj({ id: undefined }).raw(daemon);
                const input2 = mock.bucket('MODULE', 'BUCKET')
                    .obj({ id: undefined }).raw(daemon);
                
                // when
                const created = await BUCKET.create(input1 as any);
                const updated = await BUCKET.patch({
                    ...input2,
                    id: created.id
                } as any);
    
                return { input1, input2, created, updated };
            });

            const {
                input1,
                input2,
                created: { id: id_create, ...created },
                updated: { id: id_update, ...updated },
            } = response.output!;

            // then
            expect(id_create).toBeTruthy();
            expect(id_update).toEqual(id_create);
            expect({
                ...created,
                // The returned timezone doesn't necessarily matches the input
                p_datetime: new NesoiDatetime(created.p_datetime.epoch, 'Z'),
            }).toEqual({
                ...input1,
                created_at: expect.any(NesoiDatetime),
                updated_at: expect.any(NesoiDatetime),
                created_by: null,
                updated_by: null
            });
            expect({
                ...updated,
                // The returned timezone doesn't necessarily matches the input
                p_datetime: new NesoiDatetime(updated.p_datetime.epoch, 'Z'),
            }).toEqual({
                ...input2,
                created_at: expect.any(NesoiDatetime),
                updated_at: expect.any(NesoiDatetime),
                created_by: null,
                updated_by: null
            });
        });
    
        it('delete should remove object from database', async() => {
            const response = await daemon.trx('MODULE').run(async trx => {
                // given
                const BUCKET = trx.bucket('BUCKET');
                const input = mock.bucket('MODULE', 'BUCKET')
                    .obj({ id: undefined }).raw(daemon);
                
                // when
                const created = await BUCKET.create(input as any);            
                const read = await BUCKET.readOneOrFail(created.id);            
                await BUCKET.delete(read.id);
                const read2 = await BUCKET.readOne(read.id);            
    
                return { read2 };
            });

            const { read2 } = response.output!;

            // then
            expect(read2).toBeUndefined();
        });
    });

    describe('Query', () => {

        it('query first by existing id should return object', async() => {
            const response = await daemon.trx('MODULE').run(async trx => {
                // given
                const BUCKET = trx.bucket('BUCKET');
                const input = mock.bucket('MODULE', 'BUCKET')
                    .obj({ id: undefined }).raw(daemon);
                
                // when
                const created = await BUCKET.create(input as any);
                const queried = await BUCKET.query({
                    'id': created.id
                }).all();
    
                return { queried, created };
            });

            const { queried, created } = response.output!;

            // then
            expect(queried.length).toEqual(1);
            expect(queried[0].id).toEqual(created.id);
        });

    });

});