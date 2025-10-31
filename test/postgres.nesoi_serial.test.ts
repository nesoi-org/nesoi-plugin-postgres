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
import { NesoiDecimal } from 'nesoi/lib/engine/data/decimal';
import { NesoiDate } from 'nesoi/lib/engine/data/date';
import { NesoiDatetime } from 'nesoi/lib/engine/data/datetime';

Log.level = 'warn';

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
let pg: PostgresService;

async function setup() {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (daemon) {
        return daemon;
    }
    
    // Build buckets used for test

    const dateBucket = new BucketBuilder('MODULE', 'date')
        .model($ => ({
            id: $.int,
            value: $.date,
            deep: $.obj({
                value: $.date,
                deeper: $.list($.obj({
                    value: $.date,
                }))
            })
        }));

    const datetimeBucket = new BucketBuilder('MODULE', 'datetime')
        .model($ => ({
            id: $.int,
            value: $.datetime,
            deep: $.obj({
                value: $.datetime,
                deeper: $.list($.obj({
                    value: $.datetime,
                }))
            })
        }));

    const decimalBucket = new BucketBuilder('MODULE', 'decimal')
        .model($ => ({
            id: $.int,
            value: $.decimal(),
            deep: $.obj({
                value: $.decimal(),
                deeper: $.list($.obj({
                    value: $.decimal(),
                }))
            })
        }));

    const durationBucket = new BucketBuilder('MODULE', 'duration')
        .model($ => ({
            id: $.int,
            value: $.duration,
            deep: $.obj({
                value: $.duration,
                deeper: $.list($.obj({
                    value: $.duration,
                }))
            })
        }));



    pg = new PostgresService(PostgresConfig);
    
    // Build test app
    const app = new InlineApp('RUNTIME', [
        dateBucket,
        datetimeBucket,
        decimalBucket,
        durationBucket,
    ])
        .service(pg)
        .config.module('MODULE', {
            buckets: {
                'date': {
                    adapter: ($, {pg}) => new PostgresBucketAdapter($, pg, 'dates'),
                },
                'datetime': {
                    adapter: ($, {pg}) => new PostgresBucketAdapter($, pg, 'datetimes'),
                },
                'decimal': {
                    adapter: ($, {pg}) => new PostgresBucketAdapter($, pg, 'decimals'),
                },
                'duration': {
                    adapter: ($, {pg}) => new PostgresBucketAdapter($, pg, 'durations'),
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
    for (const bucket of ['date', 'datetime', 'decimal', 'duration']) {
        const migration = await migrator.generateForBucket('MODULE', bucket, bucket+'s');
        if (migration) {
            migration.name = 'postgres.nql.'+bucket;
            await MigrationRunner.fromSchema.up(daemon, pg, migration);
        }
    }
    
    return daemon;
}

/* Generic Test */

describe('Serialization', () => {

    beforeAll(async () => {
        await setup();
    }, 30000);
    afterAll(async () => {
        await daemon.destroy();
    }, 30000);

    it('should store/read date properly', async () => {
        let created: any;
        let read: any;
        await daemon.trx('MODULE').run(async trx => {
            created = await trx.bucket('date').create({
                value: NesoiDate.fromISO('2025-01-02'),
                deep: {
                    value: NesoiDate.fromISO('2025-03-04'),
                    deeper: [
                        {
                            value: NesoiDate.fromISO('2025-05-06')
                        }
                    ]
                }
            });
            read = await trx.bucket('date').readOne(created.id);
        });

        const rows = await pg.sql`SELECT * FROM dates`;

        expect(rows[0].value).toBeInstanceOf(NesoiDate);
        expect(typeof rows[0].deep.value).toBe('string');
        expect(typeof rows[0].deep.deeper[0].value).toBe('string');
        
        expect(created.value).toBeInstanceOf(NesoiDate);
        expect(created.deep.value).toBeInstanceOf(NesoiDate);
        expect(created.deep.deeper[0].value).toBeInstanceOf(NesoiDate);
        
        expect(read.value).toBeInstanceOf(NesoiDate);
        expect(read.deep.value).toBeInstanceOf(NesoiDate);
        expect(read.deep.deeper[0].value).toBeInstanceOf(NesoiDate);

        // then
        expect(rows.length).toEqual(1);
    });

    it('should store/read datetime properly', async () => {
        let created: any;
        let read: any;
        await daemon.trx('MODULE').run(async trx => {
            created = await trx.bucket('datetime').create({
                value: NesoiDatetime.fromISO('2025-01-02T00:01:02Z'),
                deep: {
                    value: NesoiDatetime.fromISO('2025-03-04T03:04:05Z'),
                    deeper: [
                        {
                            value: NesoiDatetime.fromISO('2025-05-06T06:07:08Z')
                        }
                    ]
                }
            });
            read = await trx.bucket('datetime').readOne(created.id);
        });

        const rows = await pg.sql`SELECT * FROM datetimes`;

        expect(rows[0].value).toBeInstanceOf(NesoiDatetime);
        expect(typeof rows[0].deep.value).toBe('string');
        expect(typeof rows[0].deep.deeper[0].value).toBe('string');
        
        expect(created.value).toBeInstanceOf(NesoiDatetime);
        expect(created.deep.value).toBeInstanceOf(NesoiDatetime);
        expect(created.deep.deeper[0].value).toBeInstanceOf(NesoiDatetime);
        
        expect(read.value).toBeInstanceOf(NesoiDatetime);
        expect(read.deep.value).toBeInstanceOf(NesoiDatetime);
        expect(read.deep.deeper[0].value).toBeInstanceOf(NesoiDatetime);

        // then
        expect(rows.length).toEqual(1);
    });
    
    it('should store/read decimal properly', async () => {
        let created: any;
        let read: any;
        await daemon.trx('MODULE').run(async trx => {
            created = await trx.bucket('decimal').create({
                value: new NesoiDecimal('123.456'),
                deep: {
                    value: new NesoiDecimal('789.012'),
                    deeper: [
                        {
                            value: new NesoiDecimal('345.678')
                        }
                    ]
                }
            });
            read = await trx.bucket('decimal').readOne(created.id);
        });

        const rows = await pg.sql`SELECT * FROM decimals`;

        expect(rows[0].value).toBeInstanceOf(NesoiDecimal);
        expect(typeof rows[0].deep.value).toBe('string');
        expect(typeof rows[0].deep.deeper[0].value).toBe('string');
        
        expect(created.value).toBeInstanceOf(NesoiDecimal);
        expect(created.deep.value).toBeInstanceOf(NesoiDecimal);
        expect(created.deep.deeper[0].value).toBeInstanceOf(NesoiDecimal);
        
        expect(read.value).toBeInstanceOf(NesoiDecimal);
        expect(read.deep.value).toBeInstanceOf(NesoiDecimal);
        expect(read.deep.deeper[0].value).toBeInstanceOf(NesoiDecimal);

        // then
        expect(rows.length).toEqual(1);
    });   
    
});