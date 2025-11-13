/* eslint-disable @typescript-eslint/require-await */
 
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
import { ExternalsBuilder } from 'nesoi/lib/elements/edge/externals/externals.builder';
import { AnyTrx, TrxStatus } from 'nesoi/lib/engine/transaction/trx';
import { TrxEngineConfig } from 'nesoi/lib/engine/transaction/trx_engine.config';
import { AnyBucketAdapter } from 'nesoi/lib/elements/entities/bucket/adapters/bucket_adapter';
import { JobBuilder } from 'nesoi/lib/elements/blocks/job/job.builder';

Log.level = 'off';

const meta = {
    created_at: 'created_at',
    created_by: 'created_by',
    updated_at: 'updated_at',
    updated_by: 'updated_by',
};

const PostgresConfig1 = (): PostgresConfig => ({
    meta,
    connection: {
        host: 'localhost',
        port: 5432,
        user: 'postgres',
        pass: 'postgres',
        db: 'NESOI_NQL_TEST1',
    }
});

const PostgresConfig2 = (): PostgresConfig => ({
    meta,
    connection: {
        host: 'localhost',
        port: 5432,
        user: 'postgres',
        pass: 'postgres',
        db: 'NESOI_NQL_TEST2',
    }
});

const PostgresConfig3 = (): PostgresConfig => ({
    meta,
    connection: {
        host: 'localhost',
        port: 5432,
        user: 'postgres',
        pass: 'postgres',
        db: 'NESOI_NQL_TEST3',
    }
});

let daemon: AnyDaemon;
let pg1: PostgresService<'pg1'>;
let pg2: PostgresService<'pg2'>;
let pg3: PostgresService<'pg3'>;

async function setup() {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (daemon) {
        await daemon.destroy();
    }
    
    // Build buckets used for test

    const oneBucket = new BucketBuilder('MODULE1', 'one')
        .model($ => ({
            id: $.string,
            name: $.string
        }));

    const twoBucket = new BucketBuilder('MODULE1', 'two')
        .model($ => ({
            id: $.string,
            name: $.string
        }));

    const threeBucket = new BucketBuilder('MODULE2', 'three')
        .model($ => ({
            id: $.string,
            name: $.string
        }));

    const fourBucket = new BucketBuilder('MODULE2', 'four')
        .model($ => ({
            id: $.string,
            name: $.string
        }));

    const fiveBucket = new BucketBuilder('MODULE3', 'five')
        .model($ => ({
            id: $.string,
            name: $.string
        }));

    const selfNonIdptJob = new JobBuilder('MODULE2', 'self_non_idpt')
        .message('', $ => ({ name: $.string }))
        .input('@')
        .method($ => $.trx.bucket('three').create({
            id: 'TEST',
            name: ($.msg as any).name
        }));

    const selfIdptJob = new JobBuilder('MODULE2', 'self_idpt')
        .message('', $ => ({ name: $.string }))
        .input('@')
        .method($ => $.trx.bucket('three').readAll());

    const otherNonIdptJob = new JobBuilder('MODULE2', 'other_non_idpt')
        .message('', $ => ({ name: $.string }))
        .input('@')
        .method($ => $.trx.bucket('MODULE3::five').create({
            id: 'TEST',
            name: ($.msg as any).name
        }));

    const otherIdptJob = new JobBuilder('MODULE2', 'other_idpt')
        .message('', $ => ({ name: $.string }))
        .input('@')
        .method($ => $.trx.bucket('MODULE3::five').readAll());

        
    pg1 = new PostgresService('pg1', PostgresConfig1);
    pg2 = new PostgresService('pg2', PostgresConfig2);
    pg3 = new PostgresService('pg3', PostgresConfig3);
    
    // Build test app
    const app = new InlineApp('RUNTIME', [
        oneBucket,
        twoBucket,
        threeBucket,
        fourBucket,
        fiveBucket,
        selfNonIdptJob,
        selfIdptJob,
        otherNonIdptJob,
        otherIdptJob,
        new ExternalsBuilder('MODULE2')
            .bucket('MODULE3::five' as never),
        new ExternalsBuilder('MODULE3')
            .bucket('MODULE1::one' as never)
    ])
        .service(pg1)
        .service(pg2)
        .service(pg3)
        .config.module('MODULE1', {
            buckets: {
                'one': {
                    adapter: ($, {pg1}) => new PostgresBucketAdapter($, pg1, 'ones'),
                },
                'two': {
                    adapter: ($, {pg1}) => new PostgresBucketAdapter($, pg1, 'twos'),
                },
            },
            trx: {
                wrap: [PostgresService.wrap('pg1')]
            }
        })
        .config.module('MODULE2', {
            buckets: {
                'three': {
                    adapter: ($, {pg1}) => new PostgresBucketAdapter($, pg1, 'threes'),
                },
                'four': {
                    adapter: ($, {pg2}) => new PostgresBucketAdapter($, pg2, 'fours'),
                },
            },
            trx: {
                wrap: [PostgresService.wrap('pg1'), PostgresService.wrap('pg2')]
            }
        })
        .config.module('MODULE3', {
            buckets: {
                'five': {
                    adapter: ($, {pg3}) => new PostgresBucketAdapter($, pg3, 'fives'),
                },
            },
            trx: {
                wrap: [PostgresService.wrap('pg3')]
            }
        });
        
    // Run test daemon
    daemon = await app.daemon();
    
    // Prepare database using daemon
    // TODO: encapsulate this

    await Database.createDatabase('NESOI_NQL_TEST1', PostgresConfig1().connection, { if_exists: 'delete' });
    await Database.createDatabase('NESOI_NQL_TEST2', PostgresConfig2().connection, { if_exists: 'delete' });
    await Database.createDatabase('NESOI_NQL_TEST3', PostgresConfig3().connection, { if_exists: 'delete' });

    const migrator1 = await MigrationProvider.create(daemon, pg1);
    const migrator2 = await MigrationProvider.create(daemon, pg2);
    const migrator3 = await MigrationProvider.create(daemon, pg3);
    
    const buckets = [
        { name: 'one', module: 'MODULE1', migrator: migrator1, pg: pg1 },
        { name: 'two', module: 'MODULE1', migrator: migrator1, pg: pg1 },
        { name: 'three', module: 'MODULE2', migrator: migrator1, pg: pg1 },
        { name: 'four', module: 'MODULE2', migrator: migrator2, pg: pg2 },
        { name: 'five', module: 'MODULE3', migrator: migrator3, pg: pg3 },
    ];

    for (const $ of buckets) {
        const migration = await $.migrator.generateForBucket($.module, $.name, $.name+'s');
        if (migration) {
            migration.name = 'postgres.nql.'+$.name;
            await MigrationRunner.fromSchema.up(daemon, $.pg, migration);
        }
    }
    
    return daemon;
}

/* Generic Test */

describe('PostgreSQL Transactions', () => {

    beforeEach(async () => {
        await setup();
    }, 30000);
    afterAll(async () => {
        await daemon.destroy();
    }, 30000);

    describe('Atomicity', () => {

        it('should commit changes', async () => {
            let ones1;
            const result1 = await daemon.trx('MODULE1').run(async trx => {
                await trx.bucket('one').create({
                    id: 'ONE1',
                    name: 'test'
                });
                await trx.bucket('one').create({
                    id: 'ONE2',
                    name: 'test'
                });
                await trx.bucket('one').create({
                    id: 'ONE3',
                    name: 'test'
                });
                
                ones1 = await trx.bucket('one').readAll();
            });
            expect(ones1).toHaveLength(3);
            expect(result1.state).toEqual('ok');            
            
            let ones2;
            const result2 = await daemon.trx('MODULE1').run(async trx => {                
                ones2 = await trx.bucket('one').readAll();
            }, undefined, true);
            expect(ones2).toHaveLength(3);
            expect(result2.state).toEqual('ok');
        });

        it('should rollback changes on error', async () => {
            let ones1;
            const result1 = await daemon.trx('MODULE1').run(async trx => {
                await trx.bucket('one').create({
                    id: 'ONE4',
                    name: 'test'
                });
                await trx.bucket('one').create({
                    id: 'ONE5',
                    name: 'test'
                });
                await trx.bucket('one').create({
                    id: 'ONE6',
                    name: 'test'
                });
                
                ones1 = await trx.bucket('one').readAll();
                throw new Error('Simulated error, used to trigger a rollback on test.');
            });
            expect(ones1).toHaveLength(3);
            expect(result1.state).toEqual('error');
            
            let ones2;
            const result2 = await daemon.trx('MODULE1').run(async trx => {                
                ones2 = await trx.bucket('one').readAll();
            }, undefined, true);
            expect(ones2).toHaveLength(0);
            expect(result2.state).toEqual('ok');
        });

        it('should rollback multiple changes on error', async () => {
            let ones1;
            let twos1;
            const result1 = await daemon.trx('MODULE1').run(async trx => {
                await trx.bucket('one').create({
                    id: 'ONE4',
                    name: 'test'
                });
                await trx.bucket('one').create({
                    id: 'ONE5',
                    name: 'test'
                });
                await trx.bucket('one').create({
                    id: 'ONE6',
                    name: 'test'
                });
                ones1 = await trx.bucket('one').readAll();
                
                await trx.bucket('two').create({
                    id: 'TWO4',
                    name: 'red'
                });
                await trx.bucket('two').create({
                    id: 'TWO5',
                    name: 'green'
                });
                await trx.bucket('two').create({
                    id: 'TWO6',
                    name: 'blue'
                });
                twos1 = await trx.bucket('two').readAll();

                throw new Error('Simulated error, used to trigger a rollback on test.');
            });
            expect(ones1).toHaveLength(3);
            expect(twos1).toHaveLength(3);
            expect(result1.state).toEqual('error');
            
            let ones2;
            let twos2;
            const result2 = await daemon.trx('MODULE1').run(async trx => {                
                ones2 = await trx.bucket('one').readAll();
                twos2 = await trx.bucket('two').readAll();
            }, undefined, true);
            expect(ones2).toHaveLength(0);
            expect(twos2).toHaveLength(0);
            expect(result2.state).toEqual('ok');
        });

    });

    async function idpt_test(fn: () => Promise<any>, spies: Record<string, {
            begin: number,
            continue: number,
            commit: number,
            rollback: number,
        }>): Promise<TrxStatus<any>> {

        const spy: Record<string, {
                begin: jest.SpyInstance,
                continue: jest.SpyInstance,
                commit: jest.SpyInstance,
                rollback: jest.SpyInstance,
            }> = {};
        for (const module in spies) {
            const trxEngines = (daemon as any).trxEngines as AnyDaemon['trxEngines'];
            const config = (trxEngines[module] as any).config as TrxEngineConfig<any, any, any, any>;
            const wrap = config.wrap![0];
                
            spy[module] = {
                begin: jest.spyOn(wrap, 'begin'),
                continue: jest.spyOn(wrap, 'continue'),
                commit: jest.spyOn(wrap, 'commit'),
                rollback: jest.spyOn(wrap, 'rollback'),
            };
        }

        const status = await fn();
        if (Log.level !== 'off') {
            console.log(status.summary());
        }

        for (const module in spies) {
            expect(spy[module].begin).toHaveBeenCalledTimes(spies[module].begin);
            expect(spy[module].continue).toHaveBeenCalledTimes(spies[module].continue);
            expect(spy[module].commit).toHaveBeenCalledTimes(spies[module].commit);
            expect(spy[module].rollback).toHaveBeenCalledTimes(spies[module].rollback);
        }

        return status;
    }

    describe('Idempotency', () => {

        it('should create non-idempotent transaction', async () => {
            let idpt;
            await daemon.trx('MODULE1').run(async trx => {
                idpt = ((trx as any).trx as AnyTrx).idempotent;
                return Promise.resolve();
            });
            expect(idpt).toEqual(false);
        });

        it('should create idempotent transaction', async () => {
            let idpt;
            await daemon.trx('MODULE1').run(async trx => {
                idpt = ((trx as any).trx as AnyTrx).idempotent;
                return Promise.resolve();
            }, undefined, true);
            expect(idpt).toEqual(true);
        });

        it('should trigger commit for non-idempotent transaction', async () => {
            await idpt_test(async () => {
                await daemon.trx('MODULE1').run(async () => {
                    return Promise.resolve();
                });
            }, {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 }
            });
        });

        it('should trigger rollback for failed non-idempotent transaction', async () => {
            await idpt_test(async () => {
                await daemon.trx('MODULE1').run(async () => {
                    throw new Error('Simulated error, used to trigger a rollback on test.');
                });
            }, {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 1 }
            });
        });

        it('should not trigger commit for idempotent transaction', async () => {
            await idpt_test(async () => {
                await daemon.trx('MODULE1').run(async () => {
                    return Promise.resolve();
                }, undefined, true);
            }, {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 }
            });
        });

        it('should not trigger rollback for idempotent transaction', async () => {
            await idpt_test(async () => {
                await daemon.trx('MODULE1').run(async () => {
                    throw new Error('Simulated error, used to trigger a rollback on test.');
                }, undefined, true);
            }, {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 }
            });
        });
    });

    async function checkTrxCleanup(logs?: {
        [name: string]: number | undefined
    }) {
        // Nesoi Transaction registry on engines
        const trxEngines = (daemon as any).trxEngines as AnyDaemon['trxEngines'];
        for (const module of ['MODULE1', 'MODULE2', 'MODULE3']) {
            const engine = trxEngines[module];
            const adapter = (engine as any).adapter as AnyBucketAdapter;
            const trxs = await adapter.index({} as any);
            expect(trxs).toHaveLength(0);
            
            if (logs?.[module]) {
                const trx_logs = await adapter.index({} as any);
                expect(trx_logs).toHaveLength(logs[module]);
            }
        }

        // DB Transaction registry on services
        expect(Object.keys((pg1 as any).transactions as PostgresService['transactions'])).toHaveLength(0);
        expect(Object.keys((pg2 as any).transactions as PostgresService['transactions'])).toHaveLength(0);
        expect(Object.keys((pg3 as any).transactions as PostgresService['transactions'])).toHaveLength(0);
    }
    
    describe('Idempotency Tree', () => {

        it('non-idempotent trx', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async () => {
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
            });
            expect(status.idempotent).toEqual(false);
        });

        it('non-idempotent trx, 1 non-idempotent child', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').create({
                        id: 'THREE5',
                        name: 'test'
                    });
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 1, rollback: 0 },
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 1 idempotent child', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').readAll();
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 0, rollback: 0 },
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 2 non-idempotent child (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').create({
                        id: 'THREE5',
                        name: 'test'
                    });
                    await trx.bucket('MODULE2::three').create({
                        id: 'THREE6',
                        name: 'test'
                    });
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 1, commit: 1, rollback: 0 }
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[1].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 2 non-idempotent child (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').create({
                        id: 'THREE5',
                        name: 'test'
                    });
                    await trx.bucket('MODULE3::five').create({
                        id: 'FIVE3',
                        name: 'test'
                    });
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 1, rollback: 0 }
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE3::bucket:five');
            expect(status.nodes[1].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 2 idempotent child (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').readAll();
                    await trx.bucket('MODULE2::three').readAll();
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 2, continue: 0, commit: 0, rollback: 0 }
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[1].ext?.idempotent).toEqual(true);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 2 idempotent child (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').readAll();
                    await trx.bucket('MODULE3::five').readAll();
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 0, rollback: 0 }
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE3::bucket:five');
            expect(status.nodes[1].ext?.idempotent).toEqual(true);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 1 idempotent child + 1 non-idempotent child (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').readAll();
                    await trx.bucket('MODULE2::three').create({
                        id: 'THREE5',
                        name: 'test'
                    });
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 2, continue: 0, commit: 1, rollback: 0 }
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[1].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 1 non-idempotent child + 1 idempotent child (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').create({
                        id: 'THREE5',
                        name: 'test'
                    });
                    await trx.bucket('MODULE2::three').readAll();
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 1, commit: 1, rollback: 0 }
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            
            // This is the edge case where a trx requested as idempotent (readAll)
            // actually becomes non-idempotent due to a previously open
            // trx with the same id on the module.
            expect(status.nodes[1].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 1 idempotent child + 1 non-idempotent child (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').readAll();
                    await trx.bucket('MODULE3::five').create({
                        id: 'FIVE3',
                        name: 'test'
                    });
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 1, rollback: 0 },
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE3::bucket:five');
            expect(status.nodes[1].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 1 non-idempotent child + 1 idempotent child (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').create({
                        id: 'THREE5',
                        name: 'test'
                    });
                    await trx.bucket('MODULE3::five').readAll();
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 0, rollback: 0 },
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE3::bucket:five');
            expect(status.nodes[1].ext?.idempotent).toEqual(true);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 1 non-idempotent child, 1 non-idempotent subchild (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::self_non_idpt').run({
                        name: 'text'
                    });
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 1, rollback: 0 },
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::job:self_non_idpt');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 1 non-idempotent child, 1 idempotent subchild (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::self_idpt').run({
                        name: 'text'
                    });
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 1, rollback: 0 },
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::job:self_idpt');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 1 idempotent child, 1 non-idempotent subchild (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::self_non_idpt').idempotent.run({
                        name: 'text'
                    });
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 1 },
                MODULE2: { begin: 1, continue: 0, commit: 0, rollback: 0 },
            });

            // This scenario should fail due to a non-idempotent bucket action
            // performed inside an idempotent transaction.
            expect(status.state).toEqual('error');
            expect(status.error?.name).toEqual('Bucket.IdempotentTransaction');
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 1 idempotent child, 1 idempotent subchild (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::self_idpt').idempotent.run({
                        name: 'text'
                    });
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 0, rollback: 0 },
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::job:self_idpt');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 1 non-idempotent child, 1 non-idempotent subchild (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::other_non_idpt').run({
                        name: 'text'
                    });
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 1, rollback: 0 },
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::job:other_non_idpt');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            expect(status.nodes[0].nodes[0].nodes[1].scope).toEqual('MODULE2::externals:MODULE3::bucket:five');
            expect(status.nodes[0].nodes[0].nodes[1].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 1 non-idempotent child, 1 idempotent subchild (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::other_idpt').run({
                        name: 'text'
                    });
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 0, rollback: 0 },
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::job:other_idpt');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            expect(status.nodes[0].nodes[0].nodes[1].scope).toEqual('MODULE2::externals:MODULE3::bucket:five');
            expect(status.nodes[0].nodes[0].nodes[1].ext?.idempotent).toEqual(true);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 1 idempotent child, 1 non-idempotent subchild (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::other_non_idpt').idempotent.run({
                        name: 'text'
                    });
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 1, rollback: 0 },
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::job:other_non_idpt');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            expect(status.nodes[0].nodes[0].nodes[1].scope).toEqual('MODULE2::externals:MODULE3::bucket:five');
            expect(status.nodes[0].nodes[0].nodes[1].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('non-idempotent trx, 1 idempotent child, 1 idempotent subchild (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::other_idpt').idempotent.run({
                        name: 'text'
                    });
                }),
            {
                MODULE1: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 0, rollback: 0 },
            });

            expect(status.idempotent).toEqual(false);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::job:other_idpt');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            expect(status.nodes[0].nodes[0].nodes[1].scope).toEqual('MODULE2::externals:MODULE3::bucket:five');
            expect(status.nodes[0].nodes[0].nodes[1].ext?.idempotent).toEqual(true);
            await checkTrxCleanup();
        });

        it('idempotent trx', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async () => {
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
            });
            expect(status.idempotent).toEqual(true);
        });

        it('idempotent trx, 1 non-idempotent child', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').create({
                        id: 'THREE5',
                        name: 'test'
                    });
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 1, rollback: 0 },
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('idempotent trx, 1 idempotent child', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').readAll();
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 0, rollback: 0 },
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            await checkTrxCleanup();
        });

        it('idempotent trx, 2 non-idempotent child (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').create({
                        id: 'THREE5',
                        name: 'test'
                    });
                    await trx.bucket('MODULE2::three').create({
                        id: 'THREE6',
                        name: 'test'
                    });
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 1, commit: 1, rollback: 0 }
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[1].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('idempotent trx, 2 non-idempotent child (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').create({
                        id: 'THREE5',
                        name: 'test'
                    });
                    await trx.bucket('MODULE3::five').create({
                        id: 'FIVE3',
                        name: 'test'
                    });
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 1, rollback: 0 }
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE3::bucket:five');
            expect(status.nodes[1].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('idempotent trx, 2 idempotent child (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').readAll();
                    await trx.bucket('MODULE2::three').readAll();
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 2, continue: 0, commit: 0, rollback: 0 }
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[1].ext?.idempotent).toEqual(true);
            await checkTrxCleanup();
        });

        it('idempotent trx, 2 idempotent child (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').readAll();
                    await trx.bucket('MODULE3::five').readAll();
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 0, rollback: 0 }
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE3::bucket:five');
            expect(status.nodes[1].ext?.idempotent).toEqual(true);
            await checkTrxCleanup();
        });

        it('idempotent trx, 1 idempotent child + 1 non-idempotent child (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').readAll();
                    await trx.bucket('MODULE2::three').create({
                        id: 'THREE5',
                        name: 'test'
                    });
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 2, continue: 0, commit: 1, rollback: 0 }
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[1].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('idempotent trx, 1 non-idempotent child + 1 idempotent child (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').create({
                        id: 'THREE5',
                        name: 'test'
                    });
                    await trx.bucket('MODULE2::three').readAll();
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 1, commit: 1, rollback: 0 }
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            
            // This is the edge case where a trx requested as idempotent (readAll)
            // actually becomes non-idempotent due to a previously open
            // trx with the same id on the module.
            expect(status.nodes[1].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('idempotent trx, 1 idempotent child + 1 non-idempotent child (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').readAll();
                    await trx.bucket('MODULE3::five').create({
                        id: 'FIVE3',
                        name: 'test'
                    });
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 1, rollback: 0 },
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE3::bucket:five');
            expect(status.nodes[1].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('idempotent trx, 1 non-idempotent child + 1 idempotent child (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    await trx.bucket('MODULE2::three').create({
                        id: 'THREE5',
                        name: 'test'
                    });
                    await trx.bucket('MODULE3::five').readAll();
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 0, rollback: 0 },
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::bucket:three');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            expect(status.nodes[1].scope).toEqual('MODULE1::externals:MODULE3::bucket:five');
            expect(status.nodes[1].ext?.idempotent).toEqual(true);
            await checkTrxCleanup();
        });

        it('idempotent trx, 1 non-idempotent child, 1 non-idempotent subchild (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::self_non_idpt').run({
                        name: 'text'
                    });
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 1, rollback: 0 },
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::job:self_non_idpt');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('idempotent trx, 1 non-idempotent child, 1 idempotent subchild (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::self_idpt').run({
                        name: 'text'
                    });
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 1, rollback: 0 },
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::job:self_idpt');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('idempotent trx, 1 idempotent child, 1 non-idempotent subchild (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::self_non_idpt').idempotent.run({
                        name: 'text'
                    });
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 0, rollback: 0 },
            });

            // This scenario should fail due to a non-idempotent bucket action
            // performed inside an idempotent transaction.
            expect(status.state).toEqual('error');
            expect(status.error?.name).toEqual('Bucket.IdempotentTransaction');
            await checkTrxCleanup();
        });

        it('idempotent trx, 1 idempotent child, 1 idempotent subchild (same module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::self_idpt').idempotent.run({
                        name: 'text'
                    });
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 0, rollback: 0 },
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::job:self_idpt');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            await checkTrxCleanup();
        });

        it('idempotent trx, 1 non-idempotent child, 1 non-idempotent subchild (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::other_non_idpt').run({
                        name: 'text'
                    });
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 1, rollback: 0 },
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::job:other_non_idpt');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            expect(status.nodes[0].nodes[0].nodes[1].scope).toEqual('MODULE2::externals:MODULE3::bucket:five');
            expect(status.nodes[0].nodes[0].nodes[1].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('idempotent trx, 1 non-idempotent child, 1 idempotent subchild (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::other_idpt').run({
                        name: 'text'
                    });
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 1, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 0, rollback: 0 },
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::job:other_idpt');
            expect(status.nodes[0].ext?.idempotent).toEqual(false);
            expect(status.nodes[0].nodes[0].nodes[1].scope).toEqual('MODULE2::externals:MODULE3::bucket:five');
            expect(status.nodes[0].nodes[0].nodes[1].ext?.idempotent).toEqual(true);
            await checkTrxCleanup();
        });

        it('idempotent trx, 1 idempotent child, 1 non-idempotent subchild (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::other_non_idpt').idempotent.run({
                        name: 'text'
                    });
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 1, rollback: 0 },
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::job:other_non_idpt');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            expect(status.nodes[0].nodes[0].nodes[1].scope).toEqual('MODULE2::externals:MODULE3::bucket:five');
            expect(status.nodes[0].nodes[0].nodes[1].ext?.idempotent).toEqual(false);
            await checkTrxCleanup();
        });

        it('idempotent trx, 1 idempotent child, 1 idempotent subchild (diff module)', async () => {
            const status = await idpt_test(async () =>
                daemon.trx('MODULE1').run(async trx => {                
                    return trx.job('MODULE2::other_idpt').idempotent.run({
                        name: 'text'
                    });
                }, undefined, true),
            {
                MODULE1: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE2: { begin: 1, continue: 0, commit: 0, rollback: 0 },
                MODULE3: { begin: 1, continue: 0, commit: 0, rollback: 0 },
            });

            expect(status.idempotent).toEqual(true);
            expect(status.nodes[0].scope).toEqual('MODULE1::externals:MODULE2::job:other_idpt');
            expect(status.nodes[0].ext?.idempotent).toEqual(true);
            expect(status.nodes[0].nodes[0].nodes[1].scope).toEqual('MODULE2::externals:MODULE3::bucket:five');
            expect(status.nodes[0].nodes[0].nodes[1].ext?.idempotent).toEqual(true);
            await checkTrxCleanup();
        });

    });

    describe('Nesoi vs PostgreSQL transactions', () => {

        it('should commit 1 nesoi with 2 pg', async () => {
            let threes1;
            let fours1;
            const result1 = await daemon.trx('MODULE2').run(async trx => {
                await trx.bucket('three').create({
                    id: 'THREE1',
                    name: 'test'
                });
                await trx.bucket('three').create({
                    id: 'THREE2',
                    name: 'test'
                });
                await trx.bucket('four').create({
                    id: 'FOUR1',
                    name: 'test'
                });
                await trx.bucket('four').create({
                    id: 'FOUR2',
                    name: 'test'
                });
                
                threes1 = await trx.bucket('three').readAll();
                fours1 = await trx.bucket('four').readAll();
            });
            expect(threes1).toHaveLength(2);
            expect(fours1).toHaveLength(2);
            expect(result1.state).toEqual('ok');            
            
            let threes2;
            let fours2;
            const result2 = await daemon.trx('MODULE2').run(async trx => {                
                threes2 = await trx.bucket('three').readAll();
                fours2 = await trx.bucket('four').readAll();
            }, undefined, true);
            expect(threes2).toHaveLength(2);
            expect(fours2).toHaveLength(2);
            expect(result2.state).toEqual('ok');
        });

        it('should rollback 1 nesoi with 2 pg', async () => {
            let threes1;
            let fours1;
            const result1 = await daemon.trx('MODULE2').run(async trx => {
                await trx.bucket('three').create({
                    id: 'THREE3',
                    name: 'test'
                });
                await trx.bucket('three').create({
                    id: 'THREE4',
                    name: 'test'
                });
                await trx.bucket('four').create({
                    id: 'FOUR3',
                    name: 'test'
                });
                await trx.bucket('four').create({
                    id: 'FOUR4',
                    name: 'test'
                });
                
                threes1 = await trx.bucket('three').readAll();
                fours1 = await trx.bucket('four').readAll();

                throw new Error('Simulated error, used to trigger a rollback on test.');
            });
            expect(threes1).toHaveLength(2);
            expect(fours1).toHaveLength(2);
            expect(result1.state).toEqual('error');
            
            let threes2;
            let fours2;
            const result2 = await daemon.trx('MODULE2').run(async trx => {                
                threes2 = await trx.bucket('three').readAll();
                fours2 = await trx.bucket('four').readAll();
            }, undefined, true);
            expect(threes2).toHaveLength(0);
            expect(fours2).toHaveLength(0);
            expect(result2.state).toEqual('ok');
        });

        it('should commit 2 nesoi with 1 pg', async () => {
            let ones1;
            let fives1;
            const result1 = await daemon.trx('MODULE3').run(async trx => {
                await trx.bucket('five').create({
                    id: 'FIVE1',
                    name: 'test'
                });
                await trx.bucket('five').create({
                    id: 'FIVE2',
                    name: 'test'
                });
                await trx.bucket('MODULE1::one').create({
                    id: 'ONE7',
                    name: 'test'
                });
                await trx.bucket('MODULE1::one').create({
                    id: 'ONE8',
                    name: 'test'
                });
                
                ones1 = await trx.bucket('MODULE1::one').readAll();
                fives1 = await trx.bucket('five').readAll();
            });
            expect(ones1).toHaveLength(2);
            expect(fives1).toHaveLength(2);
            expect(result1.state).toEqual('ok');
            
            let ones2;
            let fives2;
            const result2 = await daemon.trx('MODULE3').run(async trx => {                
                ones2 = await trx.bucket('MODULE1::one').readAll();
                fives2 = await trx.bucket('five').readAll();
            }, undefined, true);
            expect(ones2).toHaveLength(2);
            expect(fives2).toHaveLength(2);
            expect(result2.state).toEqual('ok');
        });

        it('should rollback 2 nesoi with 1 pg', async () => {
            let ones1;
            let fives1;
            const result1 = await daemon.trx('MODULE3').run(async trx => {
                await trx.bucket('five').create({
                    id: 'FIVE1',
                    name: 'test'
                });
                await trx.bucket('five').create({
                    id: 'FIVE2',
                    name: 'test'
                });
                await trx.bucket('MODULE1::one').create({
                    id: 'ONE1',
                    name: 'test'
                });
                await trx.bucket('MODULE1::one').create({
                    id: 'ONE2',
                    name: 'test'
                });
                
                ones1 = await trx.bucket('MODULE1::one').readAll();
                fives1 = await trx.bucket('five').readAll();

                throw new Error('Simulated error, used to trigger a rollback on test.');
            });
            expect(ones1).toHaveLength(2);
            expect(fives1).toHaveLength(2);
            expect(result1.state).toEqual('error');
            
            let ones2;
            let fives2;
            const result2 = await daemon.trx('MODULE3').run(async trx => {                
                ones2 = await trx.bucket('MODULE1::one').readAll();
                fives2 = await trx.bucket('five').readAll();
            }, undefined, true);
            expect(fives2).toHaveLength(0);
            expect(ones2).toHaveLength(0);
            expect(result2.state).toEqual('ok');
        });

    });
});