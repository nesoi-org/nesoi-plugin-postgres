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
import { NQL_AnyQuery, NQL_Pagination } from 'nesoi/lib/elements/entities/bucket/query/nql.schema';

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
        db: 'NESOI_NQL_TEST',
    }
};

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
            id: $.int,
            name: $.string,
            r: $.float,
            g: $.float,
            b: $.float,
            tag: $.string,
            scope: $.string.optional,
        }));

    const shapeBucket = new BucketBuilder('MODULE', 'shape')
        .model($ => ({
            id: $.int,
            name: $.string,
            size: $.float,
            color_id: $.int,
            tag: $.string,
            scope: $.string.optional,
            props: $.dict($.string)
        }));


    const pg = new PostgresService(PostgresConfig);
    
    // Build test app
    const app = new InlineApp('RUNTIME', [
        tagBucket,
        colorBucket,
        shapeBucket
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
                'shape': {
                    adapter: ($, {pg}) => new PostgresBucketAdapter($, pg, 'shapes'),
                },
            },
            trx: {
                wrap: PostgresService.wrap('pg')
            }
        });
        
    // Run test daemon
    daemon = await app.daemon();
    
    // Prepare database using daemon
    // TODO: encapsulate this

    await Database.createDatabase('NESOI_NQL_TEST', PostgresConfig.connection, { if_exists: 'delete' });

    const migrator = await MigrationProvider.create(daemon, pg);
    for (const bucket of ['tag', 'color', 'shape']) {
        const migration = await migrator.generateForBucket('MODULE', bucket, bucket+'s');
        if (migration) {
            migration.name = 'postgres.nql.'+bucket;
            await MigrationRunner.fromSchema.up(daemon, pg, migration);
        }
    }
        
    // Populate database using daemon
    await daemon.trx('MODULE').run(async trx => {
        await trx.bucket('tag').put({
            id: 'Tag 1',
            scope: 'Scope 1',
            '#composition': {}
        });
        await trx.bucket('tag').put({
            id: 'Tag 2',
            scope: 'Scope 1',
            '#composition': {}
        });
        await trx.bucket('tag').put({
            id: 'Tag 3',
            scope: 'Scope 2',
            '#composition': {}
        });

        await trx.bucket('color').put({
            id: 1,
            name: 'Red',
            r: 1, g: 0, b: 0,
            tag: 'Tag 1',
            scope: 'Scope 1',
            '#composition': {}
        });
        await trx.bucket('color').put({
            id: 2,
            name: 'Green',
            r: 0, g: 1, b: 0,
            tag: 'Tag 2',
            scope: 'Scope 2',
            '#composition': {}
        });
        await trx.bucket('color').put({
            id: 3,
            name: 'Blue',
            r: 0, g: 0, b: 1,
            tag: 'Tag 3',
            '#composition': {}
        });

        await trx.bucket('shape').put({
            id: 1,
            name: 'Shape 1',
            size: 11,
            color_id: 1,
            tag: 'Tag 1',
            scope: 'Scope 1',
            props: { a: 1, b: 3, c: 0 },
            '#composition': {}
        });
        await trx.bucket('shape').put({
            id: 2,
            name: 'Shape 2',
            size: 22,
            color_id: 2,
            tag: 'Tag 2',
            scope: 'Scope 2',
            props: { a: 2, b: 2, c: 0 },
            '#composition': {}
        });

        await trx.bucket('shape').put({
            id: 3,
            name: 'Shape 3',
            size: 33,
            color_id: 3,
            tag: 'Tag 3',
            props: { a: 3, b: 1, c: 1 },
            '#composition': {}
        });
    });
    
    return daemon;
}

/* Generic Test */

type ExpectIdsFn = ((bucket: string, query: NQL_AnyQuery, ids: number[]) => Promise<void>)

const expectIds = (async function (this: any, bucket: string, query: NQL_AnyQuery, ids: number[]) {
    const page = (this)?.page as NQL_Pagination | undefined;
    const params = (this)?.params as Record<string, any>[] | undefined;
    const param_templates = (this)?.param_templates as Record<string, any>[] | undefined;
    try {
        const { output } = await daemon.trx('MODULE').run(async trx => {
            const q = trx.bucket(bucket)
                .query(query)
                .params(params)
                .param_templates(param_templates);
            if (page) return q.page(page).then(res => res.data);
            return q.all();
        });
        const e = expect(output);
    
        e.toHaveLength(ids.length);
        e.toEqual(ids.map(id =>
            expect.objectContaining({ id })
        ));
    }
    catch {
        if (daemon as any) await daemon.destroy();
    }
}) as ExpectIdsFn & {
    withPage: (page: NQL_Pagination) => ExpectIdsFn
    withParams: (params: Record<string, any>[], param_templates?: Record<string, string>[]) => ExpectIdsFn
};
(expectIds as any).withPage = (page: NQL_Pagination) => {
    return (expectIds as any).bind({page});
};
(expectIds as any).withParams = (params: Record<string, any>[], param_templates?: Record<string, string>[]) => {
    return (expectIds as any).bind({params, param_templates});
};

/* Generic Test */

describe('PostgreSQL NQL Runner', () => {

    beforeAll(async () => {
        await setup();
    }, 30000);
    afterAll(async () => {
        await daemon.destroy();
    }, 30000);

    describe('Operators', () => {

        /* == */

        it('Operator: ', async () => {
            await expectIds('shape', { 'id': 1 }, [1]);
            await expectIds('shape', { 'id': 99 }, []);
            await expectIds('shape', { 'name': 'Shape 1' }, [1]);
            await expectIds('shape', { 'name': 'shape 1' }, []);
        });
        it('Operator: not', async () => {
            await expectIds('shape', { 'id not': 1 }, [2,3]);
            await expectIds('shape', { 'id not': 99 }, [1,2,3]);
            await expectIds('shape', { 'name not': 'Shape 1' }, [2,3]);
            await expectIds('shape', { 'name not': 'shape 1' }, [1,2,3]);
        });

        it('Operator: ~', async () => {
            await expectIds('shape', { 'name ~': 'Shape 1' }, [1]);
            await expectIds('shape', { 'name ~': 'shape 1' }, [1]);
            await expectIds('shape', { 'name ~': 'shape 99' }, []);
        });
        it('Operator: not ~', async () => {
            await expectIds('shape', { 'name not ~': 'Shape 1' }, [2,3]);
            await expectIds('shape', { 'name not ~': 'shape 1' }, [2,3]);
            await expectIds('shape', { 'name not ~': 'shape 99' }, [1,2,3]);
        });

        it('Operator: ==', async () => {
            await expectIds('shape', { 'id ==': 1 }, [1]);
            await expectIds('shape', { 'id ==': 99 }, []);
            await expectIds('shape', { 'name ==': 'Shape 1' }, [1]);
            await expectIds('shape', { 'name ==': 'shape 1' }, []);
        });
        it('Operator: not ==', async () => {
            await expectIds('shape', { 'id not ==': 1 }, [2,3]);
            await expectIds('shape', { 'id not ==': 99 }, [1,2,3]);
            await expectIds('shape', { 'name not ==': 'Shape 1' }, [2,3]);
            await expectIds('shape', { 'name not ==': 'shape 1' }, [1,2,3]);
        });

        /* ~== */

        it('Operator: ~==', async () => {
            await expectIds('shape', { 'name ~==': 'Shape 1' }, [1]);
            await expectIds('shape', { 'name ~==': 'shape 1' }, [1]);
            await expectIds('shape', { 'name ~==': 'shape 99' }, []);
        });
        it('Operator: not ~==', async () => {
            await expectIds('shape', { 'name not ~==': 'Shape 1' }, [2,3]);
            await expectIds('shape', { 'name not ~==': 'shape 1' }, [2,3]);
            await expectIds('shape', { 'name not ~==': 'shape 99' }, [1,2,3]);
        });

        //  >, <, >=, <=

        it('Operator: >', async () => {
            await expectIds('shape', { 'size >': 1 }, [1,2,3]);
            await expectIds('shape', { 'size >': 11 }, [2,3]);
            await expectIds('shape', { 'size >': 33 }, []);
        });
        it('Operator: not >', async () => {
            await expectIds('shape', { 'size not >': 1 }, []);
            await expectIds('shape', { 'size not >': 11 }, [1]);
            await expectIds('shape', { 'size not >': 33 }, [1,2,3]);
        });

        it('Operator: <', async () => {
            await expectIds('shape', { 'size <': 44 }, [1,2,3]);
            await expectIds('shape', { 'size <': 33 }, [1,2]);
            await expectIds('shape', { 'size <': 11 }, []);
        });
        it('Operator: not <', async () => {
            await expectIds('shape', { 'size not <': 44 }, []);
            await expectIds('shape', { 'size not <': 33 }, [3]);
            await expectIds('shape', { 'size not <': 11 }, [1,2,3]);
        });

        it('Operator: >=', async () => {
            await expectIds('shape', { 'size >=': 1 }, [1,2,3]);
            await expectIds('shape', { 'size >=': 22 }, [2,3]);
            await expectIds('shape', { 'size >=': 34 }, []);
        });
        it('Operator: not >=', async () => {
            await expectIds('shape', { 'size not >=': 1 }, []);
            await expectIds('shape', { 'size not >=': 22 }, [1]);
            await expectIds('shape', { 'size not >=': 34 }, [1,2,3]);
        });

        it('Operator: <=', async () => {
            await expectIds('shape', { 'size <=': 33 }, [1,2,3]);
            await expectIds('shape', { 'size <=': 22 }, [1,2]);
            await expectIds('shape', { 'size <=': 10 }, []);
        });
        it('Operator: not <=', async () => {
            await expectIds('shape', { 'size not <=': 33 }, []);
            await expectIds('shape', { 'size not <=': 22 }, [3]);
            await expectIds('shape', { 'size not <=': 10 }, [1,2,3]);
        });

        // in

        it('Operator: in', async () => {
            await expectIds('shape', { 'size in': [11,22,33,44] }, [1,2,3]);
            await expectIds('shape', { 'size in': [11,33,44] }, [1,3]);
            await expectIds('shape', { 'size in': [44] }, []);
        });
        it('Operator: not in', async () => {
            await expectIds('shape', { 'size not in': [11,22,33,44] }, []);
            await expectIds('shape', { 'size not in': [11,33,44] }, [2]);
            await expectIds('shape', { 'size not in': [44] }, [1,2,3]);
        });

        // contains

        it('Operator: contains', async () => {
            await expectIds('shape', { 'name contains': 'Shape' }, [1,2,3]);
            await expectIds('shape', { 'name contains': 'shape' }, []);
            await expectIds('shape', { 'name contains': 'ape 2' }, [2]);
            await expectIds('shape', { 'name contains': 'aPe 2' }, []);
            await expectIds('shape', { 'name contains': 'garbage' }, []);
        });
        it('Operator: ~contains', async () => {
            await expectIds('shape', { 'name ~contains': 'Shape' }, [1,2,3]);
            await expectIds('shape', { 'name ~contains': 'shape' }, [1,2,3]);
            await expectIds('shape', { 'name ~contains': 'ape 2' }, [2]);
            await expectIds('shape', { 'name ~contains': 'aPe 2' }, [2]);
            await expectIds('shape', { 'name ~contains': 'gARbAgE' }, []);
        });
        it('Operator: not contains', async () => {
            await expectIds('shape', { 'name not contains': 'Shape' }, []);
            await expectIds('shape', { 'name not contains': 'shape' }, [1,2,3]);
            await expectIds('shape', { 'name not contains': 'ape 2' }, [1,3]);
            await expectIds('shape', { 'name not contains': 'aPe 2' }, [1,2,3]);
            await expectIds('shape', { 'name not contains': 'garbage' }, [1,2,3]);
        });
        it('Operator: not ~contains', async () => {
            await expectIds('shape', { 'name not ~contains': 'Shape' }, []);
            await expectIds('shape', { 'name not ~contains': 'shape' }, []);
            await expectIds('shape', { 'name not ~contains': 'ape 2' }, [1,3]);
            await expectIds('shape', { 'name not ~contains': 'aPe 2' }, [1,3]);
            await expectIds('shape', { 'name not ~contains': 'gARbAgE' }, [1,2,3]);
        });

        // contains_any

        it('Operator: contains_any', async () => {
            await expectIds('shape', { 'name contains_any': ['pe 1', 'e 2', ' 3', 'garbage'] }, [1,2,3]);
            await expectIds('shape', { 'name contains_any': ['Pe 1', 'E 2', ' 3', 'gArBaGe'] }, [3]);
            await expectIds('shape', { 'name contains_any': ['ape 2', 'Shape 1', 'garbage'] }, [1,2]);
            await expectIds('shape', { 'name contains_any': ['Ape 2', 'shape 1', 'garBage'] }, []);
            await expectIds('shape', { 'name contains_any': ['garbage', 'Shape 99'] }, []);            
        });
        it('Operator: ~contains_any', async () => {
            await expectIds('shape', { 'name ~contains_any': ['pe 1', 'e 2', ' 3', 'garbage'] }, [1,2,3]);
            await expectIds('shape', { 'name ~contains_any': ['Pe 1', 'E 2', ' 3', 'gArBaGe'] }, [1,2,3]);
            await expectIds('shape', { 'name ~contains_any': ['ape 2', 'Shape 1', 'garbage'] }, [1,2]);
            await expectIds('shape', { 'name ~contains_any': ['Ape 2', 'shape 1', 'garBage'] }, [1,2]);
            await expectIds('shape', { 'name ~contains_any': ['garbage', 'Shape 99'] }, []);            
        });
        it('Operator: not contains_any', async () => {
            await expectIds('shape', { 'name not contains_any': ['pe 1', 'e 2', ' 3', 'garbage'] }, []);
            await expectIds('shape', { 'name not contains_any': ['Pe 1', 'E 2', ' 3', 'gArBaGe'] }, [1,2]);
            await expectIds('shape', { 'name not contains_any': ['ape 2', 'Shape 1', 'garbage'] }, [3]);
            await expectIds('shape', { 'name not contains_any': ['Ape 2', 'shape 1', 'garBage'] }, [1,2,3]);
            await expectIds('shape', { 'name not contains_any': ['garbage', 'Shape 99'] }, [1,2,3]);
        });
        it('Operator: not ~contains_any', async () => {
            await expectIds('shape', { 'name not ~contains_any': ['pe 1', 'e 2', ' 3', 'garbage'] }, []);
            await expectIds('shape', { 'name not ~contains_any': ['Pe 1', 'E 2', ' 3', 'gArBaGe'] }, []);
            await expectIds('shape', { 'name not ~contains_any': ['ape 2', 'Shape 1', 'garbage'] }, [3]);
            await expectIds('shape', { 'name not ~contains_any': ['Ape 2', 'shape 1', 'garBage'] }, [3]);
            await expectIds('shape', { 'name not ~contains_any': ['garbage', 'Shape 99'] }, [1,2,3]);
        });

        // present

        it('Operator: present', async () => {
            await expectIds('shape', { 'id present': '' }, [1,2,3]);        
            await expectIds('shape', { 'scope present': '' }, [1,2]);        
        });
        it('Operator: not present', async () => {
            await expectIds('shape', { 'id not present': '' }, []);        
            await expectIds('shape', { 'scope not present': '' }, [3]);        
        });
    });

    describe('Boolean Expressions', () => {

        it('A && B', async () => {
            await expectIds('shape', {
                'id': 1,
                'name': 'Shape 1'
            }, [1]);
            await expectIds('shape', {
                'id': 1,
                'name': 'Shape 2'
            }, []);
        });

        it('A || B', async () => {
            await expectIds('shape', {
                'id': 1,
                'or name': 'Shape 1'
            }, [1]);
            await expectIds('shape', {
                'id': 1,
                'or name': 'Shape 2'
            }, [1,2]);
        });

        it('A && B || C', async () => {
            await expectIds('shape', {
                'id': 1,
                'name': 'Shape 1',
                'or size >': 22
            }, [1,3]);
            await expectIds('shape', {
                'id': 1,
                'name': 'Shape 1',
                'or size >': 33
            }, [1]);
        });

        it('A || B && C', async () => {
            await expectIds('shape', {
                'id': 1,
                'or name': 'Shape 2',
                'size >=': 11
            }, [1,2]);
            await expectIds('shape', {
                'id': 2,
                'or name': 'Shape 2',
                'size >=': 11
            }, [2]);
        });

    });

    describe('Sub-Queries', () => {

        it('A -> B (X)', async () => {
            await expectIds('shape', {
                'color_id': {
                    '@color.id': {
                        'name': 'Red'
                    }
                }
            }, [1]);
        });

        it('A -> B (X && Y)', async () => {
            await expectIds('shape', {
                'color_id': {
                    '@color.id': {
                        'id': 1,
                        'name': 'Red'
                    }
                }
            }, [1]);
        });

        it('A -> B (X || Y)', async () => {
            await expectIds('shape', {
                'color_id in': {
                    '@color.id': {
                        'id': 2,
                        'or name': 'Red'
                    }
                }
            }, [1,2]);
        });


        it('A -> B (X) -> C (X)', async () => {
            await expectIds('shape', {
                'color_id': {
                    '@color.id': {
                        'tag': {
                            '@tag.id': {
                                'scope': 'Scope 1'
                            }
                        }
                    },
                }
            }, [1]);
        });

        it('A -> B (X) -> C (X and Y)', async () => {
            await expectIds('shape', {
                'color_id in': {
                    '@color.id': {
                        'tag in': {
                            '@tag.id': {
                                'id': 'Tag 2',
                                'scope': 'Scope 1'
                            }
                        }
                    },
                }
            }, [2]);
        });

        it('A -> B (X) -> C (X or Y)', async () => {
            await expectIds('shape', {
                'color_id in': {
                    '@color.id': {
                        'tag in': {
                            '@tag.id': {
                                'id': 'Tag 2',
                                'or scope': 'Scope 1'
                            }
                        }
                    },
                }
            }, [1, 2]);
        });

        it('A -> B (X) -> C and D (X or Y)', async () => {
            await expectIds('shape', {
                'color_id in': {
                    '@color.id': {
                        'g >': 0,
                        'tag in': {
                            '@tag.id': {
                                'id': 'Tag 2'
                            }
                        }
                    },
                }
            }, [2]);
        });

        it('A -> B (X) -> C or D (X or Y)', async () => {
            await expectIds('shape', {
                'color_id in': {
                    '@color.id': {
                        'b >': 0,
                        'or tag in': {
                            '@tag.id': {
                                'id': 'Tag 2'
                            }
                        }
                    },
                }
            }, [2, 3]);
        });
    });

    describe('Params', () => {

        it('Single Id', async () => {
            await expectIds.withParams([
                { id: 1 }
            ])('shape', {
                'color_id': { '.': 'id' }
            }, [1]);
        });
        
        it('Multiple Ids', async () => {
            await expectIds.withParams([
                { id: 1 },
                { id: 3 },
                { id: 9 },
            ])('shape', {
                'color_id': { '.': 'id' }
            }, [1, 3]);
        });

        it('Single Deep Param', async () => {
            await expectIds.withParams([
                { id: 11, color: { id: 1 } }
            ])('shape', {
                'color_id': { '.': 'color.id' }
            }, [1]);
        });

        it('Multiple Deep Params', async () => {
            await expectIds.withParams([
                { id: 11, color: { id: 1 } },
                { id: 13, color: { id: 3 } },
                { id: 19, color: { id: 9 } },
            ])('shape', {
                'color_id': { '.': 'color.id' }
            }, [1, 3]);
        });

    });

    describe('Param Templates', () => {

        it('Single Param, Single Template', async () => {
            await expectIds.withParams([
                { id: 1, color: { a: 1, b: 2, c: 3 } }
            ], [
                { '$0': 'b' }
            ])('shape', {
                'color_id': { '$': 'color.$0' }
            }, [2]);
        });

        it('Single Param, Multiple Templates', async () => {
            await expectIds.withParams([
                { id: 1, color: { a: 1, b: 2, c: 3 } }
            ], [
                { '$0': 'a' },
                { '$0': 'c' },
                { '$0': 'z' }
            ])('shape', {
                'color_id': { '$': 'color.$0' }
            }, [1, 3]);
        });

        it('Multiple Params, Single Template', async () => {
            await expectIds.withParams([
                { id: 1, color: { a: 2, b: 1, c: 4 } },
                { id: 1, color: { a: 2, b: 3, c: 4 } },
                { id: 1, color: { a: 2, b: 9, c: 4 } },
            ], [
                { '$0': 'b' }
            ])('shape', {
                'color_id': { '$': 'color.$0' }
            }, [1, 3]);
        });

        it('Multiple Params, Multiple Templates', async () => {
            await expectIds.withParams([
                { id: 1, color: { a: 2, b: 1, c: 3 } },
                { id: 1, color: { a: 2, b: 3, c: 9 } },
            ], [
                { '$0': 'b' },
                { '$0': 'c' },
            ])('shape', {
                'color_id': { '$': 'color.$0' }
            }, [1, 3]);
        });
        
    });

    describe('Sorting', () => {

        it('Sort by Id, Asc', async () => {
            await expectIds('shape', {
                '#sort': 'id@asc'
            }, [1,2,3]);
        });

        it('Sort by Id, Asc', async () => {
            await expectIds('shape', {
                '#sort': 'id@desc'
            }, [3,2,1]);
        });

        it('Sort by Deep Prop, Asc', async () => {
            await expectIds('shape', {
                '#sort': 'props.b@asc'
            }, [3,2,1]);
        });

        it('Sort by Deep Prop, Desc', async () => {
            await expectIds('shape', {
                '#sort': 'props.b@desc'
            }, [1,2,3]);
        });

        it('Multi-sort by Deep Prop, Asc', async () => {
            await expectIds('shape', {
                '#sort': ['props.c@asc','props.b@asc']
            }, [2,1,3]);
        });

        it('Multi-sort by Deep Prop, Desc', async () => {
            await expectIds('shape', {
                '#sort': ['props.c@desc','props.b@desc']
            }, [3,1,2]);
        });

        it('Multi-sort by Deep Prop, Desc and Asc', async () => {
            await expectIds('shape', {
                '#sort': ['props.c@desc','props.b@asc']
            }, [3,2,1]);
        });

    });

    describe('Pagination', () => {

        it('Per Page = -1', async () => {
            await expectIds.withPage({
                perPage: -1
            })('shape', {
                '#sort': ['id@desc']
            }, [3, 2, 1]);
        });

        it('Per Page = 0', async () => {
            await expectIds.withPage({
                perPage: 0
            })('shape', {
                '#sort': ['id@desc']
            }, []);
        });

        it('Per Page = 1', async () => {
            await expectIds.withPage({
                perPage: 1
            })('shape', {
                '#sort': ['id@desc']
            }, [3]);
        });

        it('Per Page = 2', async () => {
            await expectIds.withPage({
                perPage: 2
            })('shape', {
                '#sort': ['id@desc']
            }, [3, 2]);
        });

    });
});