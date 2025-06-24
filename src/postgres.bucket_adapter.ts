import { $Bucket } from 'nesoi/lib/elements';
import { BucketAdapter } from 'nesoi/lib/elements/entities/bucket/adapters/bucket_adapter';
import { Log } from 'nesoi/lib/engine/util/log';
import { AnyTrxNode, TrxNode } from 'nesoi/lib/engine/transaction/trx_node';
import postgres from 'postgres';
import { Trx } from 'nesoi/lib/engine/transaction/trx';
import { NQL_QueryMeta } from 'nesoi/lib/elements/entities/bucket/query/nql.schema';
import { PostgresService } from './postgres.service';

export class PostgresBucketAdapter<
    $ extends $Bucket,
    Obj extends $['#data']
> extends BucketAdapter<$['#data']> {


    constructor(
        public schema: $,
        public service: PostgresService,
        public tableName: string
    ) {
        super(schema, service.nql, service.config);
    }

    private guard(sql: postgres.Sql<any>) {
        return (template: TemplateStringsArray, ...params: readonly any[]) => {
            return sql.call(sql, template, ...params).catch((e: unknown) => {
                Log.error('bucket', 'postgres', (e as any).toString(), e as any);
                throw new Error('Database error.');
            }) as unknown as Promise<Obj[]>;
        };
    }

    getQueryMeta() {
        return {
            scope: `PG.${this.service.name}`,
            avgTime: 50
        };
    }

    /* Dangerous, not implemented. */

    protected deleteEverything() {
        throw new Error('Unsafe operation.');
        return Promise.resolve();
    }

    /* Read operations */

    async index(trx: AnyTrxNode) {
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');
        const objs = await this.guard(sql)`
            SELECT *
            FROM ${sql(this.tableName)}
            ORDER BY ${this.config.meta.updated_at} DESC
        `;
        return objs;
    }

    async get(trx: AnyTrxNode, id: Obj['id']) {
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');
        const objs = await this.guard(sql)`
            SELECT *
            FROM ${sql(this.tableName)}
            WHERE id = ${ id }
        `;
        return objs[0];
    }

    /* Write Operations */

    private precleanup(obj: Record<string, any>) {
        obj[this.config.meta.created_by] ??= null;
        obj[this.config.meta.updated_by] ??= null;

        for (const key in obj) {
            if (obj[key] === undefined) {
                // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
                delete obj[key];
            }
        }
    }

    async create(
        trx: AnyTrxNode,
        obj: Record<string, any>
    ) {
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');

        // Use schema fields excluding id
        const keys = Object.keys(this.schema.model.fields)
            .filter(key => obj[key] !== undefined);
        
        // Add meta (created_*/updated_*)
        keys.push(...Object.values(this.config.meta));
        
        this.precleanup(obj);

        // Create
        const objs = await this.guard(sql)`
            INSERT INTO ${sql(this.tableName)}
            ${ sql(obj, keys) }
            RETURNING *`;

        return objs[0];
    }

    async createMany(
        trx: AnyTrxNode,
        objs: Record<string, any>[]
    ) {
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');

        // Use schema fields excluding id
        const keys = Object.keys(this.schema.model.fields)
            .filter(key => key !== 'id');

        // Add meta (created_*/updated_*)
        keys.push(...Object.values(this.config.meta));
        
        // Pre-cleanup
        for (const obj of objs) {
            this.precleanup(obj);
        }

        // Create
        const inserted = await this.guard(sql)`
            INSERT INTO ${sql(this.tableName)}
            ${ sql(objs as Record<string, any>, keys) }
            RETURNING *
        `;

        return inserted;
    }

    async patch(
        trx: AnyTrxNode,
        obj: Record<string, any>
    ) {
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');

        // Use schema keys that exist on object
        const keys = Object.keys(this.schema.model.fields)
            .filter(key => obj[key] !== undefined)
            .filter(key => key in obj);

        // Add meta
        keys.push(this.config.meta.updated_by, this.config.meta.updated_at);

        // Pre-cleanup
        this.precleanup(obj);
            
        // Update
        const objs = await this.guard(sql)`
            UPDATE ${sql(this.tableName)} SET
            ${ sql(obj, keys) }
            WHERE id = ${ obj.id }
            RETURNING *
        `;
        return objs[0];
    }

    async patchMany(
        trx: AnyTrxNode,
        objs: Record<string, any>[]
    ) {
        const _objs: $['#data'][] = [];
        for (const obj of objs) {
            _objs.push(await this.patch(trx, obj));
        }
        return _objs;
    }

    async replace(
        trx: AnyTrxNode,
        obj: Record<string, any>
    ) {
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');

        // Use all schema keys
        const keys = Object.keys(this.schema.model.fields)
            .filter(key => obj[key] !== undefined);

        keys.push(this.config.meta.updated_by, this.config.meta.updated_at);
        this.precleanup(obj);
            
        const objs = await this.guard(sql)`
            UPDATE ${sql(this.tableName)} SET
            ${ sql(obj, keys) }
            WHERE id = ${ obj.id }
            RETURNING *
        `;
        return objs[0];
    }

    async replaceMany(
        trx: AnyTrxNode,
        objs: Record<string, any>[]
    ) {
        const _objs: $['#data'][] = [];
        for (const obj of objs) {
            _objs.push(await this.replace(trx, obj));
        }
        return _objs;
    }

    async put(
        trx: AnyTrxNode,
        obj: Record<string, any>
    ) {
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');

        // Use all schema keys
        const keys = Object.keys(this.schema.model.fields)
            .filter(key => obj[key] !== undefined);

        // Add meta (created_*/updated_*)
        const ikeys = keys.concat(...Object.values(this.config.meta));
        const ukeys = keys.concat(this.config.meta.updated_by, this.config.meta.updated_at);
        
        this.precleanup(obj);

        const objs = await this.guard(sql)`
            INSERT INTO ${sql(this.tableName)}
            ${ sql(obj, ikeys) }
            ON CONFLICT(id)
            DO UPDATE SET
            ${ sql(obj, ukeys) }
            RETURNING *
        `;
        return objs[0];
    }

    async putMany(
        trx: AnyTrxNode,
        objs: Record<string, any>[]
    ) {
        const _objs: $['#data'][] = [];
        for (const obj of objs) {
            _objs.push(await this.put(trx, obj));
        }
        return _objs;
    }

    async delete(
        trx: AnyTrxNode,
        id: Obj['id']
    ) {
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');
        await this.guard(sql)`
            DELETE FROM ${sql(this.tableName)}
            WHERE id = ${ id }
        `;
    }

    async deleteMany(
        trx: AnyTrxNode,
        ids: Obj['id'][]
    ) {
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');
        await this.guard(sql)`
            DELETE FROM ${sql(this.tableName)}
            WHERE id IN ${ ids }
        `;
    }

    /* Cache Operations */

    syncOne(
    // async syncOne(
        // trx: AnyTrxNode,
        // id: Obj['id'],
        // lastObjUpdateEpoch: number
    ) {
        throw new Error('Not implemented yet.');
        return {} as any;
        // // 1. Check if object was deleted
        // const obj = await this.get(trx, id);
        // if (!obj) {
        //     return 'deleted' as const;
        // }

        // // 2. Check if object was updated
        // const updateEpoch = this.getUpdateEpoch(obj);

        // const hasObjUpdated = updateEpoch > lastObjUpdateEpoch;
        // if (!hasObjUpdated) {
        //     return null;
        // }

        // // 3. Return updated object and epoch
        // return {
        //     obj,
        //     updateEpoch
        // };
    }

    syncOneAndPast(
    // async syncOneAndPast(
    //     trx: AnyTrxNode,
    //     id: Obj['id'],
    //     lastUpdateEpoch: number
    ) {
        throw new Error('Not implemented yet.');
        return {} as any;
        // // 1. Check if object was deleted
        // const obj = await this.get(trx, id);
        // if (!obj) {
        //     return 'deleted' as const;
        // }

        // // 2. Check if object was updated
        // const objUpdateEpoch = this.getUpdateEpoch(obj);
        // const hasObjUpdated = objUpdateEpoch > lastUpdateEpoch;       
        // if (!hasObjUpdated) {
        //     return null;
        // }

        // // 3. Return all objects updated and the max epoch
        // let updateEpoch = 0;
        // const changed = (Object.values(this.data) as Obj[])
        //     .map(obj => {
        //         const epoch = this.getUpdateEpoch(obj);
        //         if (epoch > updateEpoch) {
        //             updateEpoch = epoch;
        //         }
        //         return { obj, updateEpoch: epoch };
        //     })
        //     .filter(obj => obj.updateEpoch > lastUpdateEpoch);

        // if (!changed.length) {
        //     return null;
        // }

        // return changed;
    }

    syncAll(
    // async syncAll(
    //     trx: AnyTrxNode,
    //     lastHash?: string,
    //     lastUpdateEpoch = 0
    ) {
        throw new Error('Not implemented yet.');
        return {} as any;
        // // 1. Hash the current ids
        // const idStr = Object.keys(this.data).sort().join('');
        // const hash = createHash('md5').update(idStr).digest('hex');
        

        // // 2. If hash changed, return a reset sync with all objects
        // if (hash !== lastHash) {
        //     let updateEpoch = 0;
        //     const sync = (await this.index(trx) as Obj[])
        //         .map(obj => {
        //             const epoch = this.getUpdateEpoch(obj);
        //             if (epoch > updateEpoch) {
        //                 updateEpoch = epoch;
        //             }
        //             return { obj, updateEpoch: epoch };
        //         });
        //     return {
        //         sync,
        //         hash,
        //         updateEpoch,
        //         reset: true
        //     };
        // }

        // // 3. Find the data that changed and return it
        // let updateEpoch = 0;
        // const sync = (Object.values(this.data) as Obj[])
        //     .map(obj => {
        //         const epoch = this.getUpdateEpoch(obj);
        //         if (epoch > updateEpoch) {
        //             updateEpoch = epoch;
        //         }
        //         return { obj, updateEpoch: epoch };
        //     })
        //     .filter(obj => obj.updateEpoch > lastUpdateEpoch);
        
        // if (!sync.length) {
        //     return null;
        // }

        // return {
        //     sync,
        //     hash,
        //     updateEpoch,
        //     reset: false
        // };
    }

    public static getTableMeta(trx: AnyTrxNode, meta: NQL_QueryMeta) {
        const schema = meta.bucket as $Bucket;
        const trxModule = TrxNode.getModule(trx);
        const bucketName = schema.name;
        const refName = (trxModule.name === schema.module ? '' : `${schema.module}::`) + bucketName;
        const bucket = trxModule.buckets[refName];
        const adapter = bucket.adapter as PostgresBucketAdapter<any, any>;
        
        return {
            tableName: adapter.tableName,
            serviceName: adapter.service.name,
            meta: adapter.config.meta
        };
    }
    

}