import { $Bucket } from 'nesoi/lib/elements';
import { BucketAdapter } from 'nesoi/lib/elements/entities/bucket/adapters/bucket_adapter';
import { Log } from 'nesoi/lib/engine/util/log';
import { AnyTrxNode, TrxNode } from 'nesoi/lib/engine/transaction/trx_node';
import postgres from 'postgres';
import { Trx } from 'nesoi/lib/engine/transaction/trx';
import { NQL_QueryMeta } from 'nesoi/lib/elements/entities/bucket/query/nql.schema';
import { PostgresService } from './postgres.service';
import { NesoiDatetime } from 'nesoi/lib/engine/data/datetime';
import { BucketCacheSync } from 'nesoi/lib/elements/entities/bucket/cache/bucket_cache';
import { BucketModel } from 'nesoi/lib/elements/entities/bucket/model/bucket_model';

export class PostgresBucketAdapter<
    $ extends $Bucket,
    Obj extends $['#data']
> extends BucketAdapter<$['#data']> {

    protected model: BucketModel<any, $>;

    constructor(
        public schema: $,
        public service: PostgresService<string>,
        public tableName: string
    ) {
        super(schema, service.nql, service.config);
        this.model = new BucketModel(schema, service.config);
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

    private static serialize_rule_save = (field?: { depth: number }) => !!field && field.depth > 0;

    async index(trx: AnyTrxNode, serialize?: boolean) {
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');
        const objs = await this.guard(sql)`
            SELECT *
            FROM ${sql(this.tableName)}
            ORDER BY ${this.config.meta.updated_at} DESC
        `;
        const parsed: any[] = [];
        for (const obj of objs) {
            parsed.push(this.model.copy(obj, 'load', () => !!serialize));
        }
        return objs;
    }

    async get(trx: AnyTrxNode, id: Obj['id'], serialize?: boolean): Promise<undefined | Obj> {
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');
        const objs = await this.guard(sql)`
            SELECT *
            FROM ${sql(this.tableName)}
            WHERE id = ${ id }
        `;
        if (!objs[0]) return undefined;
        return this.model.copy(objs[0], 'load', () => !!serialize);
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
        const input = this.model.copy(obj, 'save', PostgresBucketAdapter.serialize_rule_save);

        // Use schema fields as keys
        const keys = Object.keys(this.schema.model.fields)
            .filter(key => input[key] !== undefined);
        
        // Add meta (created_*/updated_*)
        keys.push(...Object.values(this.config.meta));
        
        this.precleanup(input);

        // Create
        const objs = await this.guard(sql)`
            INSERT INTO ${sql(this.tableName)}
            ${ sql(input, keys) }
            RETURNING *`;

        return this.model.copy(objs[0], 'load');
    }

    async createMany(
        trx: AnyTrxNode,
        objs: Record<string, any>[]
    ) {
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');

        // Use schema fields as keys
        const keys = Object.keys(this.schema.model.fields);

        // Add meta (created_*/updated_*)
        keys.push(...Object.values(this.config.meta));
        
        const inputs: Record<string, any>[] = [];
        for (const obj of objs) {
            inputs.push(this.model.copy(obj, 'save'), PostgresBucketAdapter.serialize_rule_save);
        }

        // Pre-cleanup
        for (const input of inputs) {
            this.precleanup(input);
        }

        // Create
        const inserted = await this.guard(sql)`
            INSERT INTO ${sql(this.tableName)}
            ${ sql(inputs as Record<string, any>, keys) }
            RETURNING *
        `;

        const parsed: any[] = [];
        for (const obj of inserted) {
            parsed.push(this.model.copy(obj, 'load'));
        }

        return parsed;
    }

    async patch(
        trx: AnyTrxNode,
        obj: Record<string, any>
    ) {
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');
        const input = this.model.copy(obj, 'save', PostgresBucketAdapter.serialize_rule_save, Object.keys(obj));

        // Use schema keys that exist on object
        const keys = Object.keys(this.schema.model.fields)
            .filter(key => input[key] !== undefined);

        // Add meta
        keys.push(this.config.meta.updated_by, this.config.meta.updated_at);

        // Pre-cleanup
        this.precleanup(input);
            
        // Update
        const objs = await this.guard(sql)`
            UPDATE ${sql(this.tableName)} SET
            ${ sql(input, keys) }
            WHERE id = ${ input.id }
            RETURNING *
        `;
        return this.model.copy(objs[0], 'load');
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
        const input = this.model.copy(obj, 'save', PostgresBucketAdapter.serialize_rule_save);

        // Use all schema keys
        const keys = Object.keys(this.schema.model.fields)
            .filter(key => input[key] !== undefined);

        keys.push(this.config.meta.updated_by, this.config.meta.updated_at);
        this.precleanup(input);
            
        const objs = await this.guard(sql)`
            UPDATE ${sql(this.tableName)} SET
            ${ sql(input, keys) }
            WHERE id = ${ input.id }
            RETURNING *
        `;

        return this.model.copy(objs[0], 'load');
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
        const input = this.model.copy(obj, 'save', PostgresBucketAdapter.serialize_rule_save);

        // Use all schema keys
        const keys = Object.keys(this.schema.model.fields)
            .filter(key => input[key] !== undefined);

        // Add meta (created_*/updated_*)
        const ikeys = keys.concat(...Object.values(this.config.meta));
        const ukeys = keys.concat(this.config.meta.updated_by, this.config.meta.updated_at);
        
        this.precleanup(input);

        const objs = await this.guard(sql)`
            INSERT INTO ${sql(this.tableName)}
            ${ sql(input, ikeys) }
            ON CONFLICT(id)
            DO UPDATE SET
            ${ sql(input, ukeys) }
            RETURNING *
        `;
        return this.model.copy(objs[0], 'load');
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
        if (!ids.length) return;
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');
        await this.guard(sql)`
            DELETE FROM ${sql(this.tableName)}
            WHERE id IN ${sql(ids as (number|string)[])}
        `;
    }

    /* Cache Operations */

    /**
     * Given an id, sync that object only.
     * - If the id doesn't exist on the source, return 'deleted'
     * - If it does, check if it changed since lastObjUpdateEpoch
     *      - If yes, return the updated object
     *      - If not, return null
     * @returns One of the below:
     *  - `null`: Object hasn't changed 
     *  - `Obj`: Object has changed
     *  - `deleted`: Object was deleted
     */
    async syncOne(
        trx: AnyTrxNode,
        id: Obj['id'],
        lastObjUpdateEpoch: number
    ) {
        
        // 1. Check if object was deleted
        const obj = await this.get(trx, id);
        if (!obj) {
            return 'deleted' as const;
        }

        // // 2. Check if object was updated
        const updateEpoch = this.getUpdateEpoch(obj);

        const hasObjUpdated = updateEpoch > lastObjUpdateEpoch;
        if (!hasObjUpdated) {
            return null;
        }

        // 3. Return updated object and epoch
        return {
            obj,
            updateEpoch
        };
    }

    /**
     * Given an id, if the object was not deleted and has changed on source,
     * sync the object and all objects of this bucket updated before it.
     * @returns One of the below:
     *  - `null`: Object hasn't changed 
     *  - `Obj[]`: Object or past objects changed
     *  - `deleted`: Object was deleted
     */
    async syncOneAndPast(
        trx: AnyTrxNode,
        id: Obj['id'],
        lastUpdateEpoch: number
    ): Promise<null|'deleted'|BucketCacheSync<Obj>[]>  {
        // 1. Check if object was deleted
        const obj = await this.get(trx, id);
        if (!obj) {
            return 'deleted' as const;
        }

        // 2. Check if object was updated
        const objUpdateEpoch = this.getUpdateEpoch(obj);
        const hasObjUpdated = objUpdateEpoch > lastUpdateEpoch;       
        if (!hasObjUpdated) {
            return null;
        }

        // 3. Return all objects updated
        const changed = await this.query(trx, {
            'updated_at >': new NesoiDatetime(lastUpdateEpoch).toISO()
        });

        if (!changed.data.length) {
            return null;
        }

        return changed.data.map(obj => ({
            obj: obj as Obj,
            updateEpoch: NesoiDatetime.fromISO((obj as any).updated_at).epoch
        }));
    }

    /**
     * Resync the entire cache.
     * - Hash the ids, check if it matches the incoming hash
     *   - If yes, read all data that changed since last time
     *   - If not, read all data and return a hard resync (previous data will be wiped)
     @returns One of the below:
     *  - `null`: Cache hasn't changed
     *  - `{ data: Obj[], hash: string, hard: true }`: Cache has changed
     */
    async syncAll(
        trx: AnyTrxNode,
        lastHash?: string,
        lastUpdateEpoch = 0
    ): Promise<null|{
        sync: BucketCacheSync<Obj>[],
        hash: string,
        updateEpoch: number,
        reset: boolean
    }> {
        // 1. Hash the current ids
        const sql = Trx.get<postgres.Sql<any>>(trx, this.service.name+'.sql');
        const results = await this.guard(sql)`SELECT md5(CAST((array_agg(id ORDER BY id)) AS TEXT)) as hash FROM ${sql(this.tableName)}`;
        const hash = (results[0] as any).hash;

        // 2. If hash changed, return a reset sync with all objects
        if (hash !== lastHash) {
            let updateEpoch = 0;
            const sync = (await this.index(trx))
                .map(obj => {
                    const epoch = this.getUpdateEpoch(obj);
                    if (epoch > updateEpoch) {
                        updateEpoch = epoch;
                    }
                    return { obj, updateEpoch: epoch };
                });
            return {
                sync,
                hash,
                updateEpoch,
                reset: true
            };
        }

        // 3. Find the data that changed and return it
        const changed = await this.query(trx, {
            'updated_at >': new NesoiDatetime(lastUpdateEpoch).toISO(),
            '#sort': 'updated_at@desc'
        });
        const updateEpoch = (changed.data[0] as any)?.updated_at || 0;
        
        if (!changed.data.length) {
            return null;
        }

        const sync = changed.data.map(obj => ({
            obj: obj as Obj,
            updateEpoch: NesoiDatetime.fromISO((obj as any).updated_at).epoch
        }));

        return {
            sync,
            hash,
            updateEpoch,
            reset: false
        };
    }

    public static getTableMeta(trx: AnyTrxNode, meta: NQL_QueryMeta) {
        const schema = meta.schema as $Bucket;
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