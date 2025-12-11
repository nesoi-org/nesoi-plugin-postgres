/* eslint-disable @typescript-eslint/no-deprecated */
import { AnyTrxNode, TrxNode } from 'nesoi/lib/engine/transaction/trx_node';
import { NQLRunner } from 'nesoi/lib/elements/entities/bucket/query/nql_engine';
import { NQL_Intersection, NQL_Pagination, NQL_Part, NQL_Rule, NQL_Union } from 'nesoi/lib/elements/entities/bucket/query/nql.schema';
import postgres from 'postgres';
import { Trx } from 'nesoi/lib/engine/transaction/trx';
import { Tree } from 'nesoi/lib/engine/data/tree';
import { PostgresBucketAdapter } from './postgres.bucket_adapter';
import { Log } from 'nesoi/lib/engine/util/log';
import { BucketModel } from 'nesoi/lib/elements/entities/bucket/model/bucket_model';

type Obj = Record<string, any>

export class PostgresNQLRunner extends NQLRunner {
    
    static fieldpathToColumn(fieldpath?: string) {
        if (!fieldpath) return undefined;
        let column = fieldpath.replace(/\.(\w+)$/, '->>\'$1\'');
        column = column.replace(/\.(\w+)/g, '->\'$1\'');
        if (!column.includes('->>')) {
            column = `"${column}"`;
        }
        return column;
    }

    async run(trx: AnyTrxNode, part: NQL_Part, params: Obj[], param_templates: Record<string, string>[], pagination?: NQL_Pagination, view?: any, serialize?: boolean) {
        const { tableName, serviceName } = PostgresBucketAdapter.getTableMeta(trx, part.union.meta);
        const sql = Trx.get<postgres.Sql<any>|undefined>(trx, serviceName+'.sql');
        if (!sql) {
            throw new Error(`Unable to find sql runner for PostgresService '${serviceName}' at module '${TrxNode.getModule(trx).name}'. Did you configure 'trx.wrap' for this module on the app?`);
        }

        const sql_params: any[] = [];

        const _param = (value: number | string | boolean) => {
            const i = sql_params.findIndex(v => v === value);
            if (i < 0) {
                sql_params.push(value);
                return `$${sql_params.length}`;
            }
            return `$${i+1}`;
        };
        const _union = (union: NQL_Union, params: Obj, param_template: Record<string, string>): string => {
            const inters = union.inters.map(
                i => _inter(i, params, param_template)
            ).filter(r => !!r).join(' OR ');
            if (!inters) return '';
            return `(${inters})`;
        };
        const _inter = (inter: NQL_Intersection, params: Obj, param_template: Record<string, string>): string => {
            const rules = inter.rules.map(
                r => (('value' in r) ? _rule(r, params, param_template) : _union(r, params, param_template))
            ).filter(r => !!r).join(' AND ');
            if (!rules) return '';
            return `(${rules})`;
        };
        const _rule = (rule: NQL_Rule, params: Obj, param_template: Record<string, string>): string => {

            // Replace '.' of fieldpath with '->' (JSONB compatible)
            let column = PostgresNQLRunner.fieldpathToColumn(rule.fieldpath)!;
            
            // TODO: handle '.#'

            if (rule.op === 'contains') {
                column = `${column}::text`;
            }

            // Special case: "present" operation
            if (rule.op === 'present') {
                if (rule.not) {
                    return `${column} IS NULL`;
                }
                else {
                    return `${column} IS NOT NULL`;
                }
            }

            if (rule.op === 'contains_any') {
                throw new Error('Operator \'contains_any\' currently not supported on SQL adapters.');
            }
            
            // Translate operation
            let op = {
                '==': '=',
                '<': '<',
                '>': '>',
                '<=': '<=',
                '>=': '>=',
                'in': 'IN',
                'contains': 'LIKE',
                'contains_any': '' // TODO
            }[rule.op];

            // Apply case insensitive modifier
            if (rule.case_i) {
                if (rule.op === '==') {
                    column = `LOWER(${column})`;
                }
                else if (rule.op === 'contains') {
                    op = 'ILIKE';
                }
            }

            // Fetch value
            let queryValue;
            if ('static' in rule.value) {
                queryValue = rule.value.static;
                if (rule.case_i) queryValue = (queryValue as string).toLowerCase();
            }
            else if ('param' in rule.value) {
                if (Array.isArray(rule.value.param)) {
                    queryValue = rule.value.param.map(p => Tree.get(params, p));
                }
                else {
                    queryValue = Tree.get(params, rule.value.param);
                }
                if (rule.case_i) queryValue = (queryValue as string).toLowerCase();
            }
            else if ('param_with_$' in rule.value) {
                let path = rule.value.param_with_$;
                for (const key in param_template) {
                    path = path.replace(new RegExp(key.replace('$','\\$'), 'g'), param_template[key]);
                }
                queryValue = Tree.get(params, path);
                if (rule.case_i) queryValue = (queryValue as string).toLowerCase();
            }
            // subquery
            else {
                const bucket = rule.value.subquery.bucket;
                const select = rule.value.subquery.select;
                const union = rule.value.subquery.union;
                const { tableName } = PostgresBucketAdapter.getTableMeta(trx, { schema: bucket } as any);

                queryValue = `SELECT ${select} FROM ${tableName} WHERE ${_union(union, params, param_template)}`;
                if (rule.case_i) queryValue = `LOWER(${queryValue})`;
                return `${rule.not ? 'NOT ' : ''} ${column} ${op} (${queryValue})`;
            }

            // Don't add condition if value is null
            if (queryValue === undefined) { return ''; }

            // Special case: "contains" operation
            if (rule.op === 'contains') {
                queryValue = `%${queryValue}%`;
            }

            let p;
            if (Array.isArray(queryValue)) {
                if (queryValue.length === 0) return rule.not ? 'TRUE' : 'FALSE';

                p = queryValue.map(v => _param(v)).join(',');
            }
            else {
                p = _param(queryValue);

            }
            return `${rule.not ? 'NOT ' : ''} ${column} ${op} (${p})`;
        };

        // Debug
        // const str = await _sql(part).describe().catch(e => {
        //     Log.error('postgres' as any, 'nql', e.query, e);
        // })
        // console.log((str as any).string);
        // End of Debug

        const param_ids = new Set<string>();
        const wheres = new Set<string>();
        for (let i = 0; i < params.length; i++) {
            const param = params[i];
            if ('id' in param) {
                if (param_ids.has(param.id)) continue;
                param_ids.add(param.id);
            }
            const param_template = param_templates[i];
            const where = _union(part.union, param, param_template);
            if (where) {
                wheres.add(where);
            }
        }
        const where = wheres.size ? `WHERE ${[...wheres].join(' OR ')}` : '';
        const sql_str = `FROM ${tableName} ${where}`;

        const sort = part.union.sort;
        let order_str = '';
        if (sort?.length) {
            order_str = 'ORDER BY ' + sort.map(s =>
                `${PostgresNQLRunner.fieldpathToColumn(s.key)} ${s.dir === 'asc' ? 'ASC' : 'DESC'}`
            ).join(',');
        }
        
        let limit_str = '';
        if (pagination?.page !== undefined || pagination?.perPage !== undefined) {
            const limit = pagination.perPage ?? 10;
            const offset = ((pagination.page || 1)-1)*limit;
            limit_str = `OFFSET ${offset} LIMIT ${limit}`;
        }

        let count: number|undefined = undefined;
        if (pagination?.returnTotal) {
            const res_count = await sql.unsafe(`SELECT count(*) ${sql_str}`, sql_params);
            count = parseInt(res_count[0].count);
        }

        const select = part.select ?? '*';

        let data = await sql.unsafe(`SELECT ${select} ${sql_str} ${order_str} ${limit_str}`, sql_params).catch((e: unknown) => {
            Log.error('bucket', 'postgres', (e as any).toString(), e as any);
            throw new Error('Database error.');
        }) as Obj[];
        
        if (part.select) {
            data = data.map(obj => obj[part.select!]);
        }
        else if (serialize) {
            const model = new BucketModel(part.union.meta.schema!);
            data = data.map(obj => model.copy(obj, 'load', () => true));
        }

        return {
            data,
            totalItems: count,
            page: pagination?.page,
            perPage: pagination?.perPage,
        };
    }

}