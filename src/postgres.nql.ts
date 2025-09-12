import { AnyTrxNode } from 'nesoi/lib/engine/transaction/trx_node';
import { NQLRunner } from 'nesoi/lib/elements/entities/bucket/query/nql_engine';
import { NQL_Intersection, NQL_Pagination, NQL_Part, NQL_Rule, NQL_Union } from 'nesoi/lib/elements/entities/bucket/query/nql.schema';
import postgres from 'postgres';
import { Trx } from 'nesoi/lib/engine/transaction/trx';
import { PostgresBucketAdapter } from './postgres.bucket_adapter';
import { Log } from 'nesoi/lib/engine/util/log';
import { $BucketView } from 'nesoi/lib/elements/entities/bucket/view/bucket_view.schema';

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

    async run(trx: AnyTrxNode, part: NQL_Part, params: Obj[], pagination?: NQL_Pagination, view?: $BucketView) {
        const { tableName, serviceName, meta } = PostgresBucketAdapter.getTableMeta(trx, part.union.meta);
        const sql = Trx.get<postgres.Sql<any>>(trx, serviceName+'.sql');

        const sql_params: any[] = [];

        const _union = (union: NQL_Union, params: Obj): string => {
            const inters = union.inters.map(
                i => _inter(i, params)
            ).filter(r => !!r).join(' OR ');
            if (!inters) return '';
            return `(${inters})`;
        };
        const _inter = (inter: NQL_Intersection, params: Obj): string => {
            const rules = inter.rules.map(
                r => (('value' in r) ? _rule(r, params) : _union(r, params))
            ).filter(r => !!r).join(' AND ');
            if (!rules) return '';
            return `(${rules})`;
        };
        const _rule = (rule: NQL_Rule, params: Obj): string => {

            // Replace '.' of fieldpath with '->' (JSONB compatible)
            let column = PostgresNQLRunner.fieldpathToColumn(rule.fieldpath)!;
            
            // TODO: handle '.#'

            // Special case: "present" operation
            if (rule.op === 'present') {
                if (rule.not) {
                    return `${column} IS NULL`;
                }
                else {
                    return `${column} IS NOT NULL`;
                }
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
            let value;
            if ('static' in rule.value) {
                value = rule.value.static;
            }
            else if ('param' in rule.value) {
                value = params[rule.value.param as string]; // TODO: deal with param[]
            }
            else {
                const bucket = rule.value.subquery.bucket;
                const select = rule.value.subquery.select;
                const union = rule.value.subquery.union;
                const { tableName } = PostgresBucketAdapter.getTableMeta(trx, { bucket} as any);

                value = `SELECT ${select} FROM ${tableName} WHERE ${_union(union, params)}`;
                return `${rule.not ? 'NOT ' : ''} ${column} ${op} (${value})`;
            }

            // Don't add condition if value is null
            if (value === undefined) { return ''; }

            // Special case: "contains" operation
            if (rule.op === 'contains') {
                value = `%${value}%`;
            }

            let p;
            if (Array.isArray(value)) {
                p = Array.from({ length: value.length }).map((_, i) => 
                    `$${i + sql_params.length + 1}`
                ).join(',');
                sql_params.push(...value);
            }
            else {
                sql_params.push(value);
                p = `$${sql_params.length}`;

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
        const wheres: string[] = [];
        for (const paramGroup of params) {
            if ('id' in paramGroup) {
                if (param_ids.has(paramGroup.id)) continue;
                param_ids.add(paramGroup.id);
            }
            const where = _union(part.union, paramGroup);
            if (where) {
                wheres.push(where);
            }
        }
        const where = wheres.length ? `WHERE ${wheres.join(' OR ')}` : '';
        const sql_str = `FROM ${tableName} ${where}`;

        const order = part.union.order;
        const order_by = PostgresNQLRunner.fieldpathToColumn(order?.by[0]);
        const order_str = `ORDER BY ${order_by || meta.updated_at} ${order?.dir[0] === 'asc' ? 'ASC' : 'DESC'}`;

        let limit_str = '';
        if (pagination?.page || pagination?.perPage) {
            const limit = pagination.perPage || 10;
            const offset = ((pagination.page || 1)-1)*limit;
            limit_str = `OFFSET ${offset} LIMIT ${limit}`;
        }

        let count: number|undefined = undefined;
        if (pagination?.returnTotal) {
            const res_count = await sql.unsafe(`SELECT count(*) ${sql_str}`, sql_params);
            count = parseInt(res_count[0].count);
        }

        const viewFields = view
            ? Object.entries(view.fields)
                .filter(e => e[1].scope === 'model')
                .map(e => e[0])
            : '*';

        const data = await sql.unsafe(`SELECT ${viewFields} ${sql_str} ${order_str} ${limit_str}`, sql_params).catch((e: unknown) => {
            Log.error('bucket', 'postgres', (e as any).toString(), e as any);
            throw new Error('Database error.');
        }) as Obj[];
        
        return {
            data,
            totalItems: count,
            page: pagination?.page,
            perPage: pagination?.perPage,
        };
    }

}