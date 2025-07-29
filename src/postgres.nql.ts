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
    
    async run(trx: AnyTrxNode, part: NQL_Part, params: Obj, pagination?: NQL_Pagination, view?: $BucketView) {
        const { tableName, serviceName, meta } = PostgresBucketAdapter.getTableMeta(trx, part.union.meta);
        const sql = Trx.get<postgres.Sql<any>>(trx, serviceName+'.sql');

        const sql_params: any[] = [];

        const _sql = (part: NQL_Part) => {
            let where = _union(part.union);
            if (where) {
                where = 'WHERE ' + where;
            }
            const sql_str = `FROM ${tableName} ${where}`;
            return sql_str;
        };
        const _union = (union: NQL_Union): string => {
            const inters = union.inters.map(
                i => _inter(i)
            ).filter(r => !!r).join(' OR ');
            if (!inters) return '';
            return `(${inters})`;
        };
        const _inter = (inter: NQL_Intersection): string => {
            const rules = inter.rules.map(
                r => (('value' in r) ? _rule(r) : _union(r))
            ).filter(r => !!r).join(' AND ');
            if (!rules) return '';
            return `(${rules})`;
        };
        const _rule = (rule: NQL_Rule): string => {

            // Replace '.' of fieldpath with '->' (JSONB compatible)
            let column = rule.fieldpath.replace(/\./g, '->');

            // TODO: handle '.#'

            // Special case: "present" operation
            if (rule.op === 'present') {
                if (rule.not) {
                    return `"${column}" IS NULL`;
                }
                else {
                    return `"${column}" IS NOT NULL`;
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

                value = `SELECT ${select} FROM ${tableName} WHERE ${_union(union)}`;
                return `${rule.not ? 'NOT ' : ''} "${column}" ${op} (${value})`;
            }

            // Don't add condition if value is null
            if (value === undefined) { return ''; }

            // Special case: "contains" operation
            if (rule.op === 'contains') {
                value = `%${value}%`;
            }

            sql_params.push(value);
            return `${rule.not ? 'NOT ' : ''} "${column}" ${op} ($${sql_params.length})`;
        };

        // Debug
        // const str = await _sql(part).describe().catch(e => {
        //     Log.error('postgres' as any, 'nql', e.query, e);
        // })
        // console.log((str as any).string);
        // End of Debug

        const sql_str = _sql(part);


        const order = part.union.order;
        const order_str = `ORDER BY ${order?.by[0] || meta.updated_at} ${order?.dir[0] === 'asc' ? 'ASC' : 'DESC'}`;

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