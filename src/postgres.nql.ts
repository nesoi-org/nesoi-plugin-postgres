import { AnyTrxNode } from 'nesoi/lib/engine/transaction/trx_node';
import { NQLRunner } from 'nesoi/lib/elements/entities/bucket/query/nql_engine';
import { NQL_Intersection, NQL_Pagination, NQL_Part, NQL_Rule, NQL_Union } from 'nesoi/lib/elements/entities/bucket/query/nql.schema';
import postgres from 'postgres';
import { Trx } from 'nesoi/lib/engine/transaction/trx';
import { Tree } from 'nesoi/lib/engine/data/tree';
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

    async run(trx: AnyTrxNode, part: NQL_Part, params: Obj[], param_templates: Record<string, string>[], pagination?: NQL_Pagination, view?: $BucketView) {
        const { tableName, serviceName, meta } = PostgresBucketAdapter.getTableMeta(trx, part.union.meta);
        const sql = Trx.get<postgres.Sql<any>>(trx, serviceName+'.sql');

        const sql_params: any[] = [];

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
            let queryValue;
            if ('static' in rule.value) {
                queryValue = rule.value.static;
            }
            else if ('param' in rule.value) {
                if (Array.isArray(rule.value.param)) {
                    // eslint-disable-next-line @typescript-eslint/no-deprecated
                    queryValue = rule.value.param.map(p => Tree.get(params, p));
                }
                else {
                    // eslint-disable-next-line @typescript-eslint/no-deprecated
                    queryValue = Tree.get(params, rule.value.param);
                }
            }
            else if ('param_with_$' in rule.value) {
                let path = rule.value.param_with_$;
                for (const key in param_template) {
                    path = path.replace(new RegExp(key.replace('$','\\$'), 'g'), param_template[key]);
                }
                // eslint-disable-next-line @typescript-eslint/no-deprecated
                queryValue = Tree.get(params, path);
            }
            else {
                const bucket = rule.value.subquery.bucket;
                const select = rule.value.subquery.select;
                const union = rule.value.subquery.union;
                const { tableName } = PostgresBucketAdapter.getTableMeta(trx, { schema: bucket } as any);

                queryValue = `SELECT ${select} FROM ${tableName} WHERE ${_union(union, params, param_template)}`;
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
                p = Array.from({ length: queryValue.length }).map((_, i) => 
                    `$${i + sql_params.length + 1}`
                ).join(',');
                sql_params.push(...queryValue);
            }
            else {
                sql_params.push(queryValue);
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
        for (const param of params) {
            if ('id' in param) {
                if (param_ids.has(param.id)) continue;
                param_ids.add(param.id);
            }
            for (const param_template of param_templates) {
                const where = _union(part.union, param, param_template);
                if (where) {
                    wheres.push(where);
                }
            }
        }
        const where = wheres.length ? `WHERE ${wheres.join(' OR ')}` : '';
        const sql_str = `FROM ${tableName} ${where}`;

        const sort = part.union.sort;
        let order_str;
        if (!sort?.length) {
            order_str = `ORDER BY ${meta.updated_at} DESC`;
        }
        else {
            order_str = 'ORDER BY ' + sort.map(s =>
                `${PostgresNQLRunner.fieldpathToColumn(s.key)} ${s.dir === 'asc' ? 'ASC' : 'DESC'}`
            ).join(',');
        }
        
        let limit_str = '';
        if (pagination?.page || pagination?.perPage) {
            const limit = pagination.perPage ?? 10;
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