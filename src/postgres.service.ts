import { Log } from 'nesoi/lib/engine/util/log';
import postgres from 'postgres';
import { NesoiDate } from 'nesoi/lib/engine/data/date';
import { NesoiDatetime } from 'nesoi/lib/engine/data/datetime';
import { NesoiDecimal } from 'nesoi/lib/engine/data/decimal';
import { PostgresNQLRunner } from './postgres.nql';
import { AnyTrx, Trx } from 'nesoi/lib/engine/transaction/trx';
import { TrxEngineWrapFn } from 'nesoi/lib/engine/transaction/trx_engine.config';
import { Database } from './migrator/database';
import { PostgresConfig } from './postgres.config';
import { Service } from 'nesoi/lib/engine/apps/service';

export class PostgresService<Name extends string = 'pg'>
    extends Service<Name, PostgresConfig | undefined> {

    static defaultName = 'pg';

    public libPaths = [
        'modules/*/migrations'
    ];

    public sql!: postgres.Sql<any>;
    public nql!: PostgresNQLRunner;

    up() {
        Log.info('service' as any, 'postgres', 'Connecting to Postgres database');
        this.sql = Database.connect({
            ...(this.config?.connection || {}),
            debug: true,
            types: {
                char: {
                    to        : 1042,
                    from      : [1042],
                    serialize : (val?: string) => val?.trim(),
                    parse     : (val?: string) => val?.trim()
                },
                date: {
                    to        : 1082,
                    from      : [1082],
                    serialize : (val?: NesoiDate) => val?.toISO(),
                    parse     : (val?: string) => val ? NesoiDate.fromISO(val) : undefined
                },
                datetime: {
                    to        : 1114,
                    from      : [1114],
                    serialize : (val?: NesoiDatetime) => typeof val === 'string'
                        ? val
                        : val?.toISO(),
                    parse     : (val?: string) => NesoiDatetime.fromISO((val?.replace(' ','T') || '')+'Z')
                },
                datetime_z: {
                    to        : 1184,
                    from      : [1184],
                    serialize : (val?: NesoiDatetime) =>
                        typeof val === 'string'
                            ? val
                            : val?.toISO(),
                    parse     : (val?: string) => NesoiDatetime.fromISO((val?.replace(' ','T') || '')+'Z')
                },
                decimal: {
                    to        : 1700,
                    from      : [1700],
                    serialize : (val?: NesoiDecimal) => val?.toString(),
                    parse     : (val?: string) => val ? new NesoiDecimal(val) : undefined
                }
            }
        });
        this.nql = new PostgresNQLRunner();
    }
    
    async down() {
        await this.sql.end();
    }

    public static wrap(service: string) {
        return (trx: AnyTrx, fn: TrxEngineWrapFn<any, any>, services: Record<string, any>) => {
            const postgres = services[service].sql as postgres.Sql<any>;
            return postgres.begin(sql => {
                Trx.set(trx.root, 'sql', sql);
                return fn(trx.root);
            });
        };
 
    }

}