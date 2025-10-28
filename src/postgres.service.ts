import { Log } from 'nesoi/lib/engine/util/log';
import postgres from 'postgres';
import { NesoiDate } from 'nesoi/lib/engine/data/date';
import { NesoiDatetime } from 'nesoi/lib/engine/data/datetime';
import { NesoiDecimal } from 'nesoi/lib/engine/data/decimal';
import { PostgresNQLRunner } from './postgres.nql';
import { AnyTrx, Trx } from 'nesoi/lib/engine/transaction/trx';
import { Database } from './migrator/database';
import { PostgresConfig } from './postgres.config';
import { Service } from 'nesoi/lib/engine/app/service';

export class PostgresService<Name extends string = 'pg'>
    extends Service<Name, PostgresConfig | undefined> {

    static defaultName = 'pg';

    public libPaths = [
        'modules/*/migrations',
        'modules/*/*/migrations'
    ];

    public sql!: postgres.Sql<any>;
    public nql!: PostgresNQLRunner;

    private transactions: Record<string, {
        sql: postgres.Sql,
        commit: () => void,
        rollback: () => void,
    }> = {};

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
                    parse     : (val?: string) => val
                        ? NesoiDatetime.fromISO((val.replace(' ','T') || '')+'Z')
                        : undefined
                },
                datetime_z: {
                    to        : 1184,
                    from      : [1184],
                    serialize : (val?: NesoiDatetime) => 
                        typeof val === 'string'
                            ? val
                            : val?.toISO(),
                    parse     : (val?: string) => 
                        val
                            ? NesoiDatetime.fromISO((val.replace(' ','T') || ''))
                            : undefined
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
        try {
            await this.sql.end();
        }
        catch (e: any) {
            Log.warn('service', 'postgres', e.toString());
        }
    }

    public static wrap(service: string) {
        const begin = (trx: AnyTrx, services: Record<string, any>) => {
            const postgres = services[service].sql as postgres.Sql<any>;
            if (trx.idempotent) {
                Trx.set(trx.root, service+'.sql', postgres);
                return Promise.resolve();
            }
            return new Promise<void>((wrap_resolve, wrap_reject) => {
                try {
                    void postgres.begin(sql => new Promise<void>((resolve, reject) => {
                        services[service].transactions[trx.id] = {
                            sql, commit: resolve, rollback: reject
                        };
                        Trx.set(trx.root, service+'.sql', sql);
                        Trx.set(trx.root, service+'.commit', resolve);
                        Trx.set(trx.root, service+'.rollback', reject);
                        wrap_resolve();
                    })).then(
                        () => {
                            services[service].transactions[trx.id].finish_ok();
                        },
                        () => {
                            Log.warn('service', 'postgres', `Transaction ${trx.id} rolled back on database`);
                            services[service].transactions[trx.id].finish_error();
                        }
                    );
                }
                catch (e: any) {
                    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
                    wrap_reject(e);
                }
            });
        };
        const _continue = (trx: AnyTrx, services: Record<string, any>) => {
            const postgres = services[service].sql as postgres.Sql<any>;
            if (trx.idempotent) {
                Trx.set(trx.root, service+'.sql', postgres);
                return Promise.resolve();
            }
            const transaction = services[service].transactions[trx.id];
            if (!transaction) {
                throw new Error(`Failed to continue transaction ${trx.id}. Runner no longer avialable.`);
            }

            Trx.set(trx.root, service+'.sql', transaction.sql);
            Trx.set(trx.root, service+'.commit', transaction.commit);
            Trx.set(trx.root, service+'.rollback', transaction.rollback);
            return Promise.resolve();
        };
        const commit = (trx: AnyTrx, services: Record<string, any>) => {
            if (trx.idempotent) {
                // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
                delete services[service].transactions[trx.id];
                return Promise.resolve();
            }
            
            const commit = Trx.get(trx.root, service+'.commit');
            return new Promise<void>((resolve, reject) => {
                const finish_ok = () => {
                    // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
                    delete services[service].transactions[trx.id];
                    resolve();
                };
                const finish_error = () => {
                    // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
                    delete services[service].transactions[trx.id];
                    reject(new Error(`Failed to commit transaction ${trx.id}`));
                };
                services[service].transactions[trx.id].finish_ok = finish_ok;
                services[service].transactions[trx.id].finish_error = finish_error;
                (commit as any)();
            });
        };
        const rollback = (trx: AnyTrx, services: Record<string, any>) => {
            if (trx.idempotent) {
                // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
                delete services[service].transactions[trx.id];
                return Promise.resolve();
            }

            const rollback = Trx.get(trx.root, service+'.rollback');
            return new Promise<void>((resolve, reject) => {
                const finish_ok = () => {
                    // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
                    delete services[service].transactions[trx.id];
                    reject(new Error(`Failed to rollback transaction ${trx.id}`));
                };
                const finish_error = () => {
                    // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
                    delete services[service].transactions[trx.id];
                    resolve();
                };
                services[service].transactions[trx.id].finish_ok = finish_ok;
                services[service].transactions[trx.id].finish_error = finish_error;
                (rollback as any)();
            });
        };
        return { begin, continue: _continue, commit, rollback };
    }
}