/* eslint-disable @typescript-eslint/prefer-promise-reject-errors */
/* eslint-disable @typescript-eslint/no-dynamic-delete */
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
import { AnyModule } from 'nesoi/lib/engine/module';

export class PostgresService<Name extends string = 'pg'>
    extends Service<Name, PostgresConfig | undefined> {

    static defaultName = 'pg';

    public libPaths = [
        'modules/*/migrations',
        'modules/*/*/migrations'
    ];

    public sql!: postgres.Sql<any>;
    public nql!: PostgresNQLRunner;

    private transactions: {
        [x in string]?: {
            begin_module: string
            state: 'open'|'ok'|'error'
            sql: postgres.Sql,
            commit: () => void,
            rollback: () => void,
            finish_commit?: () => void,
            finish_rollback?: () => void,
        }
     } = {};

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

        /*
            Called on:
            - Begin: a new nesoi transaction is starting
            - Chain: a nesoi transaction from a module A is extending to a module B
        */
        const begin = (trx: AnyTrx, services: Record<string, PostgresService>) => {
            const postgres = services[service].sql;

            // If nesoi transaction is idempotent, we don't open a db transaction.
            if (trx.idempotent) {
                // Expose the service SQL runner for the transaction nodes
                Trx.set(trx.root, service+'.sql', postgres);
                return Promise.resolve();
            }

            const trxs = services[service].transactions;
            return new Promise<void>((wrap_resolve, wrap_reject) => {
                try {
                    // If it's a nesoi transaction that already started a db transaction on this service,
                    // we must not create/commit/rollback a new db transaction
                    if (trx.id in trxs) {
                        Trx.set(trx.root, service+'.sql', trxs[trx.id]!.sql);
                        wrap_resolve();
                        return;
                    }

                    // If it's an unseen nesoi transaction, start a new db transaction
                    void postgres.begin(sql => new Promise<void>((resolve, reject) => {
                        
                        const module = (trx as any).module as AnyModule;
                        // Register the SQL runner and commit/rollback callbacks
                        // for the db transaction associated with the nesoi transaction
                        trxs[trx.id] = {
                            begin_module: module.name,
                            state: 'open',
                            sql,
                            commit: resolve,
                            rollback: reject
                        };

                        // Expose the db transaction SQL runner for the transaction nodes
                        Trx.set(trx.root, service+'.sql', sql);

                        // Begin is done
                        Log.info('service', 'postgres', `Transaction ${trx.root.globalId} started on PostgreSQL service ${service}`);
                        wrap_resolve();
                    }))
                    // The db transaction commit/rollback callbacks triggers the sections below when called.
                        .then(
                            () => {
                                if (!trxs[trx.id]?.finish_commit) {
                                    throw new Error(`Failed to finish PostgreSQL transaction ${trx.root.globalId}. The finish_commit callback is not available. This might mean the PostgreSQL transaction was already commited/rolledback.`);
                                }
                                trxs[trx.id]?.finish_commit!();
                            },
                            () => {
                                if (!trxs[trx.id]?.finish_rollback) {
                                    throw new Error(`Failed to finish PostgreSQL transaction ${trx.root.globalId}. The finish_rollback callback is not available. This might mean the PostgreSQL transaction was already commited/rolledback.`);
                                }
                                trxs[trx.id]?.finish_rollback!();
                            }
                        );
                }
                catch (e: any) {
                    // Begin failed
                    wrap_reject(e);
                }
            });
        };

        /*
            Called on:
            - Begin/Continue*: an idempotent transaction was requested for the engine with a specific id,
            which didn't previously exist.
            - Continue: an ongoing nesoi transaction was requested for the engine,
            either because of an external transaction on the same module,
            or a nesoi transaction that has asynchronous behavior.
        */
        const _continue = (trx: AnyTrx, services: Record<string, PostgresService>) => {
            const postgres = services[service].sql;
            
            // If nesoi transaction is idempotent, we don't open a db transaction.
            if (trx.idempotent) {
                Trx.set(trx.root, service+'.sql', postgres);
                return Promise.resolve();
            }

            const transaction = services[service].transactions[trx.id];
            if (!transaction) {
                throw new Error(`Failed to continue transaction ${trx.root.globalId}. PostgreSQL transaction no longer avialable (already committed/rolledback).`);
            }

            // Expose the db transaction SQL runner for the transaction nodes
            Trx.set(trx.root, service+'.sql', transaction.sql);
            return Promise.resolve();
        };

        /*
            Called on:
            - Commit: the nesoi transaction is done successfully
        */
        const commit = (trx: AnyTrx, services: Record<string, PostgresService>) => {
            
            // If nesoi transaction is idempotent, there's no db transaction to commit
            // (In reality, this method should not even be called for idempotent transactions)
            if (trx.idempotent) {
                return Promise.resolve();
            }
            
            const transaction = services[service].transactions[trx.id];
            if (!transaction) {
                throw new Error(`Critical: Failed to commit transaction ${trx.root.globalId}. PostgreSQL transaction no longer avialable (already committed/rolledback).`);
            }

            const clearRegistry = () => {
                // We must only delete the the database transaction from the dictionary
                // if this nesoi transaction is the original issuer, to guarantee
                // it won't be deleted ahead of time.
                const module = (trx as any).module as AnyModule;
                if (module.name === transaction.begin_module) {
                    delete services[service].transactions[trx.id];
                    Log.debug('service', 'postgres', `Transaction ${trx.root.globalId} on PostgreSQL service ${service} finished, removed from registry.`);
                }
            };

            // Multiple nesoi transactions can share the same ID (on different modules),
            // if they also use the same service this would trigger a commit twice.
            // Due to this, we only commit once.

            switch (transaction.state) {
            case 'open':
                break;
            case 'ok':
                Log.debug('service', 'postgres', `Attempt to commit transaction ${trx.root.globalId} on PostgreSQL service ${service} skipped, already commited.`);
                clearRegistry();
                return Promise.resolve();
            case 'error':
                throw new Error(`Critical: Failed to commit transaction ${trx.root.globalId}. PostgreSQL transaction previously rolledback.`);
            }
            transaction.state = 'ok';

            // The commit is only finished after the `.begin` method resolves,
            // so we register the `finish` callbacks, trigger a commit and
            // wait for it to be called, once PostgreSQL finished commiting.
            // This ensures that Nesoi waits for PostgreSQL to finish before proceeding.

            return new Promise<void>((resolve, reject) => {
                const ok = () => {
                    Log.info('service', 'postgres', `Transaction ${trx.root.globalId} commited on PostgreSQL service ${service}`);
                    clearRegistry();
                    resolve();
                };
                const error = () => {
                    delete services[service].transactions[trx.id];
                    reject(new Error(`Critical: Failed to commit transaction ${trx.root.globalId}`));
                };
                transaction.finish_commit = ok;
                transaction.finish_rollback = error;
                transaction.commit();
            });
        };
        
        /*
            Called on:
            - Rollback: the nesoi transaction had some error and is rolling back
        */
        const rollback = (trx: AnyTrx, services: Record<string, PostgresService>) => {

            // If nesoi transaction is idempotent, there's no db transaction to rollback
            // (In reality, this method should not even be called for idempotent transactions)
            if (trx.idempotent) {
                return Promise.resolve();
            }

            const transaction = services[service].transactions[trx.id];
            if (!transaction) {
                throw new Error(`Critical: Failed to rollback transaction ${trx.root.globalId}. PostgreSQL transaction no longer avialable (already committed/rolledback).`);
            }

            const clearRegistry = () => {
                // We must only delete the the database transaction from the dictionary
                // if this nesoi transaction is the original issuer, to guarantee
                // it won't be deleted ahead of time.
                const module = (trx as any).module as AnyModule;
                if (module.name === transaction.begin_module) {
                    delete services[service].transactions[trx.id];
                    Log.debug('service', 'postgres', `Transaction ${trx.root.globalId} on PostgreSQL service ${service} finished, removed from registry.`);
                }
            };

            // Multiple nesoi transactions can share the same ID (on different modules),
            // if they also use the same service this would trigger a rollback twice.
            // Due to this, we only rollback once.

            switch (transaction.state) {
            case 'open':
                break;
            case 'ok':
                throw new Error(`Critical: Failed to rollback transaction ${trx.root.globalId}. PostgreSQL transaction previously commited.`);
            case 'error':
                Log.debug('service', 'postgres', `Attempt to commit transaction ${trx.root.globalId} on PostgreSQL service ${service} skipped, already rolled back.`);
                clearRegistry();
                return Promise.resolve();
            }
            transaction.state = 'error';

            // The rollback is only finished after the `.begin` method rejects,
            // so we register the `finish` callbacks, trigger a rollback and
            // wait for it to be called, once PostgreSQL finished rolling back.
            // This ensures that Nesoi waits for PostgreSQL to finish before proceeding.

            return new Promise<void>((resolve, reject) => {
                const ok = () => {
                    Log.info('service', 'postgres', `Transaction ${trx.root.globalId} rolledback on PostgreSQL service ${service}`);
                    clearRegistry();
                    resolve();
                };
                const error = () => {
                    delete services[service].transactions[trx.id];
                    reject(new Error(`Critical: Failed to rollback transaction ${trx.root.globalId}`));
                };
                transaction.finish_commit = error;
                transaction.finish_rollback = ok;
                transaction.rollback();
            });
        };
        return { begin, continue: _continue, commit, rollback };
    }
}