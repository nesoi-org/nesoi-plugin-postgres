import Nesoi from '../nesoi';
import { MonolythApp } from 'nesoi/lib/engine/apps/monolyth/monolyth.app';
import { PostgresBucketAdapter, PostgresCLI, PostgresConfig, PostgresService } from '../../../src';

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
        db: 'postgres_nesoi_sandbox',
    }
};

export default new MonolythApp('main', Nesoi)

    .modules([
        'example'
    ])

    .service(
        new PostgresService(PostgresConfig)
    )

    .config.buckets({
        example: {
            user: {
                adapter: ($, { pg }) => new PostgresBucketAdapter($, pg, 'users')
            }
        }
    })

    .config.trash({
        example: {
            adapter: ($, { pg }) => new PostgresBucketAdapter($, pg, '__trash__')
        }
    })

    .config.cli({
        adapters: {
            pg: (cli, { pg }) => new PostgresCLI(cli, pg)
        }
    });