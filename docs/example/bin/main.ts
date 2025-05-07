import { Log } from 'nesoi/lib/engine/util/log';
import App from '../apps/main.app';

Log.level = 'info';

async function main() {
    const daemon = await App.daemon();

    const response = await daemon.trx('example')
        .run(async trx => {

            const users = await trx.bucket('user').readAll();
            console.log({users});
            
        });

    console.log(response.summary());
    // console.log(response.output?.summary());
}

void main();