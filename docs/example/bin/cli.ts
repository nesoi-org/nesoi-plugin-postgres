import 'nesoi/tools/dotenv';
import { Log } from 'nesoi/lib/engine/util/log';
import BigRock from '../apps/main.app';

Log.level = 'info';

async function main() {
    const app = await BigRock.daemon();
    await app.cli();
}

void main();