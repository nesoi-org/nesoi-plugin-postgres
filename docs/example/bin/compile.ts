import { Compiler, MonolythCompiler } from 'nesoi/lib/compiler';
import { Log } from 'nesoi/lib/engine/util/log';
import Nesoi from '../nesoi';

Log.level = 'info';

async function main() {

    /* Elements */
    
    const compiler = await new Compiler(Nesoi).run();
    
    /* Monolyth App */

    await new MonolythCompiler(
        compiler,
        './apps/main.app.ts',
        {
            scripts: {
                'main': 'bin/main.ts'
            }
        }).run();
    
}

main();