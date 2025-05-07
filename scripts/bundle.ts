import Console from 'nesoi/lib/engine/util/console'
import Shell from 'nesoi/lib/engine/util/shell'
import * as fs from 'fs';
import * as path from 'path';

/**
 * [ bundle ]
 * This script is run when building a plugin for export.
 */

Console.header('Bundle Plugin')

async function main() {
    
    Console.step('(Step 1) Clean build folder')
    fs.rmSync('build', { recursive: true, force: true });

    Console.step('(Step 2) Run Lint')
    await Shell.cmd('.', 'npm run lint')

    Console.step('(Step 3) Transpile TypeScript source files')
    await Shell.cmd('.', 'npm run build')

    Console.step('(Step 4) Run Unit Tests')
    await Shell.cmd('.', 'npm run test')

    Console.step('(Step 5) Include package.json file on build/');
    const packageJson = path.resolve('.', 'package.json')
    const buildPackageJson = path.resolve('.', 'build', 'package.json')
    fs.copyFileSync(packageJson, buildPackageJson);

    Console.step('(Step 6) Include README.md file on build/');
    const readme = path.resolve('.', 'README.md')
    const buildReadme = path.resolve('.', 'build', 'README.md')
    fs.copyFileSync(readme, buildReadme);
    
}

main();