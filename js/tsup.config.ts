import { defineConfig } from 'tsup'

const emitDeclarations = [
  'tsc --ignoreConfig --strict --noUncheckedIndexedAccess --noImplicitOverride',
  '--target ES2022 --module NodeNext --declaration --emitDeclarationOnly --rootDir src --outDir dist src/index.ts',
].join(' ')

const mirrorCommonJsDeclarations = String.raw`node -e "
  const { readdirSync, readFileSync, writeFileSync } = require('node:fs');
  for (const file of readdirSync('dist').filter(file => file.endsWith('.d.ts'))) {
    const contents = readFileSync('dist/' + file, 'utf8');
    const commonJs = contents.replaceAll(/(?<=from ')[^']+\.js(?=')/g, path => path.slice(0, -3) + '.cjs');
    writeFileSync('dist/' + file.slice(0, -5) + '.d.cts', commonJs);
  }
"`.replaceAll('\n', ' ')

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  clean: true,
  onSuccess: `${emitDeclarations} && ${mirrorCommonJsDeclarations}`,
})
