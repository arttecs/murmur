import { defineConfig } from 'tsup'

export default defineConfig({
  entry: ['src/index.ts', 'src/echo.ts'],
  external: ['laravel-echo'],
  format: ['esm'],
  splitting: true,
  clean: true,
})
