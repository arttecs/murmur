import { defineConfig } from 'tsup'

export default defineConfig({
  entry: ['src/index.ts', 'src/echo.ts'],
  format: ['esm'],
  splitting: true,
  clean: true,
})
