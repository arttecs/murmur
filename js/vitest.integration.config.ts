import { spawnSync } from 'node:child_process'
import { defineConfig } from 'vitest/config'

/** Whether PHP is absent from the test command's PATH. */
const phpMissing = spawnSync('php', ['-v'], { timeout: 5000 }).error?.code === 'ENOENT'

if (phpMissing) {
  console.info('Skipping Murmur integration tests because php is not on PATH.')
}

export default defineConfig({
  test: {
    include: phpMissing ? [] : ['tests/integration/**/*.test.ts'],
    passWithNoTests: phpMissing,
    testTimeout: 10_000,
  },
})
