import js from '@eslint/js'
import {defineConfig} from 'eslint/config'
import tseslint from 'typescript-eslint'

export default defineConfig(
  {ignores: ['dist/**', 'node_modules/**', '.artifacts/**']},
  {
    files: ['**/*.ts'],
    extends: [js.configs.recommended, tseslint.configs.recommendedTypeChecked],
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      // Unused parameters prefixed with _ document an interface requirement.
      '@typescript-eslint/no-unused-vars': ['error', {argsIgnorePattern: '^_'}],
    },
  },
  {
    files: [
      'src/connectors/*.ts',
      'src/services/partageonsleau-client.ts',
      'src/queues/compatibility.integration.test.ts',
    ],
    // Connector and worker contracts return promises even for local computations.
    rules: {'@typescript-eslint/require-await': 'off'},
  },
  {files: ['eslint.config.mjs'], extends: [js.configs.recommended]},
)
