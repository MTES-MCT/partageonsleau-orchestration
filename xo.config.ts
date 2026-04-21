/** @type {import('xo').FlatXoConfig} */
export default [
  {
    ignores: ['dist/**', 'node_modules/**'],
  },
  {
    prettier: true,
    space: 2,
    semicolon: false,
    rules: {
      'no-warning-comments': 'off',
      'no-await-in-loop': 'off',
      'unicorn/filename-case': 'off',
      'n/prefer-global/process': 'off',
      '@typescript-eslint/class-literal-property-style': 'off',
      '@stylistic/curly-newline': 'off',
      '@typescript-eslint/no-unused-vars': [
        'warn',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          caughtErrorsIgnorePattern: '^_',
        },
      ],

      '@typescript-eslint/naming-convention': [
        'error',
        {
          selector: 'default',
          format: ['camelCase', 'PascalCase', 'UPPER_CASE'],
          leadingUnderscore: 'allow',
        },
        {
          selector: 'typeLike',
          format: ['PascalCase'],
        },
        {
          selector: 'enumMember',
          format: ['UPPER_CASE', 'PascalCase'],
        },
        {
          selector: 'property',
          format: null,
        },
        {
          selector: 'objectLiteralProperty',
          format: null,
        },
      ],
    },
  },
]
