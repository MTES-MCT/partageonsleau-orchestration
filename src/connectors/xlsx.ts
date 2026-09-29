import {createRequire} from 'node:module'
import type * as SheetJs from 'xlsx'

// The Node build includes filesystem and legacy code-page support. Keep the
// same parser for historical XLS files and modern XLSX/CSV declarations.
const require = createRequire(import.meta.url)
const xlsx = require('xlsx') as typeof SheetJs

export default xlsx
