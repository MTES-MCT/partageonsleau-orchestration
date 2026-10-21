#!/usr/bin/env bash
set -euo pipefail

image_ref="${1:?Image digest required}"
[[ "$image_ref" =~ @sha256:[0-9a-f]{64}$ || "$image_ref" =~ ^sha256:[0-9a-f]{64}$ ]] || exit 1

docker run --rm --network none --env NODE_ENV=test --env "EXPECTED_NODE=$(<.nvmrc)" --entrypoint node "$image_ref" --input-type=module -e '
  if (process.versions.node !== process.env.EXPECTED_NODE) {
    throw new Error(`Unexpected Node runtime: ${process.versions.node}; expected ${process.env.EXPECTED_NODE}`);
  }
  const profiling = await import("@sentry/profiling-node");
  profiling.nodeProfilingIntegration();
  const {createRequire} = await import("node:module");
  const require = createRequire(import.meta.url);
  const xlsx = require("xlsx");
  const workbook = xlsx.utils.book_new();
  xlsx.utils.book_append_sheet(workbook, xlsx.utils.aoa_to_sheet([["test"], [1]]), "test");
  const buffer = xlsx.write(workbook, {bookType: "xlsx", type: "buffer"});
  if (!xlsx.read(buffer, {type: "buffer"}).Sheets.test) throw new Error("Spreadsheet round trip failed");
  console.log(`Node ${process.versions.node}, native Sentry profiling and SheetJS work in the production image`);
'
