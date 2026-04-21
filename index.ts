import {connectorRegistry} from './src/connectors/index.js';
import {pullUpdatedData} from './src/jobs/pull_updated_data.js';

// No orchestration for now, only one job.
pullUpdatedData(connectorRegistry);
