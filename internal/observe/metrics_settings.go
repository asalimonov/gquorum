package observe

import internalmetrics "github.com/asalimonov/gquorum/internal/metrics"

// MetricsSettings is shared with the local meters; Resolve validates collector
// budgets as well as retention without starting any workers.
type MetricsSettings = internalmetrics.Settings
