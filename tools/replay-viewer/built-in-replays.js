export const BUILT_IN_REPLAYS_CONFIG = [
  { key: "csv-payments", label: "CSV Payments built-in", path: "./datasets/csv-payments-built-in.json" },
  { key: "csv-payments-1k-slow", label: "CSV Payments 1k slow provider", path: "./datasets/csv-payments-1k-slow.json.gz", compression: "gzip" },
  { key: "csv-payments-10k", label: "CSV Payments 10k paged", path: "./datasets/csv-payments-10k-paged.json.gz", compression: "gzip" },
  { key: "search-warm-cache", label: "Search built-in pre-warm", path: "./datasets/search-built-in-pre-warm.json" },
  { key: "search-cache-hit", label: "Search built-in", path: "./datasets/search-built-in.json" }
];
