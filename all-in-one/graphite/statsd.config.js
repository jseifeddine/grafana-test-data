// StatsD configuration for the all-in-one container.
// Forwards aggregated metrics to carbon-cache on localhost:2003.
{
  port: 8125,
  backends: ["/usr/lib/node_modules/statsd/backends/graphite"],
  graphite: {
    host: "127.0.0.1",
    port: 2003
  }
}
