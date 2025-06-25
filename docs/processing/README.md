# Processing

This section covers Benthos processors for data transformation and message handling. Processors allow you to modify, filter, and enrich messages as they flow through your data pipelines.

## Available Processors

- **[Tag Processor](tag-processor.md)** - Optimized for processing tags and time series data, converting them to the UMH data model within the `_historian` data contract. Provides automatic message formatting, metadata generation, and structured processing stages.

- **[Node-RED JavaScript Processor](node-red-javascript-processor.md)** - Provides full control over payload and metadata through custom JavaScript code. Use this processor when you need complex transformations, conditional logic, or custom processing beyond standard tag handling.

- **[Downsampler](downsampler.md)** - Advanced data compression processor that reduces data volume while preserving important trends and changes. Supports multiple algorithms including swinging door and deadband compression.

- **[Classic to Core Processor](classic-to-core-processor.md)** - Transforms messages from the classic UMH data format to the new core format, ensuring backward compatibility and smooth migration paths.

- **[Topic Browser](topic-browser.md)** - Specialized processor for browsing and analyzing MQTT topic structures, providing insights into data flows and topic hierarchies.

- **[More Processors](https://docs.redpanda.com/redpanda-connect/components/processors/about/)** - Additional built-in processors available in Benthos/Redpanda Connect for various data processing needs.

## Which Processor to Choose?

- Use **Tag Processor** when working with structured time series data that needs to conform to the UMH data model
- Use **Node-RED JavaScript Processor** when you need maximum flexibility and custom processing logic
- Use **Downsampler** when you need to reduce data volume while preserving important trends
- Use **Classic to Core Processor** when migrating from legacy UMH data formats
- Use **Topic Browser** when you need to analyze and understand MQTT topic structures
- Explore the **additional processors** for specific use cases like JSON manipulation, HTTP requests, caching, and more

