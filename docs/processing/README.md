# Processing

This section covers Benthos processors for data transformation and message handling. Processors allow you to modify, filter, and enrich messages as they flow through your data pipelines.

## Available Processors

- **[Tag Processor](tag-processor.md)** - Optimized for processing tags and time series data, converting them to the UMH data model within the `_historian` data contract. Provides automatic message formatting, metadata generation, and structured processing stages.

- **[Classic to Core Processor](classic-to-core-processor.md)** - Converts UMH Historian Data Contract format messages into Core format, following the "one tag, one message, one topic" principle.

- **[Downsampler](downsampler.md)** - Reduces time-series data volume by filtering out insignificant changes using configurable algorithms. Integrates with UMH data pipelines to compress historian data while preserving significant trends.

- **[Node-RED JavaScript Processor](node-red-javascript-processor.md)** - Provides full control over payload and metadata through custom JavaScript code. Use this processor when you need complex transformations, conditional logic, or custom processing beyond standard tag handling.

- **[More Processors](https://docs.redpanda.com/redpanda-connect/components/processors/about/)** - Additional built-in processors available in Benthos/Redpanda Connect for various data processing needs.

## Which Processor to Choose?

- Use **Tag Processor** when working with structured time series data that needs to conform to the UMH data model
- Use **Classic to Core Processor** when migrating from legacy UMH Historian Data Contract format to modern Core format
- Use **Downsampler** after tag_processor to reduce data volume while preserving important changes in time-series data
- Use **Node-RED JavaScript Processor** when you need maximum flexibility and custom processing logic
- Explore the **additional processors** for specific use cases like JSON manipulation, HTTP requests, caching, and more
