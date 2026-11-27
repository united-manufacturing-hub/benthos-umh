# Swinging Door Compression Example

This example demonstrates how to use the Benthos downsampler plugin with swinging door compression on CSV data.

## Files

- `export.csv` - Original CSV data file (~473k rows)
- `csv_swinging_door_example.yaml` - Benthos configuration for swinging door compression
- `visualize_compression.py` - Python script to visualize compression results
- `requirements.txt` - Python dependencies

## Configuration Parameters

The Benthos configuration uses the following swinging door parameters:
- **Threshold**: 0.5 (deviation threshold for swinging door algorithm)
- **Max Time**: 30s (maximum time interval between samples)

## Usage

### 1. Run the Benthos Compression

First, run the Benthos configuration to compress the CSV data:

```bash
benthos -c csv_swinging_door_example.yaml
```

This will:
- Read the original `export.csv` file
- Apply swinging door compression with the specified parameters
- Output the compressed data to `export_compressed.csv`

### 2. Install Python Dependencies

Install the required Python packages:

```bash
pip install -r requirements.txt
```

### 3. Run the Visualization Script

Execute the Python script to visualize the compression results:

```bash
python visualize_compression.py
```

This will:
- Load both the original and compressed CSV files
- Calculate compression statistics (ratio, percentage reduction, etc.)
- Generate comparison plots showing both datasets
- Save the visualization as `compression_comparison.png`
- Display detailed statistics in the console

## Expected Results

The swinging door compression should significantly reduce the number of data points while preserving the important trends and changes in the data. Typical compression ratios can range from 5:1 to 20:1 depending on the data characteristics and threshold settings.

## Data Format

The CSV files contain the following columns:
- `timestamp` - ISO 8601 formatted timestamp
- `name` - Sensor/variable name
- `origin` - Data origin identifier
- `asset_id` - Asset identifier
- `value` - Numeric measurement value

## Algorithm Details

The swinging door compression algorithm:
1. Keeps the latest point as a held point and draws a tolerance band of the threshold around the trend from the last emitted point
2. Emits the held point once a new point breaks this trend, and keeps the new point as the next held point
3. Emits an arriving point once max_time has passed since the last emitted point, and then discards the held point
4. Drops the points in between, which lie on the trend

With a threshold of 0.5, a held point is emitted only once a later point leaves the trend by about 0.5 units. With a max_time of 30s, a point is emitted at least every 30 seconds while data keeps arriving.
