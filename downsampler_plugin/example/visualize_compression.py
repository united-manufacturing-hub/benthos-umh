#!/usr/bin/env python3
"""
CSV Compression Visualization Script

This script reads the original and compressed CSV files, plots both datasets
on the same graph, and calculates/displays the compression ratio.

Requirements:
- pandas
- matplotlib
- numpy
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import os
import sys

def load_csv_data(filename):
    """Load CSV data and convert timestamp to datetime."""
    try:
        df = pd.read_csv(filename)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except FileNotFoundError:
        print(f"Error: {filename} not found!")
        return None
    except Exception as e:
        print(f"Error loading {filename}: {e}")
        return None

def calculate_compression_ratio(original_count, compressed_count):
    """Calculate compression ratio."""
    if compressed_count == 0:
        return float('inf')
    ratio = original_count / compressed_count
    percentage = (1 - compressed_count / original_count) * 100
    return ratio, percentage

def plot_comparison(original_df, compressed_df):
    """Plot original and compressed data on the same graph."""
    
    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10))
    
    # Plot 1: Full time series comparison
    ax1.plot(original_df['timestamp'], original_df['value'], 
             label='Original Data', alpha=0.7, linewidth=0.5, color='blue')
    ax1.plot(compressed_df['timestamp'], compressed_df['value'], 
             label='Compressed Data (Swinging Door)', marker='o', markersize=1, 
             linewidth=1, color='red', alpha=0.8)
    
    ax1.set_xlabel('Timestamp')
    ax1.set_ylabel('Value')
    ax1.set_title('Data Compression Comparison - Full Dataset')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Zoomed view of first 1000 points for better detail
    zoom_points = min(1000, len(original_df))
    original_zoom = original_df.head(zoom_points)
    compressed_zoom = compressed_df[compressed_df['timestamp'] <= original_zoom['timestamp'].iloc[-1]]
    
    ax2.plot(original_zoom['timestamp'], original_zoom['value'], 
             label='Original Data', alpha=0.8, linewidth=1, color='blue')
    ax2.plot(compressed_zoom['timestamp'], compressed_zoom['value'], 
             label='Compressed Data (Swinging Door)', marker='o', markersize=3, 
             linewidth=1, color='red')
    
    ax2.set_xlabel('Timestamp')
    ax2.set_ylabel('Value')
    ax2.set_title('Data Compression Comparison - Detailed View (First 1000 Original Points)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def main():
    """Main execution function."""
    
    # File paths
    original_file = 'export.csv'
    compressed_file = 'export_compressed.csv'
    
    print("Loading CSV files...")
    
    # Load original data
    original_df = load_csv_data(original_file)
    if original_df is None:
        sys.exit(1)
    
    # Load compressed data
    compressed_df = load_csv_data(compressed_file)
    if compressed_df is None:
        print(f"Compressed file {compressed_file} not found.")
        print("Please run the Benthos configuration first to generate the compressed data.")
        sys.exit(1)
    
    # Calculate statistics
    original_count = len(original_df)
    compressed_count = len(compressed_df)
    ratio, percentage = calculate_compression_ratio(original_count, compressed_count)
    
    # Print statistics
    print("\n" + "="*60)
    print("COMPRESSION STATISTICS")
    print("="*60)
    print(f"Original data points:    {original_count:,}")
    print(f"Compressed data points:  {compressed_count:,}")
    print(f"Compression ratio:       {ratio:.2f}:1")
    print(f"Data reduction:          {percentage:.2f}%")
    print(f"Space saved:             {original_count - compressed_count:,} data points")
    print("="*60)
    
    # Calculate time range
    time_range = original_df['timestamp'].max() - original_df['timestamp'].min()
    print(f"Time range:              {time_range}")
    print(f"Original sampling rate:  ~{original_count / time_range.total_seconds():.2f} samples/second")
    print(f"Compressed sampling rate: ~{compressed_count / time_range.total_seconds():.2f} samples/second")
    print("="*60)
    
    # Value statistics
    print("\nVALUE STATISTICS")
    print("-"*30)
    print("Original data:")
    print(f"  Min:  {original_df['value'].min():.6f}")
    print(f"  Max:  {original_df['value'].max():.6f}")
    print(f"  Mean: {original_df['value'].mean():.6f}")
    print(f"  Std:  {original_df['value'].std():.6f}")
    
    print("\nCompressed data:")
    print(f"  Min:  {compressed_df['value'].min():.6f}")
    print(f"  Max:  {compressed_df['value'].max():.6f}")
    print(f"  Mean: {compressed_df['value'].mean():.6f}")
    print(f"  Std:  {compressed_df['value'].std():.6f}")
    
    # Create visualization
    print(f"\nGenerating comparison plot...")
    fig = plot_comparison(original_df, compressed_df)
    
    # Add compression info to the plot
    fig.suptitle(f'Swinging Door Compression Results\n'
                f'Compression Ratio: {ratio:.2f}:1 | Data Reduction: {percentage:.1f}% | '
                f'Threshold: 0.5 | Max Time: 30s', 
                fontsize=14, y=0.98)
    
    # Save the plot
    output_file = 'compression_comparison.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved as: {output_file}")
    
    # Show the plot
    plt.show()

if __name__ == "__main__":
    main() 