# Copyright 2025 UMH Systems GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
import csv

def load_csv_data(filename):
    """Load CSV data, robust to files with or without headers."""
    try:
        # Peek at the first row to check for header
        with open(filename, 'r') as f:
            first_row = next(csv.reader(f))
        if first_row[0].lower() == 'timestamp':
            df = pd.read_csv(filename)
        else:
            df = pd.read_csv(filename, header=None)
            if df.shape[1] == 5:
                df.columns = ['timestamp', 'name', 'origin', 'asset_id', 'value']
            elif df.shape[1] == 3:
                df.columns = ['timestamp', 'name', 'value']
            else:
                raise ValueError(f"Unexpected number of columns ({df.shape[1]}) in {filename}")
        # Always select only the relevant columns
        df = df[['timestamp', 'name', 'value']]
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
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

def plot_comparison(original_df, compressed_df, compressed_label="Compressed Data"):
    """Plot original and compressed data on the same graph."""
    
    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10))
    
    # Plot 1: Full time series comparison
    ax1.plot(original_df['timestamp'], original_df['value'], 
             label='Original Data', alpha=0.7, linewidth=0.5, color='blue')
    ax1.plot(compressed_df['timestamp'], compressed_df['value'], 
             label=f'{compressed_label}', marker='o', markersize=1, 
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
             label=f'{compressed_label}', marker='o', markersize=3, 
             linewidth=1, color='red')
    
    ax2.set_xlabel('Timestamp')
    ax2.set_ylabel('Value')
    ax2.set_title('Data Compression Comparison - Detailed View (First 1000 Original Points)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def compare_and_plot(original_file, compressed_file, output_png, compressed_label):
    print(f"\nComparing {original_file} vs {compressed_file} ...")
    original_df = load_csv_data(original_file)
    compressed_df = load_csv_data(compressed_file)
    if original_df is None or compressed_df is None:
        print("Error loading files.")
        return
    ratio, percentage = calculate_compression_ratio(len(original_df), len(compressed_df))
    fig = plot_comparison(original_df, compressed_df, compressed_label)
    fig.suptitle(
        f'{compressed_label} Compression Results\n'
        f'Compression Ratio: {ratio:.2f}:1 | Data Reduction: {percentage:.1f}%',
        fontsize=14, y=0.98
    )
    plt.savefig(output_png, dpi=300, bbox_inches='tight')
    print(f"Plot saved as: {output_png}")
    plt.close(fig)

def create_combined_plot(original_df, swinging_door_df, deadband_df):
    """Create a 2x2 grid plot showing both compression methods."""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 15))
    
    # Plot 1: Swinging Door Full View
    ax1.plot(original_df['timestamp'], original_df['value'], 
             label='Original Data', alpha=0.7, linewidth=0.5, color='blue')
    ax1.plot(swinging_door_df['timestamp'], swinging_door_df['value'], 
             label='Swinging Door', marker='o', markersize=1, 
             linewidth=1, color='red', alpha=0.8)
    ax1.set_xlabel('Timestamp')
    ax1.set_ylabel('Value')
    ax1.set_title('Swinging Door - Full Dataset')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Deadband Full View
    ax2.plot(original_df['timestamp'], original_df['value'], 
             label='Original Data', alpha=0.7, linewidth=0.5, color='blue')
    ax2.plot(deadband_df['timestamp'], deadband_df['value'], 
             label='Deadband', marker='o', markersize=1, 
             linewidth=1, color='green', alpha=0.8)
    ax2.set_xlabel('Timestamp')
    ax2.set_ylabel('Value')
    ax2.set_title('Deadband - Full Dataset')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Swinging Door Zoomed View
    zoom_points = min(1000, len(original_df))
    original_zoom = original_df.head(zoom_points)
    swinging_door_zoom = swinging_door_df[swinging_door_df['timestamp'] <= original_zoom['timestamp'].iloc[-1]]
    
    ax3.plot(original_zoom['timestamp'], original_zoom['value'], 
             label='Original Data', alpha=0.8, linewidth=1, color='blue')
    ax3.plot(swinging_door_zoom['timestamp'], swinging_door_zoom['value'], 
             label='Swinging Door', marker='o', markersize=3, 
             linewidth=1, color='red')
    ax3.set_xlabel('Timestamp')
    ax3.set_ylabel('Value')
    ax3.set_title('Swinging Door - Detailed View (First 1000 Points)')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Deadband Zoomed View
    deadband_zoom = deadband_df[deadband_df['timestamp'] <= original_zoom['timestamp'].iloc[-1]]
    
    ax4.plot(original_zoom['timestamp'], original_zoom['value'], 
             label='Original Data', alpha=0.8, linewidth=1, color='blue')
    ax4.plot(deadband_zoom['timestamp'], deadband_zoom['value'], 
             label='Deadband', marker='o', markersize=3, 
             linewidth=1, color='green')
    ax4.set_xlabel('Timestamp')
    ax4.set_ylabel('Value')
    ax4.set_title('Deadband - Detailed View (First 1000 Points)')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def main():
    """Main execution function."""
    print("Loading CSV files...")
    
    # Load all data
    original_df = load_csv_data('export.csv')
    swinging_door_df = load_csv_data('export_compressed_swinging_door_1.csv')
    deadband_df = load_csv_data('export_compressed_deadband_1.csv')
    
    if original_df is None or swinging_door_df is None or deadband_df is None:
        print("Error loading files.")
        return
    
    # Calculate compression ratios
    swinging_door_ratio, swinging_door_percentage = calculate_compression_ratio(
        len(original_df), len(swinging_door_df))
    deadband_ratio, deadband_percentage = calculate_compression_ratio(
        len(original_df), len(deadband_df))
    
    # Create combined plot
    print("\nGenerating combined comparison plot...")
    fig = create_combined_plot(original_df, swinging_door_df, deadband_df)
    
    # Add compression info to the plot
    fig.suptitle(
        f'Compression Comparison Results\n'
        f'Swinging Door: {swinging_door_ratio:.2f}:1 ratio ({swinging_door_percentage:.1f}% reduction) | '
        f'Deadband: {deadband_ratio:.2f}:1 ratio ({deadband_percentage:.1f}% reduction)',
        fontsize=14, y=0.98
    )
    
    # Save the plot
    output_file = 'compression_comparison_combined.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved as: {output_file}")
    plt.close(fig)

if __name__ == "__main__":
    main() 