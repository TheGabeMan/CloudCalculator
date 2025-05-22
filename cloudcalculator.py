""" This script analyzes VM activity data to determine which hostnames have 
active guests per hour in per month. It reads a TSV file containing from
the vcd.broadcom.com portal.
The detailed output includes a detailed CSV file with hourly active hosts and 
the billable cores. The summary output includes a summary CSV file with total 
billable cores per hour.
"""
from datetime import timedelta
from collections import defaultdict
import pandas as pd


def analyze_vm_activity(inputfile):
    """
    Analyze VM activity to show which hostnames have active guests per hour in a month.
    Args:
        inputfile (str): Path to the CSV file containing VM data
        output_file (str, optional): Path to save the output CSV file
    Returns:
        list: List of dictionaries containing hourly activity data
    """

    # Read the dataset
    try:
        # Try different separators as the data might be tab or space separated
        try:
            df = pd.read_csv(inputfile, sep='\t', comment='#')
        except Exception:
            try:
                df = pd.read_csv(inputfile, sep=' ', skipinitialspace=True, comment='#')
            except Exception:
                df = pd.read_csv(inputfile, comment='#')  # Default comma separator
    except Exception as e:
        print(f"Error reading file: {e}")
        return []

    print(f"Loaded {len(df)} records from dataset")

    # Convert datetime columns
    df['startTime'] = pd.to_datetime(df['startTime'])
    df['endTime'] = pd.to_datetime(df['endTime'])

    # Determine the month start and end times
    month_start = df['startTime'].min()
    month_end = df['endTime'].max()

    print(f"Found {len(df)} records in this month")
    # Generate all hours in this month
    month_hours = []
    current_hour = month_start
    while current_hour < month_end:
        month_hours.append(current_hour)
        current_hour += timedelta(hours=1)

    print(f"Analyzing {len(month_hours)} hours")

    # Results list
    results = []

    # For each hour, find hostnames with active guests
    for hour_start in month_hours:
        hour_end = hour_start + timedelta(hours=1)

        # Find all records that are active during this hour
        active_records = df[
            (df['startTime'] < hour_end) & 
            (df['endTime'] > hour_start)
        ]

        # Make active_records unique based on hostname
        active_records = active_records.drop_duplicates(subset=['hostName'])

        # Group by hostname
        activehosts = defaultdict(float)
        for _, record in active_records.iterrows():
            hostname = record['hostName']
            hostBillableCores = int(record['hostBillableCores'])
            activehosts[hostname] += hostBillableCores

        # Create result entry for this hour
        hour_result = {
            'hour': hour_start.strftime('%Y-%m-%d %H:00:00 UTC'),
            'day_of_month': hour_start.day,
            'hour_of_day': hour_start.hour,
            'total_hostnames': int(len(activehosts)),
            'total_hostBillableCores': int(sum(activehosts.values())),
            'hostnames': []
        }

        # Add hostname details
        for hostname in activehosts.items():
            hostname_detail = {
                'hostname': hostname[0],
                'hostBillableCores': hostname[1]
                }
            hour_result['hostnames'].append(hostname_detail)
        results.append(hour_result)

    print("\nAnalysis complete.")

    # Save detailed results to CSV if requested
    save_detailed_csv(results, inputfile)
    save_summary_csv(results, inputfile)

    return results


def save_summary_csv(results,inputfile):
    """Save summary results to CSV file."""
    summary_rows = []
    for hour_data in results:
        summary_rows.append({
            'hour': hour_data['hour'],
            'day_of_month': hour_data['day_of_month'],
            'hour_of_day': hour_data['hour_of_day'],
            'total_hosts': hour_data['total_hostnames'],
            'total_hostBillableCores': hour_data['total_hostBillableCores']
        })
    df_summary = pd.DataFrame(summary_rows)
    output_file = f"{inputfile}_summary.csv"
    df_summary.to_csv(output_file, index=False)
    print(f"Summary results saved to: {output_file}")


def save_detailed_csv(results, inputfile):
    """Save detailed results to CSV file."""
    rows = []
    for hour_data in results:
        for hostname_data in hour_data['hostnames']:
            rows.append({
                'hour': hour_data['hour'],
                'day_of_month': hour_data['day_of_month'],
                'hour_of_day': hour_data['hour_of_day'],
                'hostname': hostname_data['hostname'],
                'hostBillableCores': hostname_data['hostBillableCores']
            })
    df_output = pd.DataFrame(rows)
    output_file = f"{inputfile}_detailed.csv"
    df_output.to_csv(output_file, index=False)
    print(f"Detailed results saved to: {output_file}")


if __name__ == "__main__":
    # Replace 'data.tsv' with the path to your actual data file
    inputfile = "./data.tsv"

    print("Starting VM activity analysis")

    # Analyze the data
    results = analyze_vm_activity(inputfile)
