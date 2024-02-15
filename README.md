# Elexons Data Scraper

This repository contains scripts to scrape data from Elexons API. The scripts provide functionality for downloading demand and generation data, with the option to run the download in parallel for better performance.

## Installation

1. Navigate to the root directory:

2. Create a virtual environment with Python 3.9:
   ```bash
   python3.9 -m venv venv
   ```

3. Activate the virtual environment:

   - On Windows:
     ```bash
     .\venv\Scripts\activate
     ```

   - On MacOS/Linux:
     ```bash
     source venv/bin/activate
     ```

4. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

To use the scraper, you need to specify various options and arguments to control the behavior. Here are the primary functions:

- `run_demand_parallel(*args, **options)`: Downloads P114 demand data in parallel for a specified date range.
- `run_demand(*args, **options)`: Downloads P114 demand data for a specific date range.
- `run_generation(*args, **options)`: Downloads B1610 generation data for a specific date range.
- `run(*args, **options)`: A general function that can call the other functions based on the options provided.

### Options:

- `missing` (bool): Indicates whether data for missing dates should be downloaded.
- `date` (list of str): Specifies the date range for which data should be downloaded. Format: ['yyyy-mm-dd', 'yyyy-mm-dd']
- `mode` (str): Specifies the execution mode of the script. Use 'parallel' for parallel execution and any other value for non-parallel execution.
- `type` (str): Specifies the type of data to be processed. Possible values are 'demand' or 'generation'.
- `overwrite` (bool): Determines whether existing files should be overwritten during data download.

### Example:
```python
options = {
    'mode': 'parallel',
    'type': 'demand',
    'date': ['2023-01-01', '2023-01-31']
}
run(**options)
```
