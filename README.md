# CryptoCandleDataToCSV-Python

CryptoCandleDataToCSV is a Python application that fetches cryptocurrency candle data from the Coinbase API and exports the data to CSV files. The application allows you to retrieve historical candle data for multiple products and granularities, handling retries and rate limits efficiently, with progress tracking through a terminal-based progress bar.

## Features

- Fetches candle data from Coinbase for multiple cryptocurrencies.
- Supports multiple granularities: `ONE_MINUTE`, `FIVE_MINUTE`, `FIFTEEN_MINUTE`, `ONE_HOUR`, `SIX_HOUR`, `ONE_DAY`.
- Saves the fetched data in CSV files with detailed logging.
- Automatically retries failed requests, up to a configurable number of retries.
- Progress bar to track the fetching process.

## Installation

### Prerequisites

- Python 3 or higher
- Required Python packages (install via `pip`):

  ```bash
  pip install pandas tqdm python-dateutil coinbase-advanced-py
  ```

### Clone the Repository

```bash
git clone https://github.com/your-username/CryptoCandleDataToCSV-Python.git
cd CryptoCandleDataToCSV-Python
```

### Configuration

The application uses a `config.json` file to define the parameters for data fetching, such as the list of products, granularities, date range, and retry settings. Customize the configuration file according to your needs.

```json
{
  "MaxRetryAttempts": 3,
  "RetryDelayMilliseconds": 2000,
  "ProductIds": [
    "AVAX-USDC",
    "BCH-USDC",
    "BTC-USDC",
    "DOGE-USDC",
    "ETH-USDC",
    "SHIB-USDC",
    "SOL-USDC",
    "UNI-USDC"
  ],
  "Granularities": ["ONE_DAY", "ONE_HOUR", "FIFTEEN_MINUTE", "FIVE_MINUTE"],
  "StartDate": "2019-01-01",
  "EndDate": "2024-09-01"
}
```

### Run the Application

After configuring the `config.json` file, you can run the application using:

```bash
python main.py
```

The application will fetch candle data for the specified products and granularities and save them as CSV files in the current directory.

### Example Output

CSV files are saved with a format like `candle_data_{product_id}_{granularity}.csv`, for example:

```
candle_data_BTC-USDC_ONE_DAY.csv
candle_data_ETH-USDC_ONE_HOUR.csv
```

Each CSV file will contain the following columns:

- `ProductId`: The cryptocurrency pair (e.g., BTC-USDC)
- `Granularity`: The time frame of the candle data (e.g., ONE_DAY)
- `StartDate`: The timestamp of the candle in UTC
- `StartUnix`: The Unix timestamp
- `Low`: The lowest price during the period
- `High`: The highest price during the period
- `Open`: The price at the beginning of the period
- `Close`: The price at the end of the period
- `Volume`: The total volume traded during the period

## Logging

The application logs important information, warnings, and errors in the console, including progress updates for each product and granularity. The progress bar shows the current fetching status for each product, with a progress indicator for chunks of data.