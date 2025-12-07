import yfinance as yf
import time
import argparse
import yaml
import os
import logging
from typing import Dict, Any, List
import requests
from bs4 import BeautifulSoup
import datetime

LOG_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

def get_time_to_next_monday() -> int:
    """
    Returns seconds until next Monday 00:00 if today is Saturday or Sunday, else 0.
    """
    SATURDAY = 5
    DAYS_OF_WEEK = 7
    today = datetime.datetime.now()
    weekday = today.weekday()
    if weekday >= SATURDAY:
        days_until_monday = DAYS_OF_WEEK - weekday
        next_monday = (today + datetime.timedelta(days=days_until_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_to_sleep = int((next_monday - today).total_seconds())
        return seconds_to_sleep
    return 0

def get_current_value(symbol: str) -> str:
    """
    Obtains the most recent closing price for a given financial symbol using yfinance.
    The output value is formatted using a comma as the decimal separator.
    """
    try:
        logging.info(f"Fetching data for yfinance symbol {symbol}...")

        ticker = yf.Ticker(symbol)
        data = ticker.history(period="1d", interval="1m")

        if not data.empty:
            last_value = data['Close'].iloc[-1]
            return f"{last_value:.6f}".replace(".", ",")
        return "No data found via yfinance."
    except Exception as e:
        logging.error(f"yfinance Error for {symbol}: {e}")
        return f"yfinance Error: {e}"

def get_fund_value(fund_url: str) -> str:
    """
    Scrapes the most recent value of a fund from Investing.com.
    The output value is formatted using a comma as the decimal separator.
    """

    try:
        logging.info(f"Fetching data for Investing.com fund {fund_url}...")
        url = f"https://es.investing.com/funds/{fund_url}-historical-data"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Connection Error for URL {url}: {e}")
        return f"Connection Error: {e}"

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'genTbl closedTbl historicalTbl'})
    if not table:
        logging.error(f"No historical data table found at URL {url}.")
        return "Error: No historical data table found."

    first_row = table.find('tbody').find('tr')
    if not first_row:
        logging.error(f"No recent data row found in the table at URL {url}.")
        return "Error: No recent data row found."

    vl_str = first_row.find_all('td')[1].text.strip()
    vl_clean = vl_str.replace('.', '').replace(',', '.')
    try:
        value_float = float(vl_clean)
        return f"{value_float:.6f}".replace('.', ',')
    except ValueError:
        logging.error(f"Format Error: Extracted value '{vl_str}' is not valid at URL {url}.")
        return f"Format Error: Extracted value '{vl_str}' is not valid."


def is_market_open(symbol: str) -> bool:
    """
    Checks the market status of a ticker by looking at the 'marketState' 
    property in the ticker's info.
    Returns True if the market is in 'REGULAR', 'PRE', or 'POST' state.
    """
    try:
        ticker = yf.Ticker(symbol)
        # Fetch a small subset of info to get the market state
        info = ticker.info
        is_open = info.get('marketState') in ['REGULAR', 'PRE', 'POST']
        if is_open:
            logging.debug(f"Market for {symbol} is OPEN (marketState: {info.get('marketState')}).")
        else:
            logging.debug(f"Market for {symbol} is CLOSED (marketState: {info.get('marketState')}).")
        return is_open
    except Exception as e:
        logging.error(f"Error checking market status for {symbol}: {e}")
        return False

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Loads configuration from a specified YAML file, validating required keys
    and mapping the log level string to a logging constant.
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Configuration Error: Configuration file not found at '{config_path}'")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Configuration Error: Error parsing YAML file: {e}")
        raise
    except KeyError as e:
        logging.error(f"Configuration Error: Missing required key {e}")
        raise
    except TypeError as e:
        logging.error(f"Configuration Error: Structure issue: {e}")
        raise

    if 'symbols' not in config:
        raise KeyError("YAML configuration missing 'symbols' section.")

    if not isinstance(config['symbols'], list):
        raise TypeError("The 'symbols' section must be a list of configuration objects.")

    if 'settings' not in config:
        config['settings'] = {}

    if 'sleep_interval' not in config['settings']:
            logging.warning("Configuration Warning: 'sleep_interval' not found. Defaulting to 30 seconds.")
            config['settings']['sleep_interval'] = 30

    log_level_str = config['settings'].get('log_level', 'INFO').upper()
    config['settings']['log_level'] = LOG_LEVEL_MAP.get(log_level_str, logging.INFO)
    config['settings'].setdefault('log_file', "./price_tracker.log")
    config['settings'].setdefault('max_log_size_mb', 5)
    config['settings'].setdefault('backup_count', 3)

    return config



def setup_cli() -> argparse.Namespace:
    """Sets up the command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="""A robust script to periodically fetch financial data
        (stocks, forex, and indices) exclusively using yfinance and output values to specified files.""",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '-c', '--config',
        type=str,
        required=True,
        help="Path to the YAML configuration file defining symbols and output paths (e.g., config.yaml)."
    )

    parser.add_argument(
        '--interval-note',
        action='store_true',
        help="Shows the currently configured refresh interval (set in the YAML file)."
    )

    return parser.parse_args()

def main():
    """Main execution loop for the price tracker."""

    args = setup_cli()

    try:
        config = load_config(args.config)

        symbols_config: List[Dict[str, str]] = config['symbols']
        sleep_interval = config['settings']['sleep_interval']
        log_level = config['settings']['log_level']
        log_file = config['settings']['log_file']

        logging.basicConfig(
            filename=log_file,
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        logging.info("--- Price Tracker Initialized ---")
        logging.debug(f"Tracking {len(symbols_config)} symbols.")
        logging.debug(f"Refresh interval: {sleep_interval} seconds.")
        logging.debug(f"Logging level set to: {logging.getLevelName(log_level)}")
        logging.debug("---------------------------------")

        for item in symbols_config:
            filepath = item['filepath']
            output_dir = os.path.dirname(filepath)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                logging.info(f"Created output directory: {output_dir}")

    except Exception:
        logging.error("Exiting due to fatal configuration or file system error.")
        return

    while True:
        logging.info("\nStarting fetch cycle...")
        seconds_to_monday = get_time_to_next_monday()
        if seconds_to_monday > 0:
            hours, remainder = divmod(seconds_to_monday, 3600)
            minutes, seconds = divmod(remainder, 60)
            logging.info(f"Weekend detected. Sleeping for {hours}h {minutes}m {seconds}s until Monday...")
            time.sleep(seconds_to_monday)
        for item in symbols_config:
            symbol = item['symbol']
            filepath = item['filepath']
            source = item['source']
            if source == "investing":
                if os.path.exists(filepath):
                    try:
                        mtime = os.path.getmtime(filepath)
                        age_seconds = time.time() - mtime
                        if age_seconds < 24 * 3600:
                            logging.info(f"Skipping fetch for {symbol}; {filepath} was updated {age_seconds/3600:.2f}h ago.")
                            continue
                    except OSError as e:
                        logging.warning(f"Could not determine modification time for {filepath}: {e}")
                value = get_fund_value(symbol)
            else:
                if not is_market_open(symbol):
                    logging.info(f"Market for {symbol} appears closed; skipping fetch.")
                    continue
                value = get_current_value(symbol)

            try:
                with open(filepath, "w") as f:
                    f.write(value)
                logging.info(f"  -> {symbol:<10} | Value: {value:<15} | Wrote to: {filepath}")
            except IOError as e:
                logging.error(f"  -> {symbol:<10} | Error writing to file {filepath}: {e}")

        logging.info(f"Cycle complete. Waiting {sleep_interval} seconds...")
        time.sleep(sleep_interval)

if __name__ == "__main__":
    main()
