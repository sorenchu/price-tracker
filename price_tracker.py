import yfinance as yf
import time
import argparse
import yaml
import os
import logging
from typing import Dict, Any, List

# Define a dictionary to map configuration strings to logging constants
LOG_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

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

# --- Configuration and CLI Functions ---

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Loads configuration from a specified YAML file, validating required keys 
    and mapping the log level string to a logging constant.
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        if 'symbols' not in config:
            raise KeyError("YAML configuration missing 'symbols' section.")
        
        if not isinstance(config['symbols'], list):
            raise TypeError("The 'symbols' section must be a list of configuration objects.")

        if 'settings' not in config:
            config['settings'] = {}
        
        # 1. Handle sleep_interval
        if 'sleep_interval' not in config['settings']:
             logging.warning("Configuration Warning: 'sleep_interval' not found. Defaulting to 30 seconds.")
             config['settings']['sleep_interval'] = 30
        
        # 2. Handle log_level (Default is INFO)
        log_level_str = config['settings'].get('log_level', 'INFO').upper()
        if log_level_str not in LOG_LEVEL_MAP:
             logging.warning(f"Configuration Warning: Unknown log_level '{log_level_str}'. Defaulting to INFO.")
             log_level_str = 'INFO'

        # 3. Handle log_file (Default is 'price_tracker.log')
        if 'log_file' not in config['settings']:
            config['settings']['log_file'] = "price_tracker.log"
             
        # Store the logging constant (integer) in the config for use in main()
        config['settings']['log_level'] = LOG_LEVEL_MAP[log_level_str]

        return config
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
    
    # Custom help for interval (for documentation clarity)
    parser.add_argument(
        '--interval-note',
        action='store_true',
        help="Shows the currently configured refresh interval (set in the YAML file)."
    )
    
    return parser.parse_args()

# --- Main Execution ---

def main():
    """Main execution loop for the price tracker."""
    
    # We initialize args early to get the config path
    args = setup_cli()
    
    try:
        # Load config which performs initial validation and log level conversion
        config = load_config(args.config)
        
        symbols_config: List[Dict[str, str]] = config['symbols']
        sleep_interval = config['settings']['sleep_interval']
        log_level = config['settings']['log_level']
        log_file = config['settings']['log_file']
        
        # Setup basic logging configuration using the level loaded from the config
        logging.basicConfig(
            filename=log_file,
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # The first log messages now use the correct configuration
        logging.info("--- Price Tracker Initialized ---")
        logging.debug(f"Tracking {len(symbols_config)} symbols.")
        logging.debug(f"Refresh interval: {sleep_interval} seconds.")
        logging.debug(f"Logging level set to: {logging.getLevelName(log_level)}")
        logging.debug("---------------------------------")
        
        # Create output directory if necessary (assuming paths like output/file.txt)
        for item in symbols_config:
            filepath = item['filepath']
            output_dir = os.path.dirname(filepath)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                logging.info(f"Created output directory: {output_dir}")

    except Exception:
        # If a fatal error occurs before logging.basicConfig is fully executed, 
        # this log might use a less structured default format, but it ensures termination is logged.
        logging.error("Exiting due to fatal configuration or file system error.")
        return

    while True:
        logging.info("\nStarting fetch cycle...")

        for item in symbols_config:
            symbol = item['symbol']
            filepath = item['filepath']
            # source = item['source'] # Currently not used, but available if needed later.

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
