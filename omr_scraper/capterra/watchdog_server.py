#!/usr/bin/env python3
import subprocess
import time
import sys
import os
import itertools
from datetime import datetime

# --- Script Names and Heartbeat Files ---
PRODUCT_SCRAPER_SCRIPT = "scrape_capterra_products_seleium_prod_server.py"
REVIEW_SCRAPER_SCRIPT  = "scraping_reviews_4.0_prod.py"

PRODUCT_HEARTBEAT_FILE = "heartbeat_products.txt"
REVIEW_HEARTBEAT_FILE  = "heartbeat_reviews.txt"

# Threshold in seconds: if heartbeat is older than this, consider the scraper stalled.
HEARTBEAT_THRESHOLD = 36000000  

def get_heartbeat_timestamp(heartbeat_file):
    """
    Return the datetime from the given heartbeat file, or None if not available or empty.
    Expected file format: ISO timestamp - Product/Review Index: X
    """
    if os.path.exists(heartbeat_file):
        try:
            with open(heartbeat_file, "r") as f:
                line = f.readline().strip()
                if not line:
                    return None
                # Extract the timestamp (assumes format "ISO_timestamp - ...")
                timestamp_str = line.split(" - ")[0]
                return datetime.fromisoformat(timestamp_str)
        except Exception as e:
            print(f"\nError reading heartbeat file {heartbeat_file}: {e}")
    return None

def run_scraper(script):
    """Run the given scraper script as a subprocess and return the process object."""
    return subprocess.Popen(
        ["python3", script],
        # Remove redirection so the subprocess uses the parent’s stdout/stderr:
        # stdout=subprocess.PIPE,
        # stderr=subprocess.PIPE,
        universal_newlines=True
    )

def monitor_process(process, heartbeat_file):
    """
    Monitor the given process and its heartbeat file.
    If the heartbeat file is stale (older than HEARTBEAT_THRESHOLD seconds),
    kill the process and return a nonzero exit code.
    
    Displays inline CLI updates using a spinner.
    """
    spinner = itertools.cycle(['|', '/', '-', '\\'])
    while True:
        retcode = process.poll()
        if retcode is not None:
            sys.stdout.write("\n")
            return retcode

        hb_timestamp = get_heartbeat_timestamp(heartbeat_file)
        if hb_timestamp:
            elapsed = (datetime.utcnow() - hb_timestamp).total_seconds()
            if elapsed > HEARTBEAT_THRESHOLD:
                sys.stdout.write(
                    f"\rHeartbeat in {heartbeat_file} is stale (last update {elapsed:.0f} seconds ago). Restarting scraper...   \n"
                )
                process.kill()
                return -1
            else:
                # Display a live update with the spinner and the age of the heartbeat.
                sys.stdout.write(
                    f"\rHeartbeat OK: last update {elapsed:.0f}s ago {next(spinner)}"
                )
                sys.stdout.flush()
        else:
            # No valid heartbeat yet; display waiting message with spinner.
            sys.stdout.write(
                f"\rWaiting for heartbeat in {heartbeat_file}... {next(spinner)}"
            )
            sys.stdout.flush()
        time.sleep(1)

def orchestrate_scraping():
    """
    Orchestrates the scraping in two phases:
      1. Run the product scraper until it finishes normally.
      2. Then run the review scraper until it finishes normally.
    
    If either process fails or stalls, the watchdog restarts it after waiting.
    """
    # --- Phase 1: Product scraper ---
    print("Starting product scraper process...")
    product_process = run_scraper(PRODUCT_SCRAPER_SCRIPT)
    retcode = monitor_process(product_process, PRODUCT_HEARTBEAT_FILE)
    while retcode != 0:
        print(f"\nProduct scraper terminated with code {retcode}. Restarting in 60 seconds...\n")
        time.sleep(60)
        print("Restarting product scraper process...")
        product_process = run_scraper(PRODUCT_SCRAPER_SCRIPT)
        retcode = monitor_process(product_process, PRODUCT_HEARTBEAT_FILE)
    print("\nProduct scraper finished normally.")

    # --- Phase 2: Review scraper ---
    print("\nStarting review scraper process...")
    review_process = run_scraper(REVIEW_SCRAPER_SCRIPT)
    retcode = monitor_process(review_process, REVIEW_HEARTBEAT_FILE)
    while retcode != 0:
        print(f"\nReview scraper terminated with code {retcode}. Restarting in 60 seconds...\n")
        time.sleep(60)
        print("Restarting review scraper process...")
        review_process = run_scraper(REVIEW_SCRAPER_SCRIPT)
        retcode = monitor_process(review_process, REVIEW_HEARTBEAT_FILE)
    print("\nReview scraper finished normally. Exiting orchestrator.")

def main():
    # You can wrap orchestrate_scraping in a loop if you wish to continue running after completion.
    orchestrate_scraping()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOrchestrator interrupted by user. Exiting.")
        sys.exit(0)
