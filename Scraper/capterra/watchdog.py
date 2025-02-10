#!/usr/bin/env python3
import subprocess
import time
import sys
import os
import itertools
import threading
from datetime import datetime
import smtplib
from email.message import EmailMessage

# --- Configuration: Script Names and Heartbeat Files ---
PRODUCT_SCRAPER_SCRIPT = "scrape_capterra_products_seleium_prod.py"
REVIEW_SCRAPER_SCRIPT  = "scraping_reviews_4.0_prod.py"

PRODUCT_HEARTBEAT_FILE = "heartbeat_products.txt"
REVIEW_HEARTBEAT_FILE  = "heartbeat_reviews.txt"

# Threshold in seconds: if heartbeat is older than this, consider the scraper stalled.
HEARTBEAT_THRESHOLD = 300  # 5 minutes

# --- Email Settings ---
# Update these with your actual email credentials.
EMAIL_ADDRESS = "your_hotmail_address@hotmail.com"  # e.g., gilles24.baumann@hotmail.com
EMAIL_PASSWORD = "your_email_password"
SMTP_SERVER = "smtp-mail.outlook.com"
SMTP_PORT = 587
RECIPIENT_ADDRESS = "gilles24.baumann@hotmail.com"

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
        ["python", script],
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
                sys.stdout.write(
                    f"\rHeartbeat OK: last update {elapsed:.0f}s ago {next(spinner)}"
                )
                sys.stdout.flush()
        else:
            sys.stdout.write(
                f"\rWaiting for heartbeat in {heartbeat_file}... {next(spinner)}"
            )
            sys.stdout.flush()
        time.sleep(1)

def get_status():
    """
    Returns a status string with the last heartbeat timestamps for both scrapers.
    """
    status = "Hourly Update from Watchdog:\n\n"
    now = datetime.utcnow()
    prod_ts = get_heartbeat_timestamp(PRODUCT_HEARTBEAT_FILE)
    rev_ts = get_heartbeat_timestamp(REVIEW_HEARTBEAT_FILE)
    if prod_ts:
        elapsed_prod = (now - prod_ts).total_seconds()
        status += f"Product scraper last heartbeat: {prod_ts.isoformat()} ({elapsed_prod:.0f} seconds ago)\n"
    else:
        status += "Product scraper heartbeat not available.\n"
    if rev_ts:
        elapsed_rev = (now - rev_ts).total_seconds()
        status += f"Review scraper last heartbeat: {rev_ts.isoformat()} ({elapsed_rev:.0f} seconds ago)\n"
    else:
        status += "Review scraper heartbeat not available.\n"
    return status

def send_update_email(status_message):
    """
    Sends an email update with the provided status_message.
    """
    try:
        msg = EmailMessage()
        msg["Subject"] = "Hourly Update from Watchdog"
        msg["From"] = EMAIL_ADDRESS
        msg["To"] = RECIPIENT_ADDRESS
        msg.set_content(status_message)
        
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()  # Secure the connection
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.send_message(msg)
        print("\nSent update email.")
    except Exception as e:
        print(f"\nFailed to send update email: {e}")

def hourly_update():
    """
    Runs in a background thread. Sleeps for one hour, then sends an update email.
    """
    while True:
        time.sleep(3600)  # Sleep for one hour.
        status = get_status()
        send_update_email(status)

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
    # Send an initial update email.
    initial_status = get_status()
    send_update_email("Initial Update from Watchdog:\n\n" + initial_status)
    
    # Start the hourly update thread (daemon thread so it will exit when the main thread does).
    update_thread = threading.Thread(target=hourly_update, daemon=True)
    update_thread.start()
    
    # Run the orchestrator.
    orchestrate_scraping()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOrchestrator interrupted by user. Exiting.")
        sys.exit(0)