import logging
from logging.handlers import RotatingFileHandler
import datetime
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import time
import sys
from tickers_one import run_tickers
from pull_two import run_pull
import os

# ---------------- Directories ----------------
BASE_DIR = Path(__file__).parent.resolve()
LOG_DIR = BASE_DIR / 'logs'
SUBPROCESS_LOG_DIR = LOG_DIR / 'subprocess_output'
LOG_RETENTION_DAYS = 7

LOG_DIR.mkdir(parents=True, exist_ok=True)
SUBPROCESS_LOG_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- Logging ----------------
MAIN_LOG_FILE = LOG_DIR / 'scheduler_main.log'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = RotatingFileHandler(MAIN_LOG_FILE, maxBytes=5*1024*1024, backupCount=5)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# ---------------- Utility Functions ----------------
def cleanup_old_logs():
    """Delete old subprocess logs."""
    now = datetime.datetime.now()
    cutoff_time = now - datetime.timedelta(days=LOG_RETENTION_DAYS)
    deleted_count = 0

    for filename in os.listdir(SUBPROCESS_LOG_DIR):
        file_path = SUBPROCESS_LOG_DIR / filename
        if file_path.is_file():
            if datetime.datetime.fromtimestamp(file_path.stat().st_mtime) < cutoff_time:
                try:
                    file_path.unlink()
                    deleted_count += 1
                    logger.debug(f"Deleted old log: {file_path}")
                except Exception as e:
                    logger.error(f"Error deleting {file_path}: {e}")

    if deleted_count:
        logger.info(f"Deleted {deleted_count} old subprocess log file(s).")
    else:
        logger.info("No old subprocess log files to delete.")

def run_script_with_logging(func, name):
    """Run a function and log its output to a timestamped file."""
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")

    log_path = SUBPROCESS_LOG_DIR / f"{name}_{timestamp}.log"

    logger.info(f"Starting '{name}', logging to {log_path}")

    try:
        # Redirect stdout/stderr to file temporarily
        with open(log_path, 'w') as f:
            original_stdout = sys.stdout
            original_stderr = sys.stderr
            sys.stdout = f
            sys.stderr = f
            try:
                func()
            finally:
                sys.stdout = original_stdout
                sys.stderr = original_stderr

        logger.info(f"Finished '{name}' successfully.")
    except Exception as e:
        logger.exception(f"Error while running '{name}': {e}")

# ---------------- Job Function ----------------
def daily_job():
    logger.info("Daily job started.")
    cleanup_old_logs()
    run_script_with_logging(run_tickers, "tickers")
    run_script_with_logging(run_pull, "pull")
    logger.info("Daily job finished.")

# ---------------- Scheduler ----------------
scheduler = BackgroundScheduler()
daily_trigger = CronTrigger(hour=23,minute=55, second=0, timezone='UTC')  # Daily 17:40pm (houston / 22 40 utc
scheduler.add_job(daily_job, daily_trigger, id='daily_job')

scheduler.start()
logger.info("Scheduler started.")

# ---------------- Keep Alive ----------------
try:
    while True:
        time.sleep(1)
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
    logger.info("Scheduler stopped.")
