########################################
########################################
import subprocess
import logging
from logging.handlers import RotatingFileHandler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import os
import sys
import shutil
import atexit
import signal
from datetime import datetime, timedelta, timezone
from pathlib import Path

# --- Configuration -------------------------------------------------

# Use UTC timezone (common and consistent)
UTC_TIMEZONE = pytz.UTC

# Schedule time (in UTC)
# SCHEDULE_HOUR = 23  # 18:30 Houston
# SCHEDULE_MINUTE = 30

# SCHEDULE_HOUR = 20  # 15:30 Houston
# SCHEDULE_MINUTE = 38

now_utc = datetime.now(timezone.utc)
SCHEDULE_HOUR = now_utc.hour
SCHEDULE_MINUTE = now_utc.minute
print(SCHEDULE_HOUR)
print(SCHEDULE_MINUTE)

# Directories
BASE_DIR = Path(__file__).parent.resolve()
SCRIPT_DIR = BASE_DIR
BASE_LOG_DIR = BASE_DIR / 'logs'
MAIN_LOG_FILE = BASE_LOG_DIR / 'scheduler_main.log'
SUBPROCESS_LOG_DIR = BASE_LOG_DIR / 'subprocess_output'
LOG_RETENTION_DAYS = 7

# Scripts to run
SCRIPTS_TO_RUN = [
    SCRIPT_DIR / '1_tickers.py',
    SCRIPT_DIR / '2_pull.py',
]

# --- Utility Functions ---------------------------------------------

def setup_logging():
    """Set up main scheduler logging."""
    try:
        BASE_LOG_DIR.mkdir(parents=True, exist_ok=True)
        SUBPROCESS_LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to create log directories: {e}", file=sys.stderr)
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            RotatingFileHandler(MAIN_LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("Main scheduler logging configured successfully.")


def cleanup_old_logs():
    """Delete old subprocess logs."""
    logging.info(f"Cleaning old subprocess logs (retention: {LOG_RETENTION_DAYS} days)")
    now = datetime.now()
    cutoff_time = now - timedelta(days=LOG_RETENTION_DAYS)
    deleted_count = 0

    for filename in os.listdir(SUBPROCESS_LOG_DIR):
        file_path = SUBPROCESS_LOG_DIR / filename
        if file_path.is_file():
            try:
                if datetime.fromtimestamp(file_path.stat().st_mtime) < cutoff_time:
                    file_path.unlink()
                    deleted_count += 1
                    logging.debug(f"Deleted old log: {file_path}")
            except Exception as e:
                logging.error(f"Error deleting {file_path}: {e}")

    if deleted_count:
        logging.info(f"Deleted {deleted_count} old log file(s).")
    else:
        logging.info("No old log files to delete.")


def run_script_subprocess(script_path):
    """Run one Python script as subprocess with real-time logging."""
    script_name = script_path.stem
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    combined_log_path = SUBPROCESS_LOG_DIR / f"{script_name}_output_{timestamp}.log"

    logging.info(f"Resolved script path: {script_path}")
    if not script_path.exists():
        logging.error(f"Script not found at: {script_path}")
        return False

    logging.info(f"Running script: {script_name}")
    try:
        with open(combined_log_path, 'w') as log_file:
            process = subprocess.Popen(
                [sys.executable, str(script_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=os.environ
            )
            logging.info(f"Subprocess for '{script_name}' started (PID: {process.pid})")

            for line in process.stdout:
                log_file.write(line)
                log_file.flush()

            process.wait()

        if process.returncode != 0:
            logging.error(f"Script '{script_name}' FAILED with exit code {process.returncode}")
            try:
                with open(combined_log_path, 'r') as f:
                    last_lines = "\n".join(f.readlines()[-100:]).strip()
                    if last_lines:
                        logging.error(f"Last 100 lines of output:\n{last_lines}")
                    else:
                        logging.warning("Combined log was empty.")
            except Exception as e:
                logging.warning(f"Failed to read combined log: {e}")
            return False

        logging.info(f"Script '{script_name}' completed successfully.")
        return True

    except Exception as e:
        logging.exception(f"Unexpected error running script '{script_name}': {e}")
        return False


# --- APScheduler Job ----------------------------------------------

def run_main_sequence():
    """Run all scripts in defined sequence."""
    logging.info("--- Scheduled job triggered. Starting script sequence. ---")
    start_time = datetime.now(timezone.utc)
    all_succeeded = True

    cleanup_old_logs()

    for script in SCRIPTS_TO_RUN:
        script_start_time = datetime.now(timezone.utc)
        logging.info(f"--- Starting: {script.name} ---")
        if not run_script_subprocess(script):
            all_succeeded = False
            logging.error(f"ABORTING sequence due to failure: {script.name}")
            break
        script_duration = datetime.now(timezone.utc) - script_start_time
        logging.info(f"--- Finished {script.name}. Duration: {script_duration} ---")

    total_duration = datetime.now(timezone.utc) - start_time
    if all_succeeded:
        logging.info(f"All scripts completed successfully. Total duration: {total_duration}")
    else:
        logging.error(f"Script sequence finished with failures. Total duration: {total_duration}")

    logging.info("--- Job complete. ---")


# --- Main ----------------------------------------------------------

def main():
    setup_logging()
    logging.info("Scheduler script launched.")
    logging.info(f"Scheduled to run daily at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} UTC")

    scheduler = BlockingScheduler(timezone=UTC_TIMEZONE)

    # Daily job
    scheduler.add_job(
        run_main_sequence,
        CronTrigger(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE),
        id='main_script_sequence',
        name='Daily Script Execution',
        max_instances=1,
        misfire_grace_time=600
    )

    # ------------------------------------------------------------------
    # Quick “minutes-till-next-run” helper
    # ------------------------------------------------------------------
    import datetime as dt

    now_utc = dt.datetime.now(dt.timezone.utc)
    next_run_utc = now_utc.replace(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE, second=0, microsecond=0)
    if next_run_utc <= now_utc:
        next_run_utc += dt.timedelta(days=1)

    minutes_left = int((next_run_utc - now_utc).total_seconds() / 60)

    print(f"Current UTC time   : {now_utc.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"Next scheduled run : {next_run_utc.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"Time until next run: ≈ {minutes_left} minutes")
    # ------------------------------------------------------------------

    def graceful_shutdown(signum=None, frame=None):
        logging.info(f"Shutdown signal received: {signum}. Shutting down scheduler...")
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    atexit.register(graceful_shutdown)

    logging.info("Scheduler configured and starting...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler process shut down gracefully.")
    except Exception as e:
        logging.critical(f"Fatal scheduler error: {e}", exc_info=True)


# ------------------------------------------------------------------
# Entry-point guard
# ------------------------------------------------------------------
if __name__ == "__main__":
    TEST_MODE = False  # Set to True to run sequence once immediately

    if TEST_MODE:
        print("--- TEST MODE: Running script sequence immediately ---")
        setup_logging()
        run_main_sequence()
        print("--- TEST MODE Finished ---")
    else:
        main()
