from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
import logging 
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

def pipeline_job():
    """
    Entry point called by APScheduler on each trigger
    Import run_pipeline here to avoid circular imports
    """
    from main import run_pipeline
    target_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    logger.info(f'Scheduler triggered - target date: {target_date}')

    run_pipeline(target_date=target_date)

def on_job_executed(event):
    logger.info(f"Scheduled run completed successfully")

def on_job_error(event):
    logger.error(
        f"Scheduled run failed: {event.exception}", exc_info=True
    )

def start_scheduler():
    """
    Configures and starts the blocking scheduler
    
    Why BlockingScheduler?
    This process exists only to run the pipeline on a schedule.
    Blocking means the scheduler occupies the main thread - 
    appropriate for a dedicated pipeline process.
    For a web app running a pipeline in the background, 
    you'd use BackgroundScheduler instead
    
    misfire_grace_time = 3600:
    If the scheduler wakes up and fineds a missed run within the last hour, 
    it runs immediately. Beyond one hour, the missed run is logged
    but not automatically executed - manual backfill required.
    """
    scheduler = BlockingScheduler()

    scheduler.add_job(
        pipeline_job, trigger="cron",
        hour=1, minute=0, misfire_grace_time=3600,
        id="weather_pipeline"
    )

    scheduler.add_listener(on_job_executed, EVENT_JOB_EXECUTED)
    scheduler.add_listener(on_job_error, EVENT_JOB_ERROR)

    logger.info(f"Scheduler started - pipeline runs daily at 01:00 UTC")
    logger.info("Press Ctrl+C stop")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info(f"Scheduler stopped manually")
        scheduler.shutdown()