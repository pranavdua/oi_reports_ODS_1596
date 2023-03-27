import json
import logging
import logging.config
import os
import pytz
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
from threading import Thread

from src.move_sheets import MoveSheets
from remote_logging import build_dd_api_key_json, init_remote_log_handler
from flask import Flask, jsonify

logging.config.fileConfig(fname="log.conf", disable_existing_loggers=False)

# Define a class to encapsulate the app
class FantasticApp:
    def __init__(self):
        self.app = Flask(__name__)
        self.config = self._load_config()
        self.logger = logging.getLogger(__name__)
        self.scheduler = BlockingScheduler()
        self.google_client = bigquery.Client()
        with open(os.environ["SECRET_API_CREDS"]) as f:
            self.dd_secrets = json.load(f)
        self.credentials = service_account.Credentials.from_service_account_file(
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=self.config["SCOPES"]
        )

    def run(self):
        self._configure_routes()
        self.scheduler_thread = Thread(target=self._start_scheduler)
        self.scheduler_thread.start()
        time.sleep(5)
        self.app.run(debug=True, use_reloader=False)

    def _configure_routes(self):
        self.app.add_url_rule("/next_run_times", "next_run_times", self.get_next_run_times)
        self.app.add_url_rule("/move_sheets", "move_sheets", self.run_move_sheets)

    def _start_scheduler(self):
        self._schedule_move_sheets()

        try:
            self.scheduler.start()
        except KeyboardInterrupt:
            self.logger.info("Scheduler stopped by user")

    def _schedule_move_sheets(self):
        day_of_week = "mon"

        self.scheduler.add_job(
            self._move_sheets_runner,
            trigger="cron",
            day_of_week=day_of_week,
            hour="07",
            minute="30",
            timezone=pytz.timezone("UTC"),
        )

    def _load_config(self):
        with open("config/config_file.json") as config_file:
            return json.load(config_file)

    def _move_sheets_runner(self):
        try:
            self.logger.info("Starting move sheets run...")
            # move_sheets = MoveSheets(self.credentials, self._load_config(), self.google_client)
            # move_sheets.create_target_spreadsheet_runner()
            self.logger.info("triggered manually")
        except HttpError as error:
            raise error

    def get_next_run_times(self):
        results = {}
        for job in self.scheduler.get_jobs():
            try:
                next_run_time = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
                results[job.id] = next_run_time
            except AttributeError:
                results[job.id] = "Job has not yet been scheduled"
        return jsonify(results)

    def run_move_sheets(self):
        try:
            self._move_sheets_runner()
            return "Move sheets job started successfully!"
        except Exception as e:
            return f"Error occurred while running move sheets job: {e}", 500


if __name__ == "__main__":
    app = FantasticApp()
    app.run()
