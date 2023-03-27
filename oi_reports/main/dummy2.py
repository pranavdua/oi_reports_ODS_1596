from threading import Thread
from flask import Flask, jsonify
from apscheduler.schedulers.blocking import BlockingScheduler

class SchedulerAPI:

    def __init__(self):
        self.app = Flask(__name__)
        self.scheduler = BlockingScheduler()

        @self.app.route('/next_run_times')
        def get_next_run_times():
            results = {}
            for job in self.scheduler.get_jobs():
                try:
                    next_run_time = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S')
                    results[job.id] = next_run_time
                except AttributeError:
                    results[job.id] = 'Job has not yet been scheduled'
            return jsonify(results)

        self.scheduler.add_job(self.job_function, 'interval', seconds=10)
        self.scheduler.add_job(self.job_function, 'cron', day_of_week='mon-fri', hour=12)

    def start_scheduler(self):
        self.scheduler.start()

    def start_app(self):
        self.app.run(debug=True)

    def start(self):
        scheduler_thread = Thread(target=self.start_scheduler)
        scheduler_thread.start()
        self.start_app()

    def job_function(self):
        print("Running job...")
