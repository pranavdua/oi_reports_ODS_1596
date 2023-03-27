from threading import Thread
from flask import Flask, jsonify
from apscheduler.schedulers.blocking import BlockingScheduler

app = Flask(__name__)

def job_function():
    print("Running job...")

scheduler = BlockingScheduler()
scheduler.add_job(job_function, 'interval', seconds=10)
scheduler.add_job(job_function, 'cron', day_of_week='mon-fri', hour=12)

def start_scheduler():
    scheduler.start()

@app.route('/next_run_times')
def get_next_run_times():
    results = {}
    for job in scheduler.get_jobs():
        try:
            next_run_time = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S')
            results[job.id] = next_run_time
        except AttributeError:
            results[job.id] = 'Job has not yet been scheduled'
    return jsonify(results)

if __name__ == '__main__':
    scheduler_thread = Thread(target=start_scheduler)
    scheduler_thread.start()
    app.run(debug=True)
