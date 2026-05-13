import logging
from flask import Flask, render_template, request, redirect, url_for
from dotenv import load_dotenv
import uuid
import pika
import json
import socket
import os

load_dotenv()

app = Flask(__name__)

os.mkdir('logs')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('logs', 'frontend.log')),
        logging.StreamHandler()
    ]
)

app.config['DATABASE'] = os.getenv('DB_PATH', 'jobs.db')
app.config['RABBITMQ_HOST'] = os.getenv('RABBITMQ_HOST', 'localhost')
app.config['RABBITMQ_QUEUE'] = os.getenv('RABBITMQ_QUEUE', 'job_queue')
app.config['DB_PASSWORD'] = os.getenv('DB_PASSWORD', '')
app.config['FLASK_HOST'] = os.getenv('FLASK_HOST', '0.0.0.0')
app.config['FLASK_PORT'] = int(os.getenv('FLASK_PORT', 5000))
app.config['FLASK_DEBUG'] = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'

try:
    HOST_ID = socket.gethostname() + '_' + socket.gethostbyname(socket.gethostname())
except socket.gaierror:
    HOST_ID = socket.gethostname() + '_unknown-ip'

def get_db_connection():
    import sqlite3
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            creator TEXT NOT NULL,
            process_time INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def publish_to_rabbitmq(job_data):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=app.config['RABBITMQ_HOST']))
    channel = connection.channel()
    channel.queue_declare(queue=app.config['RABBITMQ_QUEUE'], durable=True)
    channel.basic_publish(
        exchange='',
        routing_key=app.config['RABBITMQ_QUEUE'],
        body=json.dumps(job_data),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    connection.close()

@app.route('/')
def index():
    return redirect(url_for('create_job'))

@app.route('/create', methods=['GET', 'POST'])
def create_job():
    if request.method == 'POST':
        job_id = request.form.get('job_id', str(uuid.uuid4()))
        creator = request.form['creator']
        process_time = int(request.form['process_time'])

        job_data = {
            'id': job_id,
            'creator': creator,
            'process_time': process_time
        }

        conn = get_db_connection()
        conn.execute(
            'INSERT INTO jobs (id, creator, process_time, status) VALUES (?, ?, ?, ?)',
            (job_id, creator, process_time, 'pending')
        )
        conn.commit()
        conn.close()

        try:
            publish_to_rabbitmq(job_data)
        except Exception as e:
            print(f"RabbitMQ connection failed: {e}")

        return redirect(url_for('view_jobs'))

    job_id = str(uuid.uuid4())
    return render_template('create_job.html', job_id=job_id)

@app.route('/jobs')
def view_jobs():
    conn = get_db_connection()
    jobs = conn.execute('SELECT * FROM jobs ORDER BY created_at DESC').fetchall()
    conn.close()
    return render_template('view_jobs.html', jobs=jobs)

@app.route('/health')
def health():
    return render_template('health.html', machine_id=HOST_ID)

if __name__ == '__main__':
    init_db()
    app.run(
        debug=app.config['FLASK_DEBUG'],
        host=app.config['FLASK_HOST'],
        port=app.config['FLASK_PORT']
    )