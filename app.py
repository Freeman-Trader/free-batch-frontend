import logging
from flask import Flask, render_template, request, redirect, url_for
from dotenv import load_dotenv
import uuid
import pika
import json
import socket
import os
import mysql.connector

load_dotenv()

app = Flask(__name__)

os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('logs', 'frontend.log')),
        logging.StreamHandler()
    ]
)

app.config['DB_HOST'] = os.getenv('DB_HOST', 'localhost')
app.config['DB_PORT'] = int(os.getenv('DB_PORT', 3306))
app.config['DB_USER'] = os.getenv('DB_USER', 'root')
app.config['DB_PASSWORD'] = os.getenv('DB_PASSWORD', '')
app.config['DB_NAME'] = os.getenv('DB_NAME', 'jobs')
app.config['RABBITMQ_HOST'] = os.getenv('RABBITMQ_HOST', 'localhost')
app.config['RABBITMQ_QUEUE'] = os.getenv('RABBITMQ_QUEUE', 'job_queue')
app.config['FLASK_HOST'] = os.getenv('FLASK_HOST', '0.0.0.0')
app.config['FLASK_PORT'] = int(os.getenv('FLASK_PORT', 5000))
app.config['FLASK_DEBUG'] = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'

try:
    HOST_ID = socket.gethostname() + '_' + socket.gethostbyname(socket.gethostname())
except socket.gaierror:
    HOST_ID = socket.gethostname() + '_unknown-ip'

def get_db_connection():
    conn = mysql.connector.connect(
        host=app.config['DB_HOST'],
        port=app.config['DB_PORT'],
        user=app.config['DB_USER'],
        password=app.config['DB_PASSWORD'],
        database=app.config['DB_NAME']
    )
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id VARCHAR(36) PRIMARY KEY,
            creator VARCHAR(255) NOT NULL,
            process_time INT NOT NULL,
            status VARCHAR(20) DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    cursor.close()
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
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO jobs (id, creator, process_time, status) VALUES (%s, %s, %s, %s)',
            (job_id, creator, process_time, 'pending')
        )
        conn.commit()
        cursor.close()
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
    cursor = conn.cursor(dictionary=True)
    cursor.execute('SELECT * FROM jobs ORDER BY created_at DESC')
    jobs = cursor.fetchall()
    cursor.close()
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