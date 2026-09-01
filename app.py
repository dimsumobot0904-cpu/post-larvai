import sqlite3
import datetime
import threading
import time
import json
import paho.mqtt.client as mqtt
from flask import Flask, request, jsonify, render_template, redirect, url_for, session

#Ceddy Espanol
#last na to

app = Flask(__name__)
app.secret_key = 'hatchguard_secret_key_123'
DB_NAME = "ulang_data.db"

# MQTT Configuration
MQTT_BROKER = "localhost" 
MQTT_PORT = 1883
TOPIC_SENSOR = "hatchguard/sensors"
TOPIC_RELAY = "hatchguard/relay"

latest_sensor_data = {
    "temp": "29.1",
    "ph": "7.7",
    "do": "5.23",
    "salinity": "12",
    "ammonia": "0.02",
    "nitrate": "12.0",
    "nitrite": "0.04",
    "turbidity": "18.3"
}

# RELAY STATES (Kasama na ang feeder)
relay_states = {
    "do": 1,
    "ph": 1,
    "temp": 1,
    "sal": 1,
    "feeder": 0  
}

# DEFAULT DYNAMIC SCHEDULES (24-Hour Format: HH:MM)
dynamic_schedules = ["06:00", "12:00", "18:00", "00:00"] 

# --- BACKGROUND TIMER PARA SA SCHEDULED FEEDING ---
def check_schedule():
    global dynamic_schedules
    while True:
        # Kunin ang kasalukuyang oras ng Raspberry Pi
        current_time = datetime.datetime.now().strftime("%H:%M")
        
        if current_time in dynamic_schedules:
            payload = json.dumps({"relay": "feeder", "state": 1})
            mqtt_client.publish(TOPIC_RELAY, payload)
            print(f"\n⏰ Scheduled Feeding Triggered at {current_time}!\n")
            
            # Mag-sleep ng 61 seconds para isang beses lang maghulog per minuto
            time.sleep(61) 
        
        # Mag-check ulit makalipas ang 10 segundo
        time.sleep(10)

# --- MQTT CALLBACK KUNG MAY DATA MULA ESP32 ---
def on_message(client, userdata, msg):
    global latest_sensor_data
    try:
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload) 
        
        # I-update ang live data
        if "temp" in data: latest_sensor_data["temp"] = str(data["temp"])
        if "ph" in data: latest_sensor_data["ph"] = str(data["ph"])
        if "do" in data: latest_sensor_data["do"] = str(data["do"])
        if "ammonia" in data: latest_sensor_data["ammonia"] = str(data["ammonia"])
        if "nitrate" in data: latest_sensor_data["nitrate"] = str(data["nitrate"])
        if "nitrite" in data: latest_sensor_data["nitrite"] = str(data["nitrite"])
        if "turbidity" in data: latest_sensor_data["turbidity"] = str(data["turbidity"])
        
        print(f"Live Data Updated: {latest_sensor_data}")
    except Exception as e:
        print(f"Error sa pagbasa ng MQTT message: {e}")

# Setup MQTT Client
try:
	mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
except AttributeError:
	mqtt_client = mqtt.Client()

mqtt_client.on_message = on_message

def start_mqtt():
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.subscribe(TOPIC_SENSOR)
        mqtt_client.loop_start()
        print("Konektado na sa MQTT Broker!")
    except Exception as e:
        print(f"MQTT Connection Error: {e}")

# DATABASE SETUP
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            dissolved_oxygen REAL, ph REAL, temperature REAL, 
            salinity REAL, ammonia REAL, nitrate_nitrite REAL, turbidity REAL
        )
    ''')
    conn.commit()
    conn.close()


# ========================================================
# FLASK WEB ROUTES & ENDPOINTS
# ========================================================

USER_DATA = {"dimsumobot": "strongpassword"}

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form.get('username') in USER_DATA and USER_DATA[request.form.get('username')] == request.form.get('password'):
            session['logged_in'] = True
            return redirect(url_for('dashboard'))
        error = 'Invalid Username or Password!'
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/')
@app.route('/dashboard')
def dashboard():
    if not session.get('logged_in'): return redirect(url_for('login'))
    return render_template('index.html')

@app.route('/sensors')
def sensors():
    if not session.get('logged_in'): return redirect(url_for('login'))
    return render_template('sensors.html')

@app.route('/feeding')
def feeding():
    if not session.get('logged_in'): return redirect(url_for('login'))
    return render_template('feeding.html')

@app.route('/about')
def about():
    if not session.get('logged_in'): return redirect(url_for('login'))
    return render_template('about.html')

@app.route('/tutorial')
def tutorial():
    if not session.get('logged_in'): return redirect(url_for('login'))
    return render_template('tutorial.html')

@app.route('/chat')
def chat():
    if not session.get('logged_in'): return redirect(url_for('login'))
    return render_template('chat.html')

@app.route('/get_data')
def get_data():
    if not session.get('logged_in'): return jsonify({"error": "Unauthorized"}), 401
    return jsonify(latest_sensor_data)


# --- ROUTE PARA I-UPDATE ANG FEEDING SCHEDULE MULA SA WEB ---
@app.route('/update_schedule', methods=['POST'])
def update_schedule():
    global dynamic_schedules
    if not session.get('logged_in'): return jsonify({"error": "Unauthorized"}), 401
    
    data = request.get_json()
    new_schedules = data.get('schedules')
    
    # I-verify kung valid na listahan ang ipinasa
    if new_schedules and isinstance(new_schedules, list):
        dynamic_schedules = new_schedules
        print(f"\n[!] Bagong Feeding Schedule na-save: {dynamic_schedules}\n")
        return jsonify({"status": "success", "schedules": dynamic_schedules})
    
    return jsonify({"status": "error", "message": "Invalid data format"}), 400


# --- ROUTE PARA SA MANUAL BUTTON (AERATOR/HEATER/PUMP/FEEDER) ---
@app.route('/control_relay', methods=['POST'])
def control_relay():
    if not session.get('logged_in'): return jsonify({"error": "Unauthorized"}), 401
    
    data = request.get_json()
    relay = data.get('relay')
    state = data.get('state')
    
    if relay in relay_states:
        relay_states[relay] = state
        
        # I-publish ang command sa ESP32 sa pamamagitan ng MQTT
        payload = json.dumps({"relay": relay, "state": state})
        mqtt_client.publish(TOPIC_RELAY, payload)
        print(f"Manual Command Sent: {payload}")
        
        return jsonify({"status": "success", "relay": relay, "state": state})
    
    return jsonify({"status": "error", "message": "Invalid relay"}), 400


if __name__ == '__main__':
    init_db()
    start_mqtt()
    
    # I-start ang background timer sa sarili nitong proseso
    threading.Thread(target=check_schedule, daemon=True).start()
    
    # use_reloader=False para iwas duplicate process na nagla-lock ng MQTT
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)