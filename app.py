import sqlite3
import datetime
import threading
import time
import json
import os
import paho.mqtt.client as mqtt
from collections import deque
import joblib
import numpy as np
import tensorflow as tf
from flask import Flask, request, jsonify, render_template, redirect, url_for, session

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

# Buffer para sa huling 10 readings
sensor_history = deque(maxlen=10)

# RELAY STATES (Kasama na ang feeder)
relay_states = {
    "do": 1,
    "ph": 1,
    "temp": 1,
    "sal": 1,
    "feeder": 0  
}

# SETTINGS FILE PARA SA PERMANENT STORAGE
SETTINGS_FILE = "settings.json"

def load_settings():
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {"timeline_date": "", "dynamic_schedules": ["06:00", "12:00", "18:00", "00:00"]}

def save_settings(data):
    with open(SETTINGS_FILE, 'w') as f:
        json.dump(data, f)

# I-load ang saved schedules o gamitin ang defaults
app_settings = load_settings()
dynamic_schedules = app_settings.get("dynamic_schedules", ["06:00", "12:00", "18:00", "00:00"])

# ========================================================
# AI MODEL LOADING & HELPER FUNCTIONS
# ========================================================
try:
    scaler = joblib.load('scaler.pkl')
    
    lstm_temp = tf.keras.models.load_model('lstm_temp.keras')
    svm_temp = joblib.load('svm_temp.pkl')

    lstm_ph = tf.keras.models.load_model('lstm_ph.keras')
    svm_ph = joblib.load('svm_ph.pkl')

    lstm_do = tf.keras.models.load_model('lstm_do.keras')
    svm_do = joblib.load('svm_do.pkl')
    print("✅ All AI models loaded successfully!")
except Exception as e:
    print(f"❌ Error loading models: {e}")
    scaler = lstm_temp = svm_temp = lstm_ph = svm_ph = lstm_do = svm_do = None

def _unscale(value, index):
    """Helper function to reverse scaling for 3 parameters"""
    dummy = np.zeros((1, 3))
    dummy[0, index] = value
    return scaler.inverse_transform(dummy)[0, index]

# ========================================================
# BACKGROUND TIMERS & MQTT
# ========================================================

def check_schedule():
    global dynamic_schedules
    while True:
        current_time = datetime.datetime.now().strftime("%H:%M")
        
        if current_time in dynamic_schedules:
            payload = json.dumps({"relay": "feeder", "state": 1})
            mqtt_client.publish(TOPIC_RELAY, payload)
            print(f"\n⏰ Scheduled Feeding Triggered at {current_time}!\n")
            time.sleep(61) 
        
        time.sleep(10)

def on_message(client, userdata, msg):
    global latest_sensor_data, sensor_history
    try:
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload) 
        
        if "temp" in data: latest_sensor_data["temp"] = str(data["temp"])
        if "ph" in data: latest_sensor_data["ph"] = str(data["ph"])
        if "do" in data: latest_sensor_data["do"] = str(data["do"])
        if "ammonia" in data: latest_sensor_data["ammonia"] = str(data["ammonia"])
        if "nitrate" in data: latest_sensor_data["nitrate"] = str(data["nitrate"])
        if "nitrite" in data: latest_sensor_data["nitrite"] = str(data["nitrite"])
        if "turbidity" in data: latest_sensor_data["turbidity"] = str(data["turbidity"])
        if "salinity" in data: latest_sensor_data["salinity"] = str(data["salinity"])
        
        # Save to buffer
        current_features = [
            float(latest_sensor_data["temp"]),
            float(latest_sensor_data["ph"]),
            float(latest_sensor_data["do"]),
            float(latest_sensor_data["salinity"]),
            float(latest_sensor_data["ammonia"]),
            float(latest_sensor_data["nitrate"]),
            float(latest_sensor_data["nitrite"]),
            float(latest_sensor_data["turbidity"])
        ]
        sensor_history.append(current_features)
        
        print(f"Live Data Updated & Buffered: {latest_sensor_data}")
    except Exception as e:
        print(f"Error sa pagbasa ng MQTT message: {e}")

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
@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    if not session.get('logged_in'): return redirect(url_for('login'))
    
    settings = load_settings()
    
    if request.method == 'POST':
        timeline = request.form.get('timeline_date')
        if timeline is not None:
            settings['timeline_date'] = timeline
            save_settings(settings)
        return redirect(url_for('dashboard'))
        
    return render_template('index.html', settings=settings)

@app.route('/sensors')
def sensors():
    if not session.get('logged_in'): return redirect(url_for('login'))
    return render_template('sensors.html')

@app.route('/feeding')
def feeding():
    if not session.get('logged_in'): return redirect(url_for('login'))
    settings = load_settings()
    
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    default_start = request.args.get('start_date', datetime.datetime.now().replace(day=1).strftime('%Y-%m-%d'))
    default_end = request.args.get('end_date', today)
    status = request.args.get('status', 'All Statuses')
    
    return render_template('feeding.html', 
                         settings=settings, 
                         start_date=default_start, 
                         end_date=default_end,
                         selected_status=status)

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

# ========================================================
# COMPRESSED AI PREDICTION ROUTE
# ========================================================
@app.route('/predict_ai')
def predict_ai():
    if not session.get('logged_in'): 
        return jsonify({"error": "Unauthorized"}), 401

    if not scaler or not lstm_temp:
        return jsonify({"status": "error", "message": "Models not initialized"}), 500

    try:
        # Extract ONLY the first 3 features (Temp, pH, DO) 
        required_features_history = [row[:3] for row in list(sensor_history)]
        
        current_3_features = [
            float(latest_sensor_data["temp"]),
            float(latest_sensor_data["ph"]),
            float(latest_sensor_data["do"])
        ]
        
        # Pad the local list if we have fewer than 10 readings
        if len(required_features_history) < 10:
            missing = 10 - len(required_features_history)
            required_features_history = ([current_3_features] * missing) + required_features_history

        # Array shaping
        data_arr = np.array(required_features_history)
        scaled_input = scaler.transform(data_arr)
        
        # Reshape for LSTM (1, 10, 3) and SVM (1, 30)
        lstm_in = np.expand_dims(scaled_input, axis=0)
        svm_in = lstm_in.reshape(1, -1)

        # Generate Predictions
        pred_lstm_temp = _unscale(lstm_temp.predict(lstm_in, verbose=0)[0][0], 0)
        pred_svm_temp = _unscale(svm_temp.predict(svm_in)[0], 0)

        pred_lstm_ph = _unscale(lstm_ph.predict(lstm_in, verbose=0)[0][0], 1)
        pred_svm_ph = _unscale(svm_ph.predict(svm_in)[0], 1)

        pred_lstm_do = _unscale(lstm_do.predict(lstm_in, verbose=0)[0][0], 2)
        pred_svm_do = _unscale(svm_do.predict(svm_in)[0], 2)

        # Evaluate Safe / Unsafe Status
        svm_temp_safe = 28.0 <= pred_svm_temp <= 31.0
        svm_ph_safe = 7.5 <= pred_svm_ph <= 8.5
        svm_do_safe = pred_svm_do >= 5.0
        
        is_safe = svm_temp_safe and svm_ph_safe and svm_do_safe
        
        water_status = "SAFE" if is_safe else "UNSAFE"
        risk_color = "#22c55e" if is_safe else "#ef4444"

        return jsonify({
            "status": "success",
            "forecast_temp": round(float(pred_lstm_temp), 2),
            "forecast_ph": round(float(pred_lstm_ph), 2),
            "forecast_do": round(float(pred_lstm_do), 2),
            "svm_status": water_status,
            "risk_color": risk_color
        })
            
    except Exception as e:
        print(f"Hybrid Pipeline Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/update_schedule', methods=['POST'])
def update_schedule():
    global dynamic_schedules
    if not session.get('logged_in'): return jsonify({"error": "Unauthorized"}), 401
    
    data = request.get_json()
    new_schedules = data.get('schedules')
    
    if new_schedules and isinstance(new_schedules, list):
        dynamic_schedules = new_schedules
        
        settings = load_settings()
        settings['dynamic_schedules'] = dynamic_schedules
        save_settings(settings)
        
        print(f"\n[!] Bagong Feeding Schedule na-save sa file: {dynamic_schedules}\n")
        return jsonify({"status": "success", "schedules": dynamic_schedules})
    
    return jsonify({"status": "error", "message": "Invalid data format"}), 400

@app.route('/control_relay', methods=['POST'])
def control_relay():
    if not session.get('logged_in'): return jsonify({"error": "Unauthorized"}), 401
    
    data = request.get_json()
    relay = data.get('relay')
    state = data.get('state')
    
    if relay in relay_states:
        relay_states[relay] = state
        
        payload = json.dumps({"relay": relay, "state": state})
        mqtt_client.publish(TOPIC_RELAY, payload)
        print(f"Manual Command Sent: {payload}")
        
        return jsonify({"status": "success", "relay": relay, "state": state})
    
    return jsonify({"status": "error", "message": "Invalid relay"}), 400


if __name__ == '__main__':
    init_db()
    start_mqtt()
    
    threading.Thread(target=check_schedule, daemon=True).start()
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)