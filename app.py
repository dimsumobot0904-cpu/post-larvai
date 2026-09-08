import sqlite3
import datetime
import threading
import time
import json
import os
import paho.mqtt.client as mqtt
#09/04/26 testing ai ngani
from collections import deque
import joblib
import numpy as np
from tensorflow.keras.models import load_model
#hanggang dito 09/04/26
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

# Mag-save ng huling 10 readings para sa LSTM time-series sequence
sensor_history = deque(maxlen=10)

# RELAY STATES (Kasama na ang feeder)
relay_states = {
    "do": 1,
    "ph": 1,
    "temp": 1,
    "sal": 1,
    "feeder": 0  
}

# 09/08/26 SETTINGS FILE PARA SA PERMANENT STORAGE
SETTINGS_FILE = "settings.json"

def load_settings():
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {"timeline_date": "", "dynamic_schedules": ["06:00", "12:00", "18:00", "00:00"]}

def save_settings(data):
    with open(SETTINGS_FILE, 'w') as f:
        json.dump(data, f)

# I-load ang saved schedules o gamitin ang defaults
app_settings = load_settings()
dynamic_schedules = app_settings.get("dynamic_schedules", ["06:00", "12:00", "18:00", "00:00"])

#09/04/26 ai model loading block 
try:
    scaler = joblib.load('scaler.pkl')
    svm_do = joblib.load('svm_do.pkl')
    svm_ph = joblib.load('svm_ph.pkl')
    svm_temp = joblib.load('svm_temp.pkl')
    lstm_do = load_model('lstm_do.keras')
    lstm_ph = load_model('lstm_ph.keras')
    lstm_temp = load_model('lstm_temp.keras')
    print("All ML models and scaler loaded successfully!")
except Exception as e:
    print(f"Error loading models or scaler: {e}")
    scaler = svm_do = svm_ph = svm_temp = lstm_do = lstm_ph = lstm_temp = None

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
    global latest_sensor_data, sensor_history
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
        
        # --- IDAGDAG ITO: I-save sa history buffer para sa LSTM ---
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
@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    if not session.get('logged_in'): return redirect(url_for('login'))
    
    settings = load_settings()
    
    # Kung nag-save ng timeline date mula sa form
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
    return render_template('feeding.html', settings=settings)

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

#09/04/26 ai model route
@app.route('/predict_ai')
def predict_ai():
    if not session.get('logged_in'): 
        return jsonify({"error": "Unauthorized"}), 401

    if not scaler or not svm_do or not lstm_do:
        return jsonify({"status": "error", "message": "Models not initialized"}), 500

    try:
        # Siguraduhing may sapat na sensor history ang buffer
        if len(sensor_history) < 10:
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
            while len(sensor_history) < 10:
                sensor_history.append(current_features)

        # 1. Sequence processing para kay LSTM (Kunin ang unang 3 columns para sa 3-feature scaler)
        history_array = np.array(list(sensor_history))
        scaled_history = scaler.transform(history_array[:, :3])
        lstm_sequence_input = scaled_history.reshape(1, scaled_history.shape[0], scaled_history.shape[1])
        
        # LSTM Forecast (Hinuhulaan ang future sensor values sa susunod na 30 mins)
        future_do = float(lstm_do.predict(lstm_sequence_input)[0][0])
        future_ph = float(lstm_ph.predict(lstm_sequence_input)[0][0])
        future_temp = float(lstm_temp.predict(lstm_sequence_input)[0][0])

        # 2. I-pasa ang hula kay SVM (Gumamit ng hiwalay at tamang shape para sa bawat model)
        svm_input_temp = np.array([[future_temp]])
        svm_input_ph = np.array([[future_ph]])
        svm_input_do = np.array([[future_do]])

        # SVM Classification / Regression Results
        svm_score_temp = float(svm_temp.predict(svm_input_temp)[0])
        svm_score_ph = float(svm_ph.predict(svm_input_ph)[0])
        svm_score_do = float(svm_do.predict(svm_input_do)[0])
        
        # Logic para sa Safe / Unsafe verdict
        is_safe = True
        if future_ph < 7.0 or future_ph > 9.0 or future_temp < 27.0 or future_temp > 32.0 or future_do < 4.0:
            is_safe = False

        water_status = "SAFE" if is_safe else "UNSAFE"
        risk_color = "#22c55e" if is_safe else "#ef4444"

        return jsonify({
            "status": "success",
            "forecast_temp": round(future_temp, 1),
            "forecast_ph": round(future_ph, 2),
            "forecast_do": round(future_do, 2),
            "svm_status": water_status,
            "risk_color": risk_color
        })
            
    except Exception as e:
        print(f"Hybrid Pipeline Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    #09/04/26

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
        
        # --- ITO ANG KULANG: I-save sa JSON file ---
        settings = load_settings()
        settings['dynamic_schedules'] = dynamic_schedules
        save_settings(settings)
        # -------------------------------------------
        
        print(f"\n[!] Bagong Feeding Schedule na-save sa file: {dynamic_schedules}\n")
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