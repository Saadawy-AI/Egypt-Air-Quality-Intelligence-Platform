"""Optional, schema-validating bridge from a public Wokwi MQTT broker to local Mosquitto.

The public broker is an untrusted development ingress. Only messages under a
high-entropy topic prefix are accepted; payloads are validated and re-labeled
as simulated before entering the existing local MQTT -> Kafka bridge.
"""
import json, logging, os, re, signal
from datetime import datetime
from threading import Event
import paho.mqtt.client as mqtt

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log=logging.getLogger("WokwiMQTTBridge")
PUBLIC_HOST=os.getenv("WOKWI_MQTT_HOST","broker.hivemq.com")
PUBLIC_PORT=int(os.getenv("WOKWI_MQTT_PORT","1883"))
LOCAL_HOST=os.getenv("MQTT_HOST","mqtt")
LOCAL_PORT=int(os.getenv("MQTT_PORT","1883"))
PREFIX=os.environ["WOKWI_TOPIC_PREFIX"].strip("/")
FILTER=PREFIX+"/#"
ALLOWED={"pm1","pm25","pm10","temperature","humidity"}
GOVERNORATE=os.getenv("WOKWI_GOVERNORATE","Cairo")
GOVERNORATE_AR=os.getenv("WOKWI_GOVERNORATE_AR","القاهرة")
LATITUDE=float(os.getenv("WOKWI_LATITUDE","30.0444"))
LONGITUDE=float(os.getenv("WOKWI_LONGITUDE","31.2357"))

def validate(topic, raw):
    if not topic.startswith(PREFIX+"/"): raise ValueError("topic outside assigned prefix")
    parts=topic.split("/")
    if len(parts)<3 or parts[0]!=PREFIX: raise ValueError("invalid topic structure")
    try: record=json.loads(raw.decode("utf-8"))
    except Exception as exc: raise ValueError("invalid JSON") from exc
    if not isinstance(record,dict) or record.get("schema_version")!="1.0": raise ValueError("unsupported schema")
    device=record.get("device_id")
    if not isinstance(device,str) or not re.fullmatch(r"wokwi-[a-zA-Z0-9_-]{1,48}",device): raise ValueError("invalid Wokwi device id")
    data=record.get("data")
    if not isinstance(data,dict) or not data or not set(data).issubset(ALLOWED): raise ValueError("invalid sensor fields")
    for key,value in data.items():
        if isinstance(value,bool) or not isinstance(value,(int,float)) or not (-100<=value<=1000): raise ValueError(f"invalid {key}")
    timestamp=record.get("timestamp")
    try: datetime.fromisoformat(timestamp.replace("Z","+00:00"))
    except Exception as exc: raise ValueError("invalid timestamp") from exc
    record["source"]="wokwi_esp32"
    record["metadata"]={"device_id":device,"device_type":"wokwi_esp32","simulation":"true","simulation_provider":"wokwi","simulation_profile":"wokwi-esp32-public-mqtt-v1","particulate_input_model":"potentiometer_test_inputs_not_physical_pm_sensor"}
    record["country_code"]="EG"
    record["governorate"]=GOVERNORATE
    record["governorate_ar"]=GOVERNORATE_AR
    record["latitude"]=LATITUDE
    record["longitude"]=LONGITUDE
    record["ingested_at"]=datetime.now().astimezone().isoformat()
    return "egypt/air-quality/simulated/wokwi/"+device, json.dumps(record,separators=(",",":"),allow_nan=False)

class Bridge:
    def __init__(self):
        self.local=mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,client_id="wokwi-local-ingress")
        username=os.getenv("MQTT_USERNAME"); password=os.getenv("MQTT_PASSWORD")
        if not username or not password: raise RuntimeError("Local MQTT credentials are required")
        self.local.username_pw_set(username,password)
        local_ready=Event(); local_error=[]
        def on_local_connect(client,userdata,flags,reason,props):
            if getattr(reason,"is_failure",False): local_error.append(reason)
            local_ready.set()
        self.local.on_connect=on_local_connect
        self.local.connect(LOCAL_HOST,LOCAL_PORT,60); self.local.loop_start()
        if not local_ready.wait(timeout=15):
            raise TimeoutError("Local MQTT broker did not complete the Wokwi bridge connection handshake")
        if local_error:
            raise RuntimeError(f"Local MQTT broker rejected Wokwi bridge credentials: {local_error[0]}")
        self.public=mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,client_id="wokwi-public-ingress")
        self.public.on_connect=self.on_connect; self.public.on_message=self.on_message
    def on_connect(self,client,userdata,flags,reason,props):
        if getattr(reason,"is_failure",False): log.error("Public broker rejected connection: %s",reason); return
        client.subscribe(FILTER,qos=1); log.info("Subscribed to assigned Wokwi topic prefix (redacted)")
    def on_message(self,client,userdata,msg):
        try:
            local_topic,payload=validate(msg.topic,msg.payload)
            info=self.local.publish(local_topic,payload,qos=1)
            info.wait_for_publish(timeout=8)
            if not info.is_published(): raise TimeoutError("local MQTT publish not acknowledged")
            log.info("Validated Wokwi telemetry forwarded to local MQTT | topic=%s",local_topic)
        except Exception as exc: log.warning("Rejected Wokwi message | reason=%s",exc)
    def run(self):
        self.public.connect(PUBLIC_HOST,PUBLIC_PORT,60)
        self.public.loop_forever()
    def close(self):
        self.public.disconnect(); self.public.loop_stop()
        self.local.loop_stop(); self.local.disconnect()

if __name__=="__main__":
    bridge=Bridge()
    signal.signal(signal.SIGTERM,lambda *_: bridge.close())
    try: bridge.run()
    except KeyboardInterrupt: pass
    finally: bridge.close()
