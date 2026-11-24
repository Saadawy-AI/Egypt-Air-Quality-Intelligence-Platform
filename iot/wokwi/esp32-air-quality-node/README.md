# Wokwi ESP32 optional source

This folder contains a Wokwi ESP32 DevKit circuit and a simulated sensor node.
It is an optional source; it does not control or affect OpenAQ/Open-Meteo.

## Sensor model and limitations
- DHT22: simulated temperature and relative humidity.
- Three potentiometers: synthetic PM1, PM2.5 and PM10 test values.
- Potentiometers are **not calibrated particulate sensors**. Their values
  must always be treated as synthetic simulation data, never real observations.
- Every record is re-labeled by the local bridge as
  `source=wokwi_esp32`, `device_type=wokwi_esp32`, `simulation=true`.

## Setup
1. Open the Wokwi project using `diagram.json` and `sketch.ino`.
2. Ensure the Wokwi libraries PubSubClient, ArduinoJson, DHT sensor library,
   and Adafruit Unified Sensor are available.
3. Open the Streamlit **IoT Sources Control** section. Copy the displayed
   Wokwi topic prefix into `TOPIC_PREFIX` in `sketch.ino`.
4. Start the Wokwi simulation. It publishes every 10 seconds to
   `<prefix>/esp32/telemetry` at `broker.hivemq.com:1883`.
5. Start **Wokwi ESP32** from Streamlit. The optional bridge validates the
   message and republishes it to local Mosquitto. The existing MQTT-to-Kafka
   bridge then forwards it to Kafka topic `air-quality`.
6. Stop Wokwi ESP32 from Streamlit. This stops only the bridge; it does not
   stop the core MQTT bridge, Kafka, Airflow, Spark, or the three API sources.

## Trust boundary
The free public broker is unauthenticated and plaintext MQTT. The random
topic prefix and strict schema/range checks reduce accidental/unwanted ingress,
but do not make a public broker a production security boundary. Do not send
secrets or personal data. This path is for development/testing only. For
production/Azure, replace it with authenticated TLS-protected MQTT ingress.
