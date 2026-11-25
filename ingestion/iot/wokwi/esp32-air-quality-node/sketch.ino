#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <DHT.h>
#include <time.h>

// Development-only Wokwi public gateway -> public MQTT broker.
// Copy WOKWI_TOPIC_PREFIX from the Streamlit IoT Sources Control panel here.
const char* WIFI_SSID = "Wokwi-GUEST";
const char* WIFI_PASSWORD = "";
const char* MQTT_HOST = "broker.hivemq.com";
const int MQTT_PORT = 1883;
const char* TOPIC_PREFIX = "REPLACE_WITH_WOKWI_TOPIC_PREFIX";
const char* DEVICE_ID = "wokwi-esp32-01";

constexpr int PIN_PM1 = 32;
constexpr int PIN_PM25 = 33;
constexpr int PIN_PM10 = 34;
constexpr int PIN_DHT = 15;
constexpr int PIN_STATUS_LED = 2;
DHT dht(PIN_DHT, DHT22);

WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);
unsigned long lastPublish = 0;

float scalePotentiometer(int pin, float minValue, float maxValue) {
  const int raw = analogRead(pin);
  return minValue + (maxValue - minValue) * (float)raw / 4095.0f;
}

void connectWifi() {
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD, 6);
  while (WiFi.status() != WL_CONNECTED) delay(250);
}

void connectMqtt() {
  mqttClient.setServer(MQTT_HOST, MQTT_PORT);
  mqttClient.setBufferSize(512);
  while (!mqttClient.connected()) {
    String clientId = String("wokwi-") + DEVICE_ID;
    if (!mqttClient.connect(clientId.c_str())) delay(1500);
  }
  digitalWrite(PIN_STATUS_LED, HIGH);
}

void setup() {
  Serial.begin(115200);
  pinMode(PIN_STATUS_LED, OUTPUT);
  digitalWrite(PIN_STATUS_LED, LOW);
  analogReadResolution(12);
  dht.begin();
  connectWifi();
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
  connectMqtt();
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) connectWifi();
  if (!mqttClient.connected()) connectMqtt();
  mqttClient.loop();

  if (millis() - lastPublish < 10000) return;
  lastPublish = millis();

  const float temperature = dht.readTemperature();
  const float humidity = dht.readHumidity();
  if (isnan(temperature) || isnan(humidity)) {
    Serial.println("DHT22 read failed; skipping this sample.");
    return;
  }

  // Potentiometers are synthetic test inputs, NOT calibrated PM sensors.
  const float pm1 = scalePotentiometer(PIN_PM1, 1.0f, 80.0f);
  const float pm25 = scalePotentiometer(PIN_PM25, 2.0f, 120.0f);
  const float pm10 = scalePotentiometer(PIN_PM10, 5.0f, 200.0f);

  const time_t now = time(nullptr);
  if (now < 1700000000) {
    Serial.println("UTC clock is not synchronized; skipping sample.");
    return;
  }
  struct tm tmUtc;
  gmtime_r(&now, &tmUtc);
  char stamp[25];
  strftime(stamp, sizeof(stamp), "%Y-%m-%dT%H:%M:%SZ", &tmUtc);

  StaticJsonDocument<512> doc;
  doc["schema_version"] = "1.0";
  doc["device_id"] = DEVICE_ID;
  doc["timestamp"] = stamp;
  JsonObject data = doc.createNestedObject("data");
  data["pm1"] = pm1;
  data["pm25"] = pm25;
  data["pm10"] = pm10;
  data["temperature"] = temperature;
  data["humidity"] = humidity;

  char payload[512];
  const size_t length = serializeJson(doc, payload, sizeof(payload));
  const String topic = String(TOPIC_PREFIX) + "/esp32/telemetry";
  if (mqttClient.publish(topic.c_str(), (const uint8_t*)payload, length, false)) {
    Serial.printf("Published simulated telemetry for %s\n", DEVICE_ID);
    digitalWrite(PIN_STATUS_LED, HIGH);
  } else {
    Serial.println("MQTT publish failed.");
    digitalWrite(PIN_STATUS_LED, LOW);
  }
}
