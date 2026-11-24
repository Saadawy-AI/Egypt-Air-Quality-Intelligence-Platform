# Optional IoT Source Control API

Internal-only API used by Streamlit to control the two allowlisted optional
Compose services. The always-on OpenAQ and Open-Meteo ingestion sources are
not in the allowlist and cannot be stopped through this API.

- GET /health
- GET /v1/sources (Bearer token required)
- POST /v1/sources/python_iot/start
- POST /v1/sources/python_iot/stop
- POST /v1/sources/wokwi/start
- POST /v1/sources/wokwi/stop

The API does not create containers. Pre-create the profile services with
Docker Compose; missing services are reported as not_deployed. It only acts on
containers whose Compose project and service labels match the fixed allowlist.

Keep this API and docker-socket-proxy off host/public ports. The API is attached
only to private Compose networks shared with Streamlit and the filtered socket
proxy. Store CONTROL_API_TOKEN in the ignored local .env.
