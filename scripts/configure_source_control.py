from pathlib import Path
import secrets

root = Path(__file__).resolve().parents[1]
compose_path = root / "docker-compose.yml"
env_path = root / ".env"
compose = compose_path.read_text(encoding="utf-8")

if "  docker-socket-proxy:" not in compose:
    anchor = "  streamlit:\n"
    block = """  # Filtered Docker API access for the narrowly allowlisted source controller.
  docker-socket-proxy:
    image: tecnativa/docker-socket-proxy:0.3.0
    environment:
      CONTAINERS: "1"
      POST: "1"
      PING: "1"
      VERSION: "1"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    networks:
      - socket-control
    restart: unless-stopped

  source-control-api:
    build:
      context: ./control_api
    environment:
      CONTROL_API_TOKEN: ${CONTROL_API_TOKEN:?Set CONTROL_API_TOKEN in the ignored .env file}
      DOCKER_API_URL: http://docker-socket-proxy:2375
      COMPOSE_PROJECT_NAME: egypt-air-quality
    depends_on:
      docker-socket-proxy:
        condition: service_started
    networks:
      - control-plane
      - socket-control
    restart: unless-stopped

"""
    if anchor not in compose:
        raise SystemExit("Could not find streamlit service insertion point")
    compose = compose.replace(anchor, block + anchor, 1)

streamlit_anchor = "      ML_METADATA_PATH: /opt/project-models/pm25/pm25_hgb_v2.json\n"
if "CONTROL_API_URL: http://source-control-api:8088" not in compose:
    if streamlit_anchor not in compose:
        raise SystemExit("Could not find Streamlit environment insertion point")
    compose = compose.replace(
        streamlit_anchor,
        streamlit_anchor
        + "      CONTROL_API_URL: http://source-control-api:8088\n"
        + "      CONTROL_API_TOKEN: ${CONTROL_API_TOKEN:?Set CONTROL_API_TOKEN in the ignored .env file}\n",
        1,
    )

net_anchor = "networks:\n  postgres-private:\n"
if "  control-plane:\n" not in compose:
    if net_anchor not in compose:
        raise SystemExit("Could not find network insertion point")
    compose = compose.replace(
        net_anchor,
        "networks:\n"
        "  control-plane:\n"
        "    internal: true\n"
        "  socket-control:\n"
        "    internal: true\n"
        "  postgres-private:\n",
        1,
    )

compose_path.write_text(compose, encoding="utf-8", newline="\n")

env_text = env_path.read_text(encoding="utf-8") if env_path.exists() else ""
updates = []
for key, value in (
    ("CONTROL_API_TOKEN", secrets.token_urlsafe(48)),
    ("WOKWI_TOPIC_PREFIX", secrets.token_hex(16)),
):
    configured = any(
        line.startswith(key + "=") and line.split("=", 1)[1].strip()
        for line in env_text.splitlines()
    )
    if not configured:
        updates.append(f"{key}={value}")

if updates:
    with env_path.open("a", encoding="utf-8", newline="\n") as handle:
        if env_text and not env_text.endswith(("\n", "\r")):
            handle.write("\n")
        handle.write("\n".join(updates) + "\n")
print("Source-control configuration and local random tokens ensured; secret values withheld.")
