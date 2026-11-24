"""Allowlisted lifecycle controller for optional IoT Compose services."""
import json, os, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

DOCKER_API=os.getenv("DOCKER_API_URL","http://docker-socket-proxy:2375")
TOKEN=os.environ["CONTROL_API_TOKEN"]
ALLOWED={"python_iot":"iot-simulator","wokwi":"wokwi-mqtt-bridge"}
PROJECT=os.getenv("COMPOSE_PROJECT_NAME","egypt-air-quality")

class DockerAPIError(RuntimeError): pass

def docker(path, method="GET", timeout=5):
    try:
        with urlopen(Request(DOCKER_API.rstrip("/")+"/"+path,method=method),timeout=timeout) as r:
            raw=r.read()
            return json.loads(raw) if raw else {}
    except HTTPError as e:
        raise DockerAPIError(f"Docker API {e.code}: {e.read().decode(errors='replace')[:250]}") from e
    except URLError as e:
        raise DockerAPIError(f"Docker API unavailable: {e.reason}") from e

def find(service):
    for item in docker("containers/json?all=1"):
        labels=item.get("Labels") or {}
        if labels.get("com.docker.compose.project")==PROJECT and labels.get("com.docker.compose.service")==service:
            return item
    return None

def status():
    result={}
    for source,service in ALLOWED.items():
        item=find(service)
        result[source]=({"service":service,"state":"not_deployed","running":False} if item is None else
          {"service":service,"state":item.get("State","unknown"),"running":item.get("State")=="running",
           "container_id":item.get("Id","")[:12],"status":item.get("Status","")})
    result["wokwi"]["topic_prefix"] = os.getenv("WOKWI_TOPIC_PREFIX", "")
    return result

def control(source,action):
    if source not in ALLOWED: raise ValueError("Unknown or non-optional source")
    if action not in ("start","stop"): raise ValueError("Action must be start or stop")
    service=ALLOWED[source]; item=find(service)
    if item is None: raise LookupError(f"{service} has not been created by Docker Compose yet")
    cid=quote(item["Id"],safe="")
    if action=="start" and item.get("State")!="running": docker(f"containers/{cid}/start","POST")
    if action=="stop" and item.get("State")=="running": docker(f"containers/{cid}/stop?t=10","POST",15)
    for _ in range(10):
        current=find(service)
        if current and ((action=="start" and current.get("State")=="running") or
                        (action=="stop" and current.get("State")!="running")):
            return {"source":source,"action":action,"result":"ok","state":current.get("State")}
        time.sleep(.25)
    raise DockerAPIError("Requested state was not confirmed by Docker")

class Handler(BaseHTTPRequestHandler):
    def reply(self,code,data):
        body=json.dumps(data).encode(); self.send_response(code)
        self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(body)))
        self.end_headers(); self.wfile.write(body)
    def auth(self): return self.headers.get("Authorization")==f"Bearer {TOKEN}"
    def do_GET(self):
        if self.path=="/health": return self.reply(200,{"status":"ok"})
        if not self.auth(): return self.reply(401,{"error":"unauthorized"})
        if self.path!="/v1/sources": return self.reply(404,{"error":"not_found"})
        try: return self.reply(200,{"sources":status()})
        except DockerAPIError as e: return self.reply(503,{"error":str(e)})
    def do_POST(self):
        if not self.auth(): return self.reply(401,{"error":"unauthorized"})
        p=self.path.strip("/").split("/")
        if len(p)!=4 or p[:2]!=["v1","sources"]: return self.reply(404,{"error":"not_found"})
        try: return self.reply(200,control(p[2],p[3]))
        except ValueError as e: return self.reply(400,{"error":str(e)})
        except LookupError as e: return self.reply(409,{"error":str(e),"state":"not_deployed"})
        except DockerAPIError as e: return self.reply(503,{"error":str(e)})
    def log_message(self,fmt,*args): print("control-api:",fmt%args,flush=True)

if __name__=="__main__":
    print("IoT source control API listening on :8088",flush=True)
    ThreadingHTTPServer(("0.0.0.0",8088),Handler).serve_forever()
