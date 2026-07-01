"""Utils métriques pour Prefect"""
import time
import json
import urllib.request
from datetime import datetime
from typing import Dict, Any, Optional

OTEL_COLLECTOR_URL = "http://otel_collector:4318/v1/metrics"


def get_cpu_usage() -> float:
    """Récupère l'utilisation CPU"""
    try:
        with open("/proc/stat", "r") as f:
            line = f.readline()
            parts = line.split()
            user = int(parts[1])
            nice = int(parts[2])
            system = int(parts[3])
            idle = int(parts[4])
            total = user + nice + system + idle
            return round(((total - idle) / total) * 100, 2) if total > 0 else 0
    except:
        return 0


def get_memory_mb() -> float:
    """Récupère l'utilisation mémoire en MB"""
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    kb = int(line.split()[1])
                    return round(kb / 1024, 2)
    except:
        return 0


def send_metric(name: str, value: float, unit: str = "1", 
                description: str = "", attributes: Optional[Dict] = None):
    """Envoie une métrique à l'OTEL Collector"""
    if attributes is None:
        attributes = {}
    
    payload = {
        "resourceMetrics": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "prefect-pipeline"}},
                    {"key": "service.version", "value": {"stringValue": "1.0.0"}}
                ]
            },
            "scopeMetrics": [{
                "scope": {"name": "pipeline.metrics", "version": "1.0.0"},
                "metrics": [{
                    "name": name,
                    "description": description,
                    "unit": unit,
                    "gauge": {
                        "dataPoints": [{
                            "attributes": [
                                {"key": k, "value": {"stringValue": str(v)}} 
                                for k, v in attributes.items()
                            ],
                            "timeUnixNano": str(int(time.time() * 1e9)),
                            "asDouble": float(value)
                        }]
                    }
                }]
            }]
        }]
    }
    
    try:
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            OTEL_COLLECTOR_URL,
            data=data,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                print(f"✅ Métrique envoyée: {name}={value}")
                return True
            else:
                print(f"⚠️ Erreur envoi {name}: {response.status}")
                return False
    except Exception as e:
        print(f"⚠️ Erreur envoi métrique {name}: {e}")
        return False