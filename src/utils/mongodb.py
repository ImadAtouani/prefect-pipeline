"""Utils MongoDB pour Prefect"""
from pymongo import MongoClient
from typing import Dict, Any, Optional
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:changeme@mongodb:27017/")
DATABASE_NAME = "data_pipeline"


def get_mongo_client() -> MongoClient:
    """Retourne un client MongoDB"""
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)


def get_collection(collection_name: str):
    """Retourne une collection MongoDB"""
    client = get_mongo_client()
    db = client[DATABASE_NAME]
    return db[collection_name]


def insert_raw_data(data: Dict[str, Any]) -> str:
    """Insère des données brutes dans MongoDB"""
    collection = get_collection("raw_data")
    result = collection.insert_one(data)
    return str(result.inserted_id)


def insert_normalized_data(data: Dict[str, Any]) -> str:
    """Insère des données normalisées dans MongoDB avec déduplication"""
    collection = get_collection("normalized_data")
    if "dedup_key" in data:
        existing = collection.find_one({"dedup_key": data["dedup_key"]})
        if existing:
            collection.update_one(
                {"dedup_key": data["dedup_key"]},
                {"$set": data}
            )
            return str(existing["_id"])
    result = collection.insert_one(data)
    return str(result.inserted_id)


def insert_rejected_data(data: Dict[str, Any]) -> str:
    """Insère des données rejetées dans MongoDB"""
    collection = get_collection("rejected_data")
    result = collection.insert_one(data)
    return str(result.inserted_id)


def save_metrics(metrics: Dict[str, Any]) -> str:
    """Sauvegarde les métriques d'exécution"""
    collection = get_collection("script_metrics")
    result = collection.insert_one(metrics)
    return str(result.inserted_id)