import os
import json
import logging
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'realtime_rides')

producer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    logging.info(f"Starting API Server. Connecting to Kafka at {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logging.info("Successfully connected to Kafka!")
    except KafkaError as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        raise e

    yield

    if producer:
        logging.info("Shutting down API Server. Closing Kafka connection...")
        producer.close()


app = FastAPI(
    title="Taxi Booking API",
    description="Backend",
    lifespan=lifespan
)


class RideEvent(BaseModel):
    ride_id: str
    driver_id: int
    client_id: int
    status: str
    fare_amount: float
    event_timestamp: str


@app.post("/api/v1/rides")
async def create_ride(ride: RideEvent):
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka producer is not initialized")

    try:
        ride_dict = ride.model_dump()
        producer.send(KAFKA_TOPIC, ride_dict)
        producer.flush()
        return {"status": "success", "message": "Ride successfully registered"}
    except Exception as e:
        logging.error(f"Error sending message to Kafka: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")