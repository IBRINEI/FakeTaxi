import os
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer

app = FastAPI(title="Taxi Booking API", description="Backend")

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'realtime_rides')

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


class RideEvent(BaseModel):
    ride_id: str
    driver_id: int
    client_id: int
    status: str
    fare_amount: float
    event_timestamp: str


# --- HTTP ENDPOINTS ---
@app.post("/api/v1/rides")
async def create_ride(ride: RideEvent):
    try:
        ride_dict = ride.model_dump()

        producer.send(KAFKA_TOPIC, ride_dict)

        producer.flush()

        return {"status": "success", "message": "Поездка успешно зарегистрирована"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка внутреннего сервера: {e}")