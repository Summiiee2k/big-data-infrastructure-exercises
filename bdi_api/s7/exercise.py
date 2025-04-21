import json
from typing import Dict, List

import boto3
import psycopg2
from fastapi import APIRouter, status

from bdi_api.settings import DBCredentials, Settings

settings = Settings()
db_credentials = DBCredentials()
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)

# S3 client
s3_client = boto3.client("s3")


def get_db_connection():
    return psycopg2.connect(
        user=db_credentials.username,
        password=db_credentials.password,
        host=db_credentials.host,
        port=db_credentials.port,
    )


@s7.post("/aircraft/prepare")
def prepare_data() -> str:
    """Get the raw data from s3 and insert it into RDS

    Use credentials passed from `db_credentials`
    """

    bucket_name = "bdi-aircraft-sumedh"
    prefix = "readsb-hist/2023/11/01/"

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Create tables if they don't exist
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS aircraft (
                    icao VARCHAR(6) PRIMARY KEY,
                    registration VARCHAR(10),
                    type VARCHAR(10)
                );

                CREATE TABLE IF NOT EXISTS positions (
                    id SERIAL PRIMARY KEY,
                    icao VARCHAR(6) REFERENCES aircraft(icao),
                    timestamp FLOAT,
                    lat FLOAT,
                    lon FLOAT,
                    altitude_baro INTEGER,
                    ground_speed INTEGER,
                    emergency BOOLEAN
                );
            """
            )

            # Process each file
            for obj in response.get("Contents", []):
                # Get file from S3
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=obj["Key"])
                data = json.loads(s3_object["Body"].read().decode("utf-8"))

                aircraft_data = data.get("aircraft", [])
                for aircraft in aircraft_data:
                    cur.execute(
                        """
                        INSERT INTO aircraft (icao, registration, type)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (icao) DO UPDATE SET
                            registration = EXCLUDED.registration,
                            type = EXCLUDED.type
                    """,
                        (aircraft["icao"], aircraft.get("registration"), aircraft.get("type")),
                    )

                    for pos in aircraft.get("positions", []):
                        cur.execute(
                            """
                            INSERT INTO positions (icao, timestamp, lat, lon,
                                altitude_baro, ground_speed, emergency)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                            (
                                aircraft["icao"],
                                pos["timestamp"],
                                pos["lat"],
                                pos["lon"],
                                pos.get("altitude_baro"),
                                pos.get("ground_speed"),
                                pos.get("emergency", False),
                            ),
                        )

            conn.commit()
    finally:
        conn.close()

    return "OK"


@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> List[Dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc FROM THE DATABASE"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT icao, registration, type
                FROM aircraft
                ORDER BY icao ASC
                LIMIT %s OFFSET %s
            """,
                (num_results, page * num_results),
            )
            results = [{"icao": r[0], "registration": r[1], "type": r[2]} for r in cur.fetchall()]
            return results
    finally:
        conn.close()


@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> List[Dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT timestamp, lat, lon
                FROM positions
                WHERE icao = %s
                ORDER BY timestamp ASC
                LIMIT %s OFFSET %s
            """,
                (icao, num_results, page * num_results),
            )
            results = [{"timestamp": r[0], "lat": r[1], "lon": r[2]} for r in cur.fetchall()]
            return results
    finally:
        conn.close()


@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> Dict:
    """Returns different statistics about the aircraft"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    MAX(altitude_baro) as max_altitude_baro,
                    MAX(ground_speed) as max_ground_speed,
                    BOOL_OR(emergency) as had_emergency
                FROM positions
                WHERE icao = %s
            """,
                (icao,),
            )
            result = cur.fetchone()
            if result and result[0] is not None:
                return {"max_altitude_baro": result[0], "max_ground_speed": result[1], "had_emergency": result[2]}
            return {"max_altitude_baro": None, "max_ground_speed": None, "had_emergency": False}
    finally:
        conn.close()
