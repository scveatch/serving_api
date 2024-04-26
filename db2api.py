from fastapi import FastAPI
from sqlalchemy import create_engine, text
import yaml
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL is None:
    raise ValueError("The DATABASE_URL environment variable MUST be set!")
if not DATABASE_URL.startswith("postgresql"):
   DATABASE_URL = f"postgresql://{DATABASE_URL}"

app = FastAPI()
eng = create_engine(DATABASE_URL)

def create_simple_endpoint(endpoint, query):
   """Function to manufacture simple endpoints for those without much
   Python experience.
   """
   @app.get(endpoint)
   def auto_simple_endpoint():
      f"""Automatic endpoint function for {endpoint}"""
      with eng.connect() as con:
         res = con.execute(query)
         return [r._asdict() for r in res]
            
with open("endpoints.yaml") as f:
   endpoints = yaml.safe_load(f)
   for endpoint, query in endpoints.items():
      create_simple_endpoint(endpoint, query)





#------------------------------------------------
# Custom Endpoints
#------------------------------------------------

#Return all data on weather
@app.get("/weather")
def all_weather():
    with eng.connect() as con:
        query = """
                SELECT pw.*, ws.*
                FROM processed_weather pw
                JOIN weather_specifics ws
                ON pw.weather_id = ws.weather_id;
                """
        res = con.execute(text(query))
        return [dict(row) for row in res]


#Return all data on traffic incidents
@app.get("/incidents")
def all_incidents():
    with eng.connect() as con:
        query = """
                SELECT m.*, i.*, tt.*
                FROM main m
                JOIN incidents i ON m.id = i.incident_id
                JOIN traffic_type tt ON i.traffic_id = tt.traffic_id;
                """
        res = con.execute(text(query))
        return[r._asdict() for r in res]


#Return all data for given city
@app.get("/all_by_city/{city}")
async def all_by_city(city):
    allowed_cities = ['eugene', 'salem', 'portland']
    if city.lower() not in allowed_cities:
        return {'error': f'City must be one of {", ".join(allowed_cities)}'}
    
    with eng.connect() as con:
        query = """
                SELECT i.*, w.*, ws.*
                FROM incidents AS i
                JOIN processed_weather AS w
                ON i.incident_id = w.id
                JOIN weather_specifics AS ws
                ON w.weather_id = ws.weather_id
                WHERE w.city_name = INITCAP(:city);
                """
        res = con.execute(text(query), city=city)
        return [dict(row) for row in res]


