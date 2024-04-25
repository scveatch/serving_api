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

@app.get("/movies/{page}")
def movies_by_page(page):
     with eng.connect() as con:
        query = """
                SELECT *
                FROM movies
                ORDER BY index
                LIMIT 50
                OFFSET :off
                """
        res = con.execute(text(query), {'off': 50*int(page)})
        return [r._asdict() for r in res]

#Return all data on weather
@app.get("/weather")
def all_weather():
    with eng.connect() as con:
        query = """
                SELECT * FROM processed_weather;
                """
        res = con.execute(text(query))
        return [dict(row) for row in res]


#Return all data on traffic incidents
#@app.get(...)
def all_incidents():
    with eng.connect() as con:
        query = """
                SELECT * FROM incidents;
                """
        #res = con.execute(text(query))
        #return[r._asdict() for r in res]


#Return all data for given city
#@app.get(...)
def all_by_city(city):
    with eng.connect() as con:
        query = """
                SELECT * FROM incidents AS i
                JOIN weather_rdb AS w
                ON --CONDITION
                JOIN weather_specifics AS ws
                ON w.weather_description=ws.weather_description
                WHERE w.city_name==(city)::VARCHAR;
                """
    #res = con.execute(text(query))
    #return[r._asdict() for r in res]


