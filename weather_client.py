import requests
import os
from datetime import datetime

API_KEY = os.getenv("OPENWEATHER_API_KEY")

def get_weather(lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units=metric&appid={API_KEY}"
    resp = requests.get(url)
    data = resp.json()

    return {
        "weather_date": datetime.utcnow().date(),
        "temp_c": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather_main": data["weather"][0]["main"],
        "weather_description": data["weather"][0]["description"],
        "wind_speed": data["wind"]["speed"]
    }
