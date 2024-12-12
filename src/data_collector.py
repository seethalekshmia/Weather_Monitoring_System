import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

class WeatherDataCollector:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv('WEATHERSTACK_API_KEY')
        self.base_url = 'http://api.weatherstack.com/current'
        self.raw_data_path = 'data/raw'

    def fetch_weather_data(self, city):
        """
        Fetch weather data for a given city using the WeatherStack API
        """
        params = {
            'access_key': self.api_key,
            'query': city
        }

        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Add timestamp to the data
            data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Save raw data
            self._save_raw_data(data, city)
            
            return data
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city}: {str(e)}")
            return None

    def _save_raw_data(self, data, city):
        """
        Save raw weather data to a JSON file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{city}_{timestamp}.json"
        filepath = os.path.join(self.raw_data_path, filename)
        
        # Ensure the directory exists
        os.makedirs(self.raw_data_path, exist_ok=True)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)

    def collect_multiple_cities(self, cities):
        """
        Collect weather data for multiple cities
        """
        weather_data = {}
        for city in cities:
            data = self.fetch_weather_data(city)
            if data:
                weather_data[city] = data
        return weather_data
