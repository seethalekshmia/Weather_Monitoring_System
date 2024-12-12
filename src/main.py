import schedule
import time
from data_collector import WeatherDataCollector
from spark_processor import WeatherDataProcessor
from visualization import WeatherDashboard
import pandas as pd
import os

class WeatherMonitoringSystem:
    def __init__(self):
        self.collector = WeatherDataCollector()
        self.processor = WeatherDataProcessor()
        self.dashboard = WeatherDashboard()
        self.cities = ['London', 'New York', 'Tokyo', 'Sydney', 'Mumbai']
        
    def collect_and_process_data(self):
        """
        Collect and process weather data
        """
        # Collect data
        weather_data = self.collector.collect_multiple_cities(self.cities)
        
        # Process data using PySpark
        raw_data_path = 'data/raw'
        processed_data_path = 'data/processed'
        
        weather_df = self.processor.process_weather_data(raw_data_path)
        
        # Calculate statistics
        stats_df = self.processor.calculate_statistics(weather_df)
        
        # Detect anomalies
        anomalies_df = self.processor.detect_anomalies(weather_df)
        
        # Save processed data
        self.processor.save_processed_data(weather_df, processed_data_path)
        
        # Convert to pandas for visualization
        pandas_df = weather_df.toPandas()
        return pandas_df

    def run(self):
        """
        Run the weather monitoring system
        """
        # Initial data collection and processing
        df = self.collect_and_process_data()
        
        # Update dashboard with initial data
        self.dashboard.update_dashboard(df)
        
        # Schedule regular updates
        schedule.every(5).minutes.do(self.collect_and_process_data)
        
        # Run the dashboard
        self.dashboard.run_server(debug=True)
        
        # Keep the scheduler running
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    # Create data directories if they don't exist
    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    
    # Initialize and run the system
    weather_system = WeatherMonitoringSystem()
    weather_system.run()
