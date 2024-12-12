# Real-Time Weather Monitoring System

A sophisticated weather monitoring system built with PySpark that provides real-time weather data analysis and visualization.

## Features
- Real-time weather data collection using WeatherStack API
- Data processing with PySpark
- Interactive visualizations using Plotly and Dash
- Weather trend analysis and anomaly detection
- Historical weather data storage and analysis

## Setup
1. Create virtual environment:
```bash
python -m venv weather_venv
source weather_venv/bin/activate  # For Linux/Mac
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
Create a `.env` file and add your WeatherStack API key:
```
WEATHERSTACK_API_KEY=your_api_key_here
```

4. Run the application:
```bash
python src/main.py
```

## Project Structure
```
weather/
├── src/
│   ├── main.py
│   ├── data_collector.py
│   ├── spark_processor.py
│   └── visualization.py
├── data/
│   ├── raw/
│   └── processed/
├── requirements.txt
└── README.md
```
