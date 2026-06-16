import pytest
from unittest.mock import patch, MagicMock
from api.fetch_weather import (
    fetch_weather_for_region,
    format_weather_response,
    REGIONS
)

def test_fetch_weather_for_region_success():
    """Test fetch_weather_for_region when the HTTP request succeeds (200)."""
    mock_response_data = {
        "latitude": 40.71,
        "longitude": -74.01,
        "daily": {
            "time": ["2014-01-01"],
            "temperature_2m_mean": [0.5],
            "precipitation_sum": [0.0],
            "snowfall_sum": [0.0],
            "wind_speed_10m_max": [15.2]
        }
    }
    
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_response_data
        mock_get.return_value = mock_response

        status_code, result = fetch_weather_for_region(40.7128, -74.0060)

        assert status_code == 200
        assert result == mock_response_data
        mock_get.assert_called_once()
        assert "latitude=40.7128" in mock_get.call_args[0][0]
        assert "longitude=-74.006" in mock_get.call_args[0][0]

def test_fetch_weather_for_region_failure():
    """Test fetch_weather_for_region when the HTTP request fails."""
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        status_code, result = fetch_weather_for_region(40.7128, -74.0060)

        assert status_code == 404
        assert result is None

def test_format_weather_response():
    """Test that format_weather_response properly flattens nested API data."""
    raw_response = {
        "daily": {
            "time": ["2014-01-01", "2014-01-02"],
            "temperature_2m_mean": [0.5, -2.1],
            "precipitation_sum": [0.0, 12.5],
            "snowfall_sum": [0.0, 5.2],
            "wind_speed_10m_max": [15.2, 22.1]
        }
    }

    formatted = format_weather_response("East", raw_response)

    assert len(formatted) == 2
    assert formatted[0] == {
        "weather_region": "East",
        "weather_date": "2014-01-01",
        "temp_c": 0.5,
        "precipitation_mm": 0.0,
        "snowfall_cm": 0.0,
        "wind_speed_kmh": 15.2
    }
    assert formatted[1] == {
        "weather_region": "East",
        "weather_date": "2014-01-02",
        "temp_c": -2.1,
        "precipitation_mm": 12.5,
        "snowfall_cm": 5.2,
        "wind_speed_kmh": 22.1
    }

def test_format_weather_response_empty():
    """Test that format_weather_response handles empty or malformed inputs gracefully."""
    assert format_weather_response("East", None) == []
    assert format_weather_response("East", {}) == []
    assert format_weather_response("East", {"foo": "bar"}) == []
