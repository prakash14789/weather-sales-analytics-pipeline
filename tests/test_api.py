import pytest
from unittest.mock import patch, MagicMock
from api.fetch_holidays import (
    fetch_holidays_from_api,
    filter_and_deduplicate,
    SALES_HOLIDAYS
)

def test_fetch_holidays_from_api_success():
    """Test fetch_holidays_from_api when the HTTP request succeeds (200)."""
    mock_response_data = {
        "response": {
            "holidays": [
                {
                    "name": "New Year's Day",
                    "date": {"iso": "2014-01-01"}
                }
            ]
        }
    }
    
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_response_data
        mock_get.return_value = mock_response

        status_code, result = fetch_holidays_from_api("dummy_api_key", 2014)

        assert status_code == 200
        assert result == mock_response_data
        mock_get.assert_called_once_with(
            "https://calendarific.com/api/v2/holidays?api_key=dummy_api_key&country=US&year=2014"
        )

def test_fetch_holidays_from_api_failure():
    """Test fetch_holidays_from_api when the HTTP request fails (e.g. 500)."""
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        status_code, result = fetch_holidays_from_api("dummy_api_key", 2014)

        assert status_code == 500
        assert result is None
        mock_get.assert_called_once()

def test_filter_and_deduplicate():
    """Test that filter_and_deduplicate properly filters and removes duplicate holidays."""
    raw_holidays = [
        # In SALES_HOLIDAYS list
        {
            "name": "New Year's Day",
            "date": {"iso": "2014-01-01"}
        },
        # Duplicate of the above (should be removed)
        {
            "name": "New Year's Day",
            "date": {"iso": "2014-01-01"}
        },
        # In SALES_HOLIDAYS list, different date
        {
            "name": "Christmas Day",
            "date": {"iso": "2014-12-25"}
        },
        # NOT in SALES_HOLIDAYS list (should be filtered out)
        {
            "name": "Groundhog Day",
            "date": {"iso": "2014-02-02"}
        },
        # Malformed holiday missing date (should be ignored gracefully)
        {
            "name": "Thanksgiving Day"
        },
        # Malformed holiday missing name (should be ignored gracefully)
        {
            "date": {"iso": "2014-11-27"}
        }
    ]

    custom_sales_holidays = ["New Year's Day", "Christmas Day", "Thanksgiving Day"]
    
    unique_filtered = filter_and_deduplicate(raw_holidays, sales_holidays=custom_sales_holidays)

    # Should contain:
    # 1. New Year's Day (once, because of deduplication)
    # 2. Christmas Day
    # Thanksgiving should be filtered because it has no date
    assert len(unique_filtered) == 2
    
    names_dates = [(h["name"], h["date"]["iso"]) for h in unique_filtered]
    assert ("New Year's Day", "2014-01-01") in names_dates
    assert ("Christmas Day", "2014-12-25") in names_dates
