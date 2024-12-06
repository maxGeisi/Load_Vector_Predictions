

import pandas as pd
import pytest
from main import verify_format

def test_dataframe_format_success():
    # sample DataFrames with the correct structure and format
    sd_test = pd.DataFrame({'Timestamp': ['01.01.20 12:00', '01.01.20 13:00'], 'kW': [100, 200]})
    lg_test = pd.DataFrame({'Timestamp': ['01.01.20 14:00', '01.01.20 15:00'], 'kW': [300, 400]})
    # Convert 'Timestamp' to datetime format
    sd_test['Timestamp'] = pd.to_datetime(sd_test['Timestamp'], format='%d.%m.%y %H:%M', errors='coerce')
    lg_test['Timestamp'] = pd.to_datetime(lg_test['Timestamp'], format='%d.%m.%y %H:%M', errors='coerce')

    verify_format(sd_test, lg_test)

def test_kW_column_missing():
    # Create DataFrames missing 'kW' column
    sd_test = pd.DataFrame({'Timestamp': ['01.01.20 12:00', '01.01.20 13:00']})
    lg_test = pd.DataFrame({'Timestamp': ['01.01.20 14:00', '01.01.20 15:00']})
    # Expect a ValueError due to missing 'kW' column
    with pytest.raises(ValueError):
        verify_format(sd_test, lg_test)

def test_kW_column_empty():
    # Create DataFrames with an empty 'kW' column
    sd_test = pd.DataFrame({'Timestamp': ['01.01.20 12:00', '01.01.20 13:00'], 'kW': [None, None]})
    lg_test = pd.DataFrame({'Timestamp': ['01.01.20 14:00', '01.01.20 15:00'], 'kW': [None, None]})

    with pytest.raises(ValueError):
        verify_format(sd_test, lg_test)

def test_invalid_timestamp_format():
    # DataFrames with invalid 'Timestamp' format
    sd_test = pd.DataFrame({'Timestamp': ['01/01/2020 12:00', '01/01/2020 13:00'], 'kW': [100, 200]})
    lg_test = pd.DataFrame({'Timestamp': ['01/01/2020 14:00', '01/01/2020 15:00'], 'kW': [300, 400]})
    # Convert 'Timestamp' to datetime format but with the wrong format
    sd_test['Timestamp'] = pd.to_datetime(sd_test['Timestamp'], format='%d.%m.%y %H:%M', errors='coerce')
    lg_test['Timestamp'] = pd.to_datetime(lg_test['Timestamp'], format='%d.%m.%y %H:%M', errors='coerce')
    # Expect a TypeError due to incorrect 'Timestamp' format
    with pytest.raises(ValueError):
        verify_format(sd_test, lg_test)
