import pytest
from vector import calculate_charge_and_surplus,calculate_discharge

@pytest.mark.parametrize("solar_energy, current_charge, battery_capacity, expected_charge, expected_surplus", [
    (100, 200, 300, 100, 0),    # Normal charging scenario
    (50, 275, 300, 25, 25),      # Partial charging (limited by capacity) --> battery not charged  to time series Modell
    (150, 200, 300, 100, 50),  # Solar energy exceeds the available space
    (0, 200, 300, 0, 0),  # No solar energy
    (100, 300, 300, 0, 100)  # Battery already full
])
def test_calculate_charge_and_surplus(solar_energy, current_charge, battery_capacity, expected_charge, expected_surplus):
    row = {'SolarEnergy': solar_energy}
    charge, surplus = calculate_charge_and_surplus(row, current_charge, battery_capacity)
    assert charge == expected_charge
    assert surplus == expected_surplus

def test_invalid_battery_capacity():
    with pytest.raises(ValueError):
        calculate_charge_and_surplus(1, 200, -100)  # Expect a ValueError due to negative battery capacity

def test_invalid_calc_discharge():
    with pytest.raises(ValueError):
        calculate_discharge(1, -200, 150,100)