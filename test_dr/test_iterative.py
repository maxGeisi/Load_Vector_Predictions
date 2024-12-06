import pytest

from iterative import charge_battery,use_surplus_for_consumption


@pytest.mark.parametrize("solar_energy, current_charge, battery_capacity, expected_surplus, expected_new_charge", [
    (50, 150, 300, 0, 200),
    (150, 250, 300, 100, 300),
    ( 350, 100, 300, 150, 300),
])

def test_charge_battery(solar_energy, current_charge, battery_capacity, expected_surplus, expected_new_charge):
    surplus, new_charge = charge_battery(solar_energy, current_charge, battery_capacity)
    assert surplus == expected_surplus
    assert new_charge == expected_new_charge

def test_invalid_battery_capacity():
    with pytest.raises(ValueError):
        charge_battery(100, 200, -100)  # Expect a ValueError due to negative battery capacity

def test_invalid_surplus():
    with pytest.raises(ValueError):
        use_surplus_for_consumption(100, -1)
