import pandas as pd

def charge_battery(solar_energy, current_charge, battery_capacity):
    #Value Checking:
    if battery_capacity<0 or current_charge<0 or solar_energy<0 :
        raise ValueError

    # Calculate possible charge based on available space in the battery
    charge = min((battery_capacity - current_charge), solar_energy)
    # Determine surplus energy after charging
    surplus = solar_energy - charge
    # Update the battery charge level
    current_charge += charge
    return surplus, current_charge

def use_surplus_for_consumption(consumption, surplus):
    # Value Checking:
    if surplus < 0:
        raise ValueError
    # Consumption is reduced by the available surplus
    consumption -= surplus
    sold_energy = 0
    # Negative consumption indicates energy was sold
    if consumption < 0:
        sold_energy = abs(consumption)
        consumption = 0
    return consumption, sold_energy

def discharge_battery(consumption, current_charge, peak_shaving_threshold, extreme_peak_threshold):
    # Discharge based on the given thresholds
    if consumption >= extreme_peak_threshold:
        discharge = consumption - extreme_peak_threshold
    else:
        discharge = consumption - peak_shaving_threshold
    # Maximum discharge is limited by the current charge level
    discharge = min(discharge, current_charge)
    # Consumption is reduced by discharging the battery
    consumption -= discharge
    # Update the battery charge level after discharging
    current_charge -= discharge
    return consumption, current_charge

def optimize_consumption_iteratively(lg, sd, battery_capacity, peak_shaving_threshold, extreme_peak_threshold):
    current_charge = 0
    sold_energy = 0

    for index, row in lg.iterrows():
        solar_energy = sd.at[index, 'kW']
        consumption = row['kW']

        # Charge the battery with available solar energy
        if solar_energy > 0:
            surplus, current_charge = charge_battery(solar_energy, current_charge, battery_capacity)
            # Surplus energy reduces consumption
            if surplus > 0:
                consumption, sold = use_surplus_for_consumption(consumption, surplus)
                sold_energy += sold

        # Discharge the battery when consumption is high and there is sufficient charge
        if consumption >= peak_shaving_threshold and current_charge > 0:
            consumption, current_charge = discharge_battery(consumption, current_charge, peak_shaving_threshold, extreme_peak_threshold)

        # Store the optimized consumption for this time step
        lg.at[index, 'Optimized Consumption'] = consumption

    return lg, sold_energy
