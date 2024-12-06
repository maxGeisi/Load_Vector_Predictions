import pandas as pd


def calculate_charge_and_surplus(row, current_charge, battery_capacity):
    # Value Checking
    if battery_capacity < 0:
        raise ValueError
    # Determine the possible charge based on the available space in the battery
    charge = min((battery_capacity - current_charge), row['SolarEnergy'])
    # Determine the surplus after charging
    surplus = row['SolarEnergy'] - charge

    return charge, surplus


def adjust_consumption_with_surplus(row):
    # Adjust consumption by the surplus
    consumption = row['kW'] - row['surplus']
    if consumption < 0:
        return abs(consumption), 0
    else:
        return 0, consumption


def calculate_discharge(row, current_charge, extreme_peak_threshold, peak_shaving_threshold, reduction_factor=0.5):
    # Value checking
    if current_charge < 0:
        raise ValueError
    # Calculate the discharge based on the given thresholds
    difference_extreme = max(0, row['consumption_after_surplus'] - extreme_peak_threshold)
    difference_normal = max(0, row['consumption_after_surplus'] - peak_shaving_threshold) - difference_extreme

    discharge_extreme = difference_extreme
    discharge_normal = difference_normal * reduction_factor

    # Discharge as much as possible, to increase possible very high peaks
    return min(discharge_extreme + discharge_normal, current_charge)


# Applying the functions

def apply(lg, sd, battery_capacity, current_charge, extreme_peak_threshold, peak_shaving_threshold):
    # Assign solar energy from the sd data
    lg['SolarEnergy'] = sd['kW']

    # Calculate charge and surplus for each row
    lg['charge'], lg['surplus'] = zip(
        *lg.apply(calculate_charge_and_surplus, args=(current_charge, battery_capacity), axis=1))
    # Update the battery charge level
    current_charge += lg['charge'].sum()
    sold_energy = 0

    # Adjust consumption by the surplus
    lg['sold_energy'], lg['consumption_after_surplus'] = zip(*lg.apply(adjust_consumption_with_surplus, axis=1))
    # Calculate the sold energy
    sold_energy += lg['sold_energy'].sum()

    # Calculate battery discharge for each row/ reduction factor determines a flexible adjustment
    # "how much" should be discharged in "normal situations"
    lg['discharge'] = lg.apply(calculate_discharge,
                               args=(current_charge, extreme_peak_threshold, peak_shaving_threshold, 0.5), axis=1)

    # Update the battery charge level after discharging
    current_charge -= lg['discharge'].sum()

    # Optimize consumption taking into account the discharge
    lg['Optimized Consumption'] = lg['consumption_after_surplus'] - lg['discharge']

    # Remove the helper columns for a clean result
    lg = lg.drop(columns=['charge', 'surplus', 'consumption_after_surplus', 'discharge', 'SolarEnergy'])

    return lg, sold_energy
