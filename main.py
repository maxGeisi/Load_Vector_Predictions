import pandas as pd
import matplotlib.pyplot as plt

# AIRFLOW IMPORTS
#from airflow.operators.bash import BashOperator
#from airflow.sensors.filesystem import FileSensor
#from airflow import DAG
#from airflow.operators.python import PythonOperator
#from datetime import datetime, timedelta

import iterative
import vector

"""Method Declaration for Airflow input:"""
#def run_project(**kwargs):


#--------------------------------------------------- Setting initial parameters ------------------------------------------


# ---------------------- INITIALIZATION ----------------------
# Define parameters

battery_capacity = 300  # kWh
energy_price = 0.80  # Price per kWh
peak_price = 130  ## Price per kW peak
std_var = 1.6  # indicates when normal peak shaving occurs
modeling = 0  # Choose between iterative and vector approach --> 0= Iterative / 1= Vector
pv = 50  # number of PV modules (scaling factor)

# Hardcoded variables
current_charge = 0  # Initially, the battery is empty
energy_sold = 0  # Amount of energy sold back to the grid


# Read data
sd = pd.read_csv("/Users/maxge/CODE/Projects/tacto_aufbereitet/solar.csv", sep=";", decimal=",")
lg = pd.read_csv("/Users/maxge/CODE/Projects/tacto_aufbereitet/last/example_lastgang.csv", sep=";", decimal=",")


# Convert timestamps for improved overview
lg['Timestamp'] = pd.to_datetime(lg['Timestamp'], format='%d.%m.%y %H:%M', errors='coerce')
sd['Timestamp'] = pd.to_datetime(sd['Timestamp'], format='%d.%m.%y %H:%M', errors='coerce')


# NaN treatments
lg.interpolate(method='linear', inplace=True)
sd.interpolate(method='linear', inplace=True)

# Test the input:
def verify_format(sd, lg):
    for df in [sd, lg]:
        if 'kW' not in df.columns:
            raise ValueError("DataFrame must contain a 'kW' column")
        if df['kW'].isnull().all():
            raise ValueError("The 'kW' column must not be empty")

        # Check if 'Timestamp' columns are in datetime format
    if not pd.api.types.is_datetime64_any_dtype(sd['Timestamp']):
        raise ValueError("Timestamp column in sd DataFrame is not in datetime format")
    if not pd.api.types.is_datetime64_any_dtype(lg['Timestamp']):
        raise ValueError("Timestamp column in lg DataFrame is not in datetime format")

    #Checking for at least one true value
    if sd['Timestamp'].isnull().any() or lg['Timestamp'].isnull().any():
        raise ValueError("Invalid 'Timestamp' format detected")


"""
#AIRFLOW-Input
ti = kwargs['ti']
lg_path = ti.xcom_pull(task_ids='find_new_file')
lg = pd.read_csv(lg_path, sep=";", decimal=",", encoding="latin-1")
"""


# Scaling the PV values:
sd['kW'] = sd['kW'] * pv

# Outputs for checking correct timestamp and kW detection
print(lg.head())
print(sd.head())


# ---------------------- DESCRIPTIVE STATISTICS ----------------------
print("Descriptives:")
print(lg.describe())
print(sd.describe())


# ---------------------- PEAK SHAVING CALCULATIONS ----------------------
extreme_peak_threshold = lg['kW'].quantile(0.99)
average = lg['kW'].mean()
standard_deviation = lg['kW'].std()
peak_shaving_threshold = average + std_var * standard_deviation  # e.g., for 2 standard deviations above the average

# ---------------------- APPLYING MODEL ----------------------
# Iterative approach
if modeling == 0:
    lg, energy_sold = iterative.optimize_consumption_iteratively(
        lg,
        sd,
        battery_capacity,
        peak_shaving_threshold,
        extreme_peak_threshold
    )
# Vector approach
else:
    lg, energy_sold = vector.apply(
        lg,
        sd,
        battery_capacity,
        current_charge,
        extreme_peak_threshold,
        peak_shaving_threshold,
    )


# ---------------------- RESULT ANALYSIS ----------------------
# Analyze peak values
old_peak = lg['kW'].max()
new_peak = lg['Optimized Consumption'].max()
print("\nOld vs. New Peak:")
print(f"Old Peak: {old_peak:.2f} kW")
print(f"New Peak after Peak Shaving: {new_peak:.2f} kW")

# Compare total consumption
total_consumption_old = lg['kW'].sum()
total_consumption_new = lg['Optimized Consumption'].sum()

print(f"\nTotal Consumption before Battery Application: {total_consumption_old:.2f} kWh")
print(f"Total Consumption after Battery Application: {total_consumption_new:.2f} kWh")

# Calculate and compare costs
#Energy price

old_energy_price = total_consumption_old * energy_price
new_energy_price = total_consumption_new * energy_price

print(f"\nOld Energy Price: {old_energy_price:.2f} €")
print(f"New Energy Price: {new_energy_price:.2f} €")
print(f"Savings on Energy Price: {old_energy_price - new_energy_price:.2f} €")

#Peak price
old_peak_price = old_peak * peak_price
new_peak_price = new_peak * peak_price

print(f"\nOld Peak Price: {old_peak_price:.2f} €")
print(f"New Peak Price: {new_peak_price:.2f} €")
print(f"Savings on Peak Price: {old_peak_price - new_peak_price:.2f} €")

#Total price
old_total_price = old_peak_price + old_energy_price
new_total_price = new_peak_price + new_energy_price

print(f"\nOld Total Price: {old_total_price:.2f} €")
print(f"New Total Price: {new_total_price:.2f} €")
print(f"Savings on Total Price: {old_total_price - new_total_price:.2f} €")

# Sold energy
print(f"\nSold energy: {energy_sold:.2f} €")


# ---------------------- VISUALIZATION ----------------------
# Display load curves
plt.figure(figsize=(12, 6))
plt.plot(lg['Timestamp'], lg['kW'], label="Old Load Curve", alpha=0.7)
plt.plot(lg['Timestamp'], lg['Optimized Consumption'], label="New Load Curve", alpha=0.7)
plt.title("Comparison of Load Curves")
plt.legend()
plt.show()

# Display peak consumption
data = {'Type': ['Old Peak', 'New Peak'], 'kW': [old_peak, new_peak]}
df_peaks = pd.DataFrame(data)
df_peaks.plot(x='Type', y='kW', kind='bar', legend=False, title="Comparison of Peaks")
plt.ylabel("kW")
plt.show()

# Save data
output_path = "/Users/maxge/CODE/Projects/tacto_aufbereitet/updated_last/output.csv"
lg[['Timestamp', 'kW', 'Optimized Consumption']].to_csv(output_path, sep=";", decimal=",", index=False)



"""
AIRFLOW UPDATE
default_args = {
    'owner': 'me',
    'start_date': datetime(2023, 10, 26),
    'retries': 1,
}

dag = DAG(
    dag_id='rough_peak_shave_analysis_schedule',
    default_args=default_args,
    schedule_interval=None  # This ensures that the DAG does not automatically run to a set schedule
)

# Creating a task that waits for a new file
file_sensor = FileSensor(
    task_id='file_sensor',
    filepath='/Users/maxge/CODE/Projects/tacto_aufbereitet/last/*.csv',  
    fs_conn_id='fs_default',  # This is the default connection to the file system
    poke_interval=5,  # How often the sensor checks if the file is present (in seconds)
    timeout=3600,  # How long the sensor should wait in total (in seconds) before triggering an error
    dag=dag
)

find_and_push = BashOperator(
    task_id='find_new_file',
    bash_command="find /Users/maxge/CODE/Projects/tacto_aufbereitet/last/ -type f -print0 | xargs -0 stat -f '%m %N' "
                 "| sort -n | tail -1 | cut -d' ' -f2-",
    do_xcom_push=True,
    dag=dag
)


run_code = PythonOperator(
    task_id='run_project',
    python_callable=run_project,
    provide_context=True,
    execution_timeout=timedelta(minutes=30),
    dag=dag
)

file_sensor >> find_and_push >> run_code  
"""
