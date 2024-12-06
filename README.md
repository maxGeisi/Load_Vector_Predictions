# Load_Vector_Predictions

A Python project to optimize energy consumption using battery storage, peak shaving, and surplus solar energy utilization.

Features
Validates input data format (timestamps and energy values).
Optimizes energy consumption through battery charging/discharging.
Reduces peak loads and calculates cost savings.
Provides visualizations of load curves and peak comparisons.

Getting Started
Install Dependencies:

```bash
pip install pandas matplotlib pytest
```
Input Data:

Load (lg.csv) and solar (sd.csv) files with Timestamp and kW columns.

Run the Script:
```bash
python main.py
```

Outputs:

Optimized data (output.csv).

Load curve and peak comparison charts.

Run tests with:

```bash
pytest test_dr/test_input.py
```
Results
Reduced peaks and optimized energy costs.

Summary visualizations and savings analysis.
