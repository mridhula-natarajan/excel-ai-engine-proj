import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

OUTPUT_DIR = "synthetic_data/structured"
os.makedirs(OUTPUT_DIR, exist_ok=True)

np.random.seed(42)
random.seed(42)

def random_dates(start, end, n):
    start_u = start.timestamp()
    end_u = end.timestamp()
    return [datetime.fromtimestamp(random.uniform(start_u, end_u)) for _ in range(n)]

# 1. Car Dealership Data
def generate_car_data():
    data = {
        "Car_ID": [f"CAR_{i}" for i in range(1000)],
        "Brand": np.random.choice(["Toyota", "Honda", "Ford", "BMW", "Hyundai"], 1000),
        "Model_Year": np.random.randint(2010, 2024, 1000),
        "Fuel_Type": np.random.choice(["Petrol", "Diesel", "Hybrid", "Electric"], 1000),
        "Price": np.random.randint(500000, 5000000, 1000),
        "Mileage_kmpl": np.random.uniform(10, 30, 1000).round(2),
        "Transmission": np.random.choice(["Manual", "Automatic"], 1000),
        "Owner_Type": np.random.choice(["First", "Second", "Third"], 1000),
        "City": np.random.choice(["Delhi", "Mumbai", "Chennai", "Bangalore", "Pune"], 1000),
        "Sold_Date": random_dates(datetime(2020, 1, 1), datetime(2024, 1, 1), 1000),
    }
    return pd.DataFrame(data)

# 2. Loan Application Data
def generate_loan_data():
    data = {
        "Application_ID": [f"LN_{i}" for i in range(1000)],
        "Customer_Age": np.random.randint(21, 65, 1000),
        "Employment_Type": np.random.choice(["Salaried", "Self-employed", "Unemployed"], 1000),
        "Loan_Amount": np.random.randint(50000, 2000000, 1000),
        "Interest_Rate": np.random.uniform(6.5, 14.5, 1000).round(2),
        "Tenure_Months": np.random.choice([12, 24, 36, 48, 60], 1000),
        "Credit_Score": np.random.randint(300, 900, 1000),
        "City": np.random.choice(["Delhi", "Bangalore", "Hyderabad", "Chennai", "Mumbai"], 1000),
        "Loan_Purpose": np.random.choice(["Home", "Car", "Education", "Personal"], 1000),
        "Status": np.random.choice(["Approved", "Pending", "Rejected"], 1000),
    }
    return pd.DataFrame(data)

# 3. Healthcare Patient Data
def generate_health_data():
    data = {
        "Patient_ID": [f"P_{i}" for i in range(1000)],
        "Age": np.random.randint(1, 90, 1000),
        "Gender": np.random.choice(["Male", "Female"], 1000),
        "Department": np.random.choice(["Cardiology", "Orthopedics", "Neurology", "General"], 1000),
        "Doctor": np.random.choice(["Dr. Rao", "Dr. Sharma", "Dr. Menon", "Dr. Das"], 1000),
        "Admission_Date": random_dates(datetime(2023, 1, 1), datetime(2024, 1, 1), 1000),
        "Discharge_Date": random_dates(datetime(2023, 1, 15), datetime(2024, 1, 15), 1000),
        "Treatment_Cost": np.random.randint(10000, 200000, 1000),
        "Insurance_Covered": np.random.choice(["Yes", "No"], 1000),
        "Outcome": np.random.choice(["Recovered", "Under Treatment", "Referred"], 1000),
    }
    return pd.DataFrame(data)

# 4. Real Estate Listings
def generate_real_estate_data():
    data = {
        "Property_ID": [f"PROP_{i}" for i in range(1000)],
        "City": np.random.choice(["Mumbai", "Chennai", "Delhi", "Kolkata", "Bangalore"], 1000),
        "Property_Type": np.random.choice(["Apartment", "Villa", "Plot"], 1000),
        "Bedrooms": np.random.randint(1, 5, 1000),
        "Bathrooms": np.random.randint(1, 4, 1000),
        "Builtup_Area_sqft": np.random.randint(600, 4000, 1000),
        "Price_Lakhs": np.random.uniform(20, 500, 1000).round(2),
        "Furnishing": np.random.choice(["Unfurnished", "Semi-Furnished", "Fully-Furnished"], 1000),
        "Listed_By": np.random.choice(["Owner", "Agent", "Builder"], 1000),
        "Listed_Date": random_dates(datetime(2022, 1, 1), datetime(2024, 1, 1), 1000),
    }
    return pd.DataFrame(data)

# 5. Online Education Students
def generate_edu_data():
    data = {
        "Student_ID": [f"STU_{i}" for i in range(1000)],
        "Course_Name": np.random.choice(["Python", "Data Science", "AI", "Cloud", "Web Dev"], 1000),
        "Enrollment_Date": random_dates(datetime(2022, 1, 1), datetime(2024, 1, 1), 1000),
        "Progress_Percent": np.random.uniform(0, 100, 1000).round(2),
        "Score": np.random.randint(0, 100, 1000),
        "Completed": np.random.choice(["Yes", "No"], 1000),
        "Country": np.random.choice(["India", "USA", "UK", "Canada", "Australia"], 1000),
        "Gender": np.random.choice(["Male", "Female"], 1000),
        "Subscription_Type": np.random.choice(["Free", "Paid"], 1000),
        "Rating": np.random.uniform(1, 5, 1000).round(1),
    }
    return pd.DataFrame(data)

datasets = {
    "car_data.xlsx": generate_car_data(),
    "loan_data.xlsx": generate_loan_data(),
    "health_data.xlsx": generate_health_data(),
    "real_estate_data.xlsx": generate_real_estate_data(),
    "education_data.xlsx": generate_edu_data(),
}

for name, df in datasets.items():
    df.to_excel(os.path.join(OUTPUT_DIR, name), index=False)
    print(f"Created structured dataset: {name}")
