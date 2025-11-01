import pandas as pd
import numpy as np
import random
from faker import Faker
import os
from datetime import datetime, timedelta

fake = Faker()

# ---- Utility for true random date generation ----
def random_dates(start_year=2018, end_year=2024, n=1000):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    start_u = start.timestamp()
    end_u = end.timestamp()
    return [
        datetime.fromtimestamp(np.random.uniform(start_u, end_u)).date()
        for _ in range(n)
    ]


# ---- 1. Sales Data ----
def generate_sales_data(n_rows=1000):
    data = {
        "OrderID": [f"ORD-{1000+i}" for i in range(n_rows)],
        "CustomerName": [fake.name() for _ in range(n_rows)],
        "Region": [random.choice(["North", "South", "East", "West"]) for _ in range(n_rows)],
        "ProductCategory": [random.choice(["Electronics", "Furniture", "Clothing", "Toys"]) for _ in range(n_rows)],
        "Quantity": np.random.randint(1, 50, n_rows),
        "UnitPrice": np.random.uniform(10, 500, n_rows).round(2),
        "Discount": np.random.choice([0, 0.05, 0.1, 0.15], n_rows),
        "ShippingCost": np.random.uniform(5, 50, n_rows).round(2),
        "OrderDate": random_dates(2020, 2024, n_rows),
        "PaymentMethod": [random.choice(["Credit Card", "Debit Card", "UPI", "Cash"]) for _ in range(n_rows)],
        "DeliveryStatus": [random.choice(["Delivered", "Pending", "Cancelled"]) for _ in range(n_rows)],
        "CustomerRating": np.random.randint(1, 6, n_rows),
        "Warehouse": [random.choice(["A", "B", "C", "D"]) for _ in range(n_rows)],
        "SalesRep": [fake.first_name() for _ in range(n_rows)],
        "Profit": np.random.uniform(10, 500, n_rows).round(2),
        "City": [fake.city() for _ in range(n_rows)],
        "State": [fake.state() for _ in range(n_rows)],
        "Country": [fake.country() for _ in range(n_rows)],
        "ReturnFlag": np.random.choice([0, 1], n_rows),
        "Feedback": [random.choice(["Good", "Average", "Poor"]) for _ in range(n_rows)],
    }
    return pd.DataFrame(data)


# ---- 2. HR Data ----
def generate_hr_data(n_rows=1000):
    data = {
        "EmployeeID": [f"EMP-{i}" for i in range(n_rows)],
        "Name": [fake.name() for _ in range(n_rows)],
        "Department": [random.choice(["IT", "HR", "Finance", "Sales", "R&D"]) for _ in range(n_rows)],
        "Age": np.random.randint(22, 60, n_rows),
        "Gender": [random.choice(["Male", "Female", "Other"]) for _ in range(n_rows)],
        "Experience": np.random.randint(0, 20, n_rows),
        "Salary": np.random.randint(30000, 200000, n_rows),
        "Bonus": np.random.uniform(1000, 10000, n_rows).round(2),
        "PerformanceScore": np.random.randint(1, 6, n_rows),
        "RemoteWork": np.random.choice([True, False], n_rows),
        "JoiningDate": random_dates(2015, 2024, n_rows),
        "Manager": [fake.name() for _ in range(n_rows)],
        "Education": [random.choice(["Bachelors", "Masters", "PhD"]) for _ in range(n_rows)],
        "PromotionEligible": np.random.choice([True, False], n_rows),
        "LeaveBalance": np.random.randint(0, 30, n_rows),
        "City": [fake.city() for _ in range(n_rows)],
        "State": [fake.state() for _ in range(n_rows)],
        "Country": [fake.country() for _ in range(n_rows)],
        "Resigned": np.random.choice([True, False], n_rows),
        "MaritalStatus": [random.choice(["Single", "Married", "Divorced"]) for _ in range(n_rows)],
    }
    return pd.DataFrame(data)


# ---- 3. Product Data ----
def generate_product_data(n_rows=1000):
    data = {
        "ProductID": [f"PROD-{1000+i}" for i in range(n_rows)],
        "ProductName": [fake.word().capitalize() for _ in range(n_rows)],
        "Category": [random.choice(["Electronics", "Apparel", "Grocery", "Books"]) for _ in range(n_rows)],
        "Supplier": [fake.company() for _ in range(n_rows)],
        "CostPrice": np.random.uniform(10, 500, n_rows).round(2),
        "SellingPrice": np.random.uniform(20, 1000, n_rows).round(2),
        "Stock": np.random.randint(0, 1000, n_rows),
        "ReorderLevel": np.random.randint(10, 100, n_rows),
        "Discount": np.random.choice([0, 0.05, 0.1, 0.2], n_rows),
        "Rating": np.random.randint(1, 6, n_rows),
        "WarrantyYears": np.random.randint(0, 5, n_rows),
        "Manufacturer": [fake.company() for _ in range(n_rows)],
        "OriginCountry": [fake.country() for _ in range(n_rows)],
        "ReturnPolicy": [random.choice(["7 days", "15 days", "30 days"]) for _ in range(n_rows)],
        "OnlineAvailable": np.random.choice([True, False], n_rows),
        "LaunchDate": random_dates(2018, 2024, n_rows),
        "Color": [random.choice(["Red", "Blue", "Black", "White", "Green"]) for _ in range(n_rows)],
        "Weight(kg)": np.random.uniform(0.1, 10.0, n_rows).round(2),
        "Dimensions(cm)": [f"{random.randint(10,100)}x{random.randint(10,100)}x{random.randint(10,100)}" for _ in range(n_rows)],
        "InDemand": np.random.choice([True, False], n_rows),
    }
    return pd.DataFrame(data)


# ---- Save Excel files ----
def save_excel_files(output_dir="synthetic_data"):
    os.makedirs(output_dir, exist_ok=True)
    datasets = {
        "sales_data.xlsx": generate_sales_data(),
        "hr_data.xlsx": generate_hr_data(),
        "product_data.xlsx": generate_product_data(),
    }

    for file_name, df in datasets.items():
        path = os.path.join(output_dir, file_name)
        df.to_excel(path, index=False)
        print(f"✅ Generated {path} with shape {df.shape}")


if __name__ == "__main__":
    save_excel_files()
