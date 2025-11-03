import pandas as pd
import random
import os

OUTPUT_DIR = "synthetic_data/unstructured"
os.makedirs(OUTPUT_DIR, exist_ok=True)

random.seed(42)

# 1. Customer Reviews
def generate_reviews():
    data = {
        "Review_ID": [f"R_{i}" for i in range(1000)],
        "Review_Text": [random.choice([
            "The product quality is great!",
            "Delivery was delayed but packaging was good.",
            "Not worth the price.",
            "Excellent customer service experience.",
            "I would definitely buy again."
        ]) for _ in range(1000)],
        "Rating": [random.randint(1, 5) for _ in range(1000)],
        "Product_Category": [random.choice(["Electronics", "Clothing", "Books", "Furniture"]) for _ in range(1000)],
        "Sentiment": [random.choice(["Positive", "Negative", "Neutral"]) for _ in range(1000)]
    }
    return pd.DataFrame(data)

# 2. IT Support Tickets
def generate_tickets():
    data = {
        "Ticket_ID": [f"TKT_{i}" for i in range(1000)],
        "Issue_Description": [random.choice([
            "Unable to connect to VPN.",
            "System crash during update.",
            "Email not syncing on mobile.",
            "Printer not responding.",
            "Slow internet connection at office."
        ]) for _ in range(1000)],
        "Priority": [random.choice(["Low", "Medium", "High"]) for _ in range(1000)],
        "Assigned_Team": [random.choice(["Network", "Hardware", "Software", "Security"]) for _ in range(1000)],
        "Status": [random.choice(["Open", "In Progress", "Resolved", "Closed"]) for _ in range(1000)],
    }
    return pd.DataFrame(data)

# 3. Social Media Posts
def generate_posts():
    data = {
        "Post_ID": [f"PST_{i}" for i in range(1000)],
        "Username": [f"user_{random.randint(100, 999)}" for _ in range(1000)],
        "Content": [random.choice([
            "Loving the new product launch!",
            "Can't believe this update broke everything!",
            "Had a wonderful experience shopping online.",
            "This service just keeps getting better!",
            "Anyone else facing issues with checkout?"
        ]) for _ in range(1000)],
        "Platform": [random.choice(["Twitter", "Instagram", "LinkedIn", "Facebook"]) for _ in range(1000)],
        "Engagement_Score": [random.randint(10, 10000) for _ in range(1000)],
    }
    return pd.DataFrame(data)

# 4. Employee Feedback
def generate_feedback():
    data = {
        "Feedback_ID": [f"FB_{i}" for i in range(1000)],
        "Employee_Comments": [random.choice([
            "Great work culture and supportive team.",
            "Need more flexibility for remote work.",
            "Career growth opportunities are limited.",
            "Appreciate the transparency from management.",
            "The new HR policies are well implemented."
        ]) for _ in range(1000)],
        "Department": [random.choice(["HR", "Finance", "Engineering", "Sales", "Support"]) for _ in range(1000)],
        "Rating": [random.randint(1, 5) for _ in range(1000)],
        "Sentiment": [random.choice(["Positive", "Negative", "Neutral"]) for _ in range(1000)],
    }
    return pd.DataFrame(data)

# 5. Product Descriptions
def generate_products():
    data = {
        "Product_ID": [f"PRD_{i}" for i in range(1000)],
        "Product_Name": [random.choice(["Wireless Mouse", "Smartphone", "Headphones", "Smartwatch", "Backpack"]) for _ in range(1000)],
        "Description": [random.choice([
            "Lightweight and durable with ergonomic design.",
            "High-performance and battery-efficient device.",
            "Compact and stylish design with premium finish.",
            "Water-resistant build with long-lasting materials.",
            "Affordable option with reliable quality."
        ]) for _ in range(1000)],
        "Category": [random.choice(["Electronics", "Accessories", "Wearables"]) for _ in range(1000)],
        "Price_USD": [random.randint(10, 500) for _ in range(1000)],
    }
    return pd.DataFrame(data)

datasets = {
    "customer_reviews.xlsx": generate_reviews(),
    "support_tickets.xlsx": generate_tickets(),
    "social_posts.xlsx": generate_posts(),
    "employee_feedback.xlsx": generate_feedback(),
    "product_descriptions.xlsx": generate_products(),
}

for name, df in datasets.items():
    df.to_excel(os.path.join(OUTPUT_DIR, name), index=False)
    print(f"Created unstructured dataset: {name}")