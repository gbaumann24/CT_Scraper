# File: preprocess_data.py

import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer
import joblib

# Load the cleaned data.
df = pd.read_csv("../cleaned_reviews.csv")

# Drop records missing essential columns.
df = df.dropna(subset=["company_field", "company_size", "categories", "product"])

# Debug: Print a sample of the original 'categories' column.
print("Sample 'categories' entries:")
print(df["categories"].head(5))

# Create the 'categories_list' column from the 'categories' string.
df["categories_list"] = df["categories"].astype(str).apply(lambda x: x.lower().split())

# Debug: Print a sample of the newly created 'categories_list' column.
print("Sample 'categories_list' entries:")
print(df["categories_list"].head(5))

# One-hot encode the company field and company size.
df_encoded = pd.get_dummies(df, columns=["company_field", "company_size"], prefix=["field", "size"])

# Initialize and fit the MultiLabelBinarizer on the new column.
mlb = MultiLabelBinarizer()
categories_encoded = pd.DataFrame(
    mlb.fit_transform(df["categories_list"]),
    columns=["cat_" + c for c in mlb.classes_],
    index=df.index
)

# Combine all features.
df_processed = pd.concat([df_encoded, categories_encoded], axis=1)

# Save the processed training data and the encoders for later use.
df_processed.to_csv("training_data.csv", index=False)
joblib.dump(mlb, "mlb_categories.pkl")

# Use errors='ignore' in case 'categories_list' is missing in df_processed.
feature_columns = df_processed.drop(columns=["product", "categories", "categories_list"], errors='ignore').columns.tolist()
joblib.dump(feature_columns, "feature_columns.pkl")

print("Data preprocessing complete. Processed data saved to 'training_data.csv'.")
print("MultiLabelBinarizer saved to 'mlb_categories.pkl'.")
print("Feature columns saved to 'feature_columns.pkl'.")