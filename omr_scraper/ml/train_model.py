# File: train_model.py

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
import joblib

# Load the processed training data.
df = pd.read_csv("training_data.csv")

# Define the target and features.
# Drop columns that are not features.
X = df.drop(columns=["product", "categories", "categories_list"])
y = df["product"]

# Split the data.
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize and train a logistic regression classifier.
clf = LogisticRegression(multi_class='multinomial', solver='lbfgs', max_iter=500)
clf.fit(X_train, y_train)

# Evaluate the model.
y_pred = clf.predict(X_test)
print("Accuracy:", accuracy_score(y_test, y_pred))
print(classification_report(y_test, y_pred))

# Save the model.
joblib.dump(clf, "tool_recommender_model.pkl")
print("Model saved to 'tool_recommender_model.pkl'.")

# Print training results
print("\nTraining Results:")
y_train_pred = clf.predict(X_train)
print("Training Accuracy:", accuracy_score(y_train, y_train_pred))
print(classification_report(y_train, y_train_pred))

print("\nTest Results:")
print("Test Accuracy:", accuracy_score(y_test, y_pred))
print(classification_report(y_test, y_pred))