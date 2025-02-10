# File: app.py

import streamlit as st
import pandas as pd
import joblib
import scrape_categories  # Assumes this module defines get_slugs()
import numpy as np

# --- Load preprocessed objects ---
@st.cache(allow_output_mutation=True)
def load_data():
    df = pd.read_csv("cleaned_reviews.csv")
    return df

@st.cache(allow_output_mutation=True)
def load_model():
    model = joblib.load("tool_recommender_model.pkl")
    return model

@st.cache(allow_output_mutation=True)
def load_encoders():
    mlb = joblib.load("mlb_categories.pkl")
    feature_columns = joblib.load("feature_columns.pkl")
    return mlb, feature_columns

# --- Helper function to process user input ---
def preprocess_user_input(field, size, category, df, mlb, feature_columns):
    # Create a DataFrame for the new input.
    input_dict = {
        "company_field": [field],
        "company_size": [size],
        "categories": [category]  # We'll process categories separately.
    }
    input_df = pd.DataFrame(input_dict)
    # Process the 'categories' field (assume it is a single slug, so we wrap it in a list).
    input_df["categories_list"] = input_df["categories"].apply(lambda x: x.lower().split())
    
    # One-hot encode company_field and company_size using pd.get_dummies.
    input_encoded = pd.get_dummies(input_df, columns=["company_field", "company_size"], prefix=["field", "size"])
    
    # Process categories with the pre-fitted MultiLabelBinarizer.
    categories_encoded = pd.DataFrame(
        mlb.transform(input_df["categories_list"]),
        columns=["cat_" + c for c in mlb.classes_]
    )
    
    # Combine the data.
    input_processed = pd.concat([input_encoded, categories_encoded], axis=1)
    # Drop unnecessary columns.
    for col in ["categories", "categories_list"]:
        if col in input_processed.columns:
            input_processed = input_processed.drop(columns=[col])
    
    # Ensure that all columns present during training are in the input.
    input_processed = input_processed.reindex(columns=feature_columns, fill_value=0)
    return input_processed

# --- Streamlit App ---
st.title("Company Tool Recommendation")

st.markdown("""
**Select your criteria below** to see:
- How many companies match your selection.
- The predicted tool recommendation based on your company profile.
""")

# Load data, model, and encoders.
df = load_data()
model = load_model()
mlb, feature_columns = load_encoders()
available_categories = scrape_categories.get_slugs()

# Get unique company fields and sizes from the cleaned data.
company_fields = sorted(df["company_field"].dropna().unique())
company_sizes = sorted(df["company_size"].dropna().unique())

# Drop-down menus.
selected_field = st.selectbox("Select Company Field", options=["All"] + company_fields)
selected_size = st.selectbox("Select Company Size", options=["All"] + company_sizes)
selected_category = st.selectbox("Select Tool Category (Slug)", options=["All"] + available_categories)

# --- Filtering the Data ---
def filter_data(df, field, size, category):
    filtered = df.copy()
    if field != "All":
        filtered = filtered[filtered["company_field"].str.lower().str.contains(field.lower(), na=False)]
    if size != "All":
        filtered = filtered[filtered["company_size"].str.lower().str.contains(size.lower(), na=False)]
    if category != "All":
        filtered = filtered[filtered["categories"].str.lower().str.contains(category.lower(), na=False)]
    return filtered

filtered_df = filter_data(df, selected_field, selected_size, selected_category)
st.subheader("Filtered Data Summary")
st.write(f"Number of companies matching your criteria: {len(filtered_df)}")

# --- Prediction Section ---
st.subheader("Tool Recommendation")

if st.button("Get Recommendation"):
    # Preprocess the user input.
    # For fields that are "All", we decide to use a default value or leave them empty.
    user_field = "" if selected_field == "All" else selected_field
    user_size = "" if selected_size == "All" else selected_size
    user_category = "" if selected_category == "All" else selected_category
    
    user_features = preprocess_user_input(user_field, user_size, user_category, df, mlb, feature_columns)
    # Predict probabilities.
    probs = model.predict_proba(user_features)
    prob_dict = dict(zip(model.classes_, probs[0]))
    
    if prob_dict:
        top_tool = max(prob_dict, key=prob_dict.get)
        top_prob = prob_dict[top_tool]
        
        st.markdown(f"**Recommended tool:** `{top_tool}`")
        st.markdown(f"**Probability:** {top_prob:.2%}")
        
        st.markdown("**Full Probability Distribution:**")
        st.write(prob_dict)
    else:
        st.error("Could not generate a recommendation. Please check your input.")