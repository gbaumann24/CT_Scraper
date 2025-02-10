import streamlit as st
import pandas as pd
import scrape_categories  # Make sure this module defines get_slugs()
import sys

# --- Data Loading and Caching ---
@st.cache
def load_data(file_path="cleaned_reviews.csv"):
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        st.error(f"Error loading CSV file: {e}")
        sys.exit(1)
    return df

# --- Get Available Categories from scrape_categories ---
@st.cache(allow_output_mutation=True)
def get_available_categories():
    try:
        category_slugs = scrape_categories.get_slugs()
    except Exception as e:
        st.error(f"Error retrieving category slugs: {e}")
        sys.exit(1)
    return category_slugs

# --- Filtering and Probability Calculation Functions ---
def filter_data(df, field, size, category):
    """
    Filter the DataFrame based on company field, company size, and tool category.
    The 'categories' column is assumed to contain space-separated slugs.
    """
    filtered = df.copy()
    if field != "All":
        # Case-insensitive exact match or substring match.
        filtered = filtered[filtered["company_field"].str.lower().str.contains(field.lower(), na=False)]
    if size != "All":
        filtered = filtered[filtered["company_size"].str.lower().str.contains(size.lower(), na=False)]
    if category != "All":
        # For categories, we assume the 'categories' column may contain multiple space-separated slugs.
        filtered = filtered[filtered["categories"].str.lower().str.contains(category.lower(), na=False)]
    return filtered

def calculate_tool_probabilities(filtered_df):
    """
    Calculate and return a probability distribution (relative frequencies)
    for the product/tool from the filtered DataFrame, along with the raw counts.
    """
    tool_counts = filtered_df["product"].value_counts()
    if tool_counts.empty:
        return None, None
    total = tool_counts.sum()
    probabilities = (tool_counts / total).sort_values(ascending=False)
    return probabilities, tool_counts

# --- Streamlit App ---
st.title("Company Tool Recommendation")

st.markdown("""
**Select criteria from the drop-downs below** to see:
- How many companies match your selection.
- The most likely tool (product) that companies with the same field, size, 
  and tool category have implemented.
""")

# Load data and available categories.
df = load_data("cleaned_reviews.csv")
available_categories = get_available_categories()

# Get unique values for company fields and sizes from the dataset.
company_fields = sorted(df["company_field"].dropna().unique())
company_sizes = sorted(df["company_size"].dropna().unique())

# Create drop-down menus (select boxes) for filtering.
selected_field = st.selectbox("Select Company Field", options=["All"] + company_fields)
selected_size = st.selectbox("Select Company Size", options=["All"] + company_sizes)
selected_category = st.selectbox("Select Tool Category (Slug)", options=["All"] + available_categories)

# Filter data based on user selections.
filtered_df = filter_data(df, selected_field, selected_size, selected_category)

st.subheader("Filtered Data Summary")
st.write(f"**Number of companies matching your criteria:** {len(filtered_df)}")

if len(filtered_df) == 0:
    st.error("No matching records found for the provided criteria. Please adjust your selections.")
else:
    # Calculate probabilities and counts.
    probabilities, tool_counts = calculate_tool_probabilities(filtered_df)
    if probabilities is None:
        st.error("No product data available in the filtered results.")
    else:
        top_tool = probabilities.index[0]
        top_prob = probabilities.iloc[0]
    
        st.subheader("Recommendation Results")
        st.markdown(f"**Most likely tool:** `{top_tool}`")
        st.markdown(f"**Probability:** {top_prob:.2%}")
    
        st.markdown("**Candidate Tools with Probabilities and Counts:**")
        # Create a DataFrame to display the probabilities and counts.
        prob_df = pd.DataFrame({
            "Tool": probabilities.index,
            "Probability": probabilities.values,
            "Count": tool_counts.loc[probabilities.index].values
        }).reset_index(drop=True)
        st.dataframe(prob_df)

        # Optional: Allow user to display a sample of matching records.
        if st.checkbox("Show a sample of matching records"):
            st.dataframe(filtered_df.head(10))