
#imports
import streamlit as st

import polars as pl

import matplotlib.pyplot as plt

import time

from datamonkeys_backend import get_product, word_cloud, rating_distribution, review_timeline, load_data, filter_data

st.set_page_config(page_title="Amazon Review Dashboard", layout="wide")
st.title("Amazon Review Dashboard")

#loading data
data_for_analysis = load_data()

#allows user to choose which product they'd like to see data for
product_name = st.text_input("Enter Product Name:")

#displaying text instructions for how to use the dashboard
st.markdown("""
Use this tool to compare filtered vs unfiltered data on Amazon product reviews. 

**User Guide:**
1. Type product name exactly as listed in column M of the data.csv
2. Adjust filters on the left sidebar
3. View charts below  
4. Compare filtered vs unfiltered tabs 
""")

#condition to proceed if box is not empty 
if product_name:
    product_review_data = get_product(data_for_analysis, product_name) #filtering through the reviews from file
    
    if product_review_data.height == 0:
        st.error("No matching product found.") #throw message if no rows exist that EXACTLY match the text are found
    else:
        st.success(f"Loaded {product_review_data.height} reviews")
        
        #generating unfiltered graphs and storing unaffected versions (meaning filters will not affect them)
        if 'most_recent_product' not in st.session_state or st.session_state.most_recent_product != product_name:
            st.session_state.most_recent_product = product_name
            st.session_state.unfiltered_rating = rating_distribution(product_review_data)
            st.session_state.unfiltered_timeline = review_timeline(product_review_data)
            st.session_state.unfiltered_wordcloud = word_cloud(product_review_data)
        
        #creating sidebar filters
        st.sidebar.header("Filters")
        min_words = st.sidebar.slider("Min Word Count", 0, 100, 10)
        selected_stars = st.sidebar.multiselect("Star Ratings", [1,2,3,4,5], [1,2,3,4,5])
        time_period = st.sidebar.selectbox("Time Period", ["All time", "Last 6 months", "Last year"])
        
        apply_filters = st.sidebar.button("Apply Filters")
        
        #applying filters
        filtered_data = None
        all_data = None
        fig_pie = None
        
        #date filter
        if apply_filters: #acts as if the current year is 2017 because of our limited dataset
            start_month = None
            end_month = None
            if time_period == "Last 6 months":
                start_month = "2016-06"
                end_month = "2016-12"
            elif time_period == "Last year":
                start_month = "2016-01"
                end_month = "2017-01"
            
            #rating filter
            min_rating = min(selected_stars) if selected_stars else None
            max_rating = max(selected_stars) if selected_stars else None
            
            #calling backend function to filter reviews
            filtered_data, all_data, fig_pie = filter_data(
                product_review_data,
                min_rating=min_rating,
                max_rating=max_rating,
                min_text_length=min_words,
                start_month=start_month,
                end_month=end_month
            )

        #creating filtered and unfiltered tabs
        unfiltered_tab, filtered_tab = st.tabs(["Unfiltered Reviews", "Filtered Reviews"])
        
        #unfiltered tab
        with unfiltered_tab:
            st.subheader("Unfiltered Reviews")
            left_side_column, right_side_column = st.columns(2)
            with left_side_column:
                st.pyplot(st.session_state.unfiltered_rating)
            with right_side_column:
                st.pyplot(st.session_state.unfiltered_timeline)
            st.pyplot(st.session_state.unfiltered_wordcloud)
        
        #filtered tab
        with filtered_tab:
            st.subheader("Filtered Reviews")
            if filtered_data is None:
                st.info("Apply filters to see results")
            else:
                left_side_column, right_side_column = st.columns(2)
                with left_side_column:
                    fig = rating_distribution(filtered_data)
                    st.pyplot(fig)
                    plt.close(fig)
                with right_side_column:
                    fig = review_timeline(filtered_data)
                    st.pyplot(fig)
                    plt.close(fig)
                
                fig = word_cloud(filtered_data)
                st.pyplot(fig)
                plt.close(fig)
                
                if fig_pie:
                    st.pyplot(fig_pie)
                    plt.close(fig_pie)
                    
                #displays all_data dict below pie chart just to see analyzed data
                if all_data:
                    st.text(str(all_data))
                    
#helps user and shows message if they enter no text
else:
    st.info("Enter a product name to start")