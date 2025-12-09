This repository contains the front end and back end code for an Amazon product review analyzer with an interactive user interface to allow for filtering and visualization, as well as a sample dataset taken from Kaggle with product reviews for various Amazon products.

datamonkeys_backend.py contains the back end code, consisting of the functions for loading, filtering, and visualizing the data.

datamonkeys_frontend.py contains the front end code, which handles the User Interface using streamlit.

data.csv contains the sample dataset used for this project, this dataset was taken from the following link: https://www.kaggle.com/datasets/kritanjalijain/amazon-reviews

How to Run: 

The dashboard expects the file named data.csv in the same folder.

1. Install dependencies by running "pip install streamlit polars matplotlib wordcloud" in your terminal

2. Make sure your project folder looks like: (project-folder) --> streamlit_app.py, datamonkeys_backend.py, data.csv, datamonkeys_frontend.py

4. Start Streamlit from inside your project directory. Run "streamlit run streamlit_app.py" in your Terminal. 

This will open a tab in your browser with the dashboard.
