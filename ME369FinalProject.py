#instead of using scraper, going to use a dataset from kaggle
#https://www.kaggle.com/datasets/kritanjalijain/amazon-reviews
#notes: define functions, then call seperately for filtered and unfiltered data

#imports
import polars as pl
import matplotlib.pyplot as plt
from wordcloud import WordCloud

#loading data
raw_data = pl.read_csv('C:/Users/jack/Desktop/School Stuff/Programming Applications for Engineering/Final Project/data.csv')
data_for_analysis = raw_data.select(
    pl.col("name"),
    pl.col("reviews.date"),
    pl.col("reviews.rating"),
    pl.col("reviews.text"),
    pl.col("reviews.title"),
    pl.col("reviews.username")
)

#for sake of testing with a single item
kindle_paperwhite_data = data_for_analysis.filter(pl.col("name") == "Kindle Paperwhite")

#function for word cloud creation
def word_cloud(df: pl.DataFrame,
    text_col: str = "reviews.text",
    extra_stopwords=None,
    max_words: int = 150):

# drop nulls and collect text
    texts = (
    df
    .select(pl.col(text_col))
    .drop_nulls()
    .to_series()
    .to_list()
    )

    joined_text = " ".join(str(t) for t in texts)

#create list of stopwords using pre existing as well as adding custom stopwords
    stopwords = set(WordCloud().stopwords)
    default_extra = {
    "Amazon", "Kindle"
    }
    
    if extra_stopwords:
        default_extra |= set(extra_stopwords)
        stopwords |= default_extra

#create and plot wordcloud
    wc = WordCloud(
        width=800,
        height=400,
        background_color="white",
        stopwords=stopwords,
        max_words=max_words
        ).generate(joined_text)

    plt.figure(figsize=(10, 5))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout()
    plt.show()
    

#function for rating distribution visualization
def rating_distribution(df: pl.DataFrame, rating_col: str = "reviews.rating"):
    
#group by rating and count
    grouped = (
        df
        .filter(pl.col(rating_col).is_not_null())
        .group_by(rating_col)
        .agg(pl.len().alias("n_reviews"))
        .sort(rating_col)
    )

    ratings = grouped[rating_col].to_list()
    counts = grouped["n_reviews"].to_list()

#plot bar graph
    plt.figure()
    plt.bar(ratings, counts, edgecolor="black")
    plt.title("Number of Reviews by Rating")
    plt.xlabel("Rating")
    plt.ylabel("Number of Reviews")
    plt.xticks(ratings)
    plt.tight_layout()
    plt.show()
    

#function for review timeline visualization
def review_timeline(df: pl.DataFrame,
                            date_col: str = "reviews.date",
                            rating_col: str = "reviews.rating"):
#clean and parse dates
    data = (
        df
        .select(
            pl.col(date_col).alias("date_str"),
            pl.col(rating_col).alias("rating_raw")
        )
        .with_columns(
#slice only year and month
            pl.col("date_str")
              .str.slice(0, 7)
              .alias("year_month"),

            pl.col("rating_raw")
              .cast(pl.Float64, strict=False)
              .alias("rating")
        )
        .drop_nulls()
        .sort("year_month")
    )

    if data.height == 0:
        return

#compute average rating for each month
    monthly = (
        data
        .group_by("year_month")
        .agg(pl.col("rating").mean().alias("avg_rating"))
        .drop_nulls()
        .sort("year_month")
    )

    if monthly.height == 0:
        return

    months = monthly["year_month"].to_list()
    avgs = monthly["avg_rating"].to_list()

#plot average rating by month
    plt.figure()
    plt.plot(months, avgs, marker="o")
    plt.title("Average Review Rating by Month")
    plt.xlabel("Month")
    plt.ylabel("Average Rating")
    plt.xticks(rotation=75)
    plt.tight_layout()
    plt.show()


#function for data filtering
def filter_data('raw data', parameters*) 
#create sets of pre existing parameters for filtering
#bulk of filtering and cleaning code
#output filtered data set, total number of reviews, average rating, and percent filtered
#keep a count of the reasons for filtering