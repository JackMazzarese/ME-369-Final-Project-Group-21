#Dataset used for analysis: https://www.kaggle.com/datasets/kritanjalijain/amazon-reviews

#imports
import polars as pl
import matplotlib.pyplot as plt
from wordcloud import WordCloud

#loading data
def load_data():
    raw_data = pl.read_csv('data.csv')  #use file path for downloaded data.csv
    data = raw_data.select(
        pl.col("name"),
        pl.col("reviews.date"),
        pl.col("reviews.rating"),
        pl.col("reviews.text"),
        pl.col("reviews.title"),
        pl.col("reviews.username")
        )
    return data

data_for_analysis = load_data()

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

    #if there is no text return an empty figure so streamlit wont throw error
    if len(texts) == 0:
        return plt.figure()


    joined_text = " ".join(str(t) for t in texts)

#create list of stopwords
    stopwords = set(WordCloud().stopwords)

#create and plot wordcloud
    wc = WordCloud(
        width=800,
        height=400,
        background_color="white",
        stopwords=stopwords,
        max_words=max_words
        ).generate(joined_text)

    fig = plt.figure(figsize=(10, 5))   
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout()
    return fig                          
    
    
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
    fig = plt.figure()              
    plt.bar(ratings, counts, edgecolor="black")
    plt.title("Number of Reviews by Rating")
    plt.xlabel("Rating")
    plt.ylabel("Number of Reviews")
    plt.xticks(ratings)
    plt.tight_layout()
    return fig                           
        

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
        return plt.figure()


#compute average rating for each month
    monthly = (
        data
        .group_by("year_month")
        .agg(pl.col("rating").mean().alias("avg_rating"))
        .drop_nulls()
        .sort("year_month")
    )

    if monthly.height == 0:
        return plt.figure()


    months = monthly["year_month"].to_list()
    avgs = monthly["avg_rating"].to_list()

#plot average rating by month
    fig = plt.figure()               
    plt.plot(months, avgs, marker="o")
    plt.title("Average Review Rating by Month")
    plt.xlabel("Month")
    plt.ylabel("Average Rating")
    plt.xticks(rotation=75)
    plt.tight_layout()
    return fig                         

#function to get product rows where the name matches exactly
def get_product(df: pl.DataFrame, product_name: str) -> pl.DataFrame:

    return df.filter(pl.col("name") == product_name)

#function for data filtering
def filter_data(
    df: pl.DataFrame,
    *,
    min_rating: float | None = None,         #minimum star rating, number 1-5
    max_rating: float | None = None,         #maximum star rating, number 1-5
    min_text_length: int | None = None,      # word count (integer)
    start_month: str | None = None,          # 'YYYY-MM'
    end_month: str | None = None,            # 'YYYY-MM'
    exclude_words: list[str] | None = None,  #['word 1', 'word 2', ..., 'word n']
    rating_col: str = "reviews.rating",
    text_col: str = "reviews.text",
    date_col: str = "reviews.date",
):

    if df.height == 0:
        return df, {
            "total_reviews": 0,
            "filtered_reviews": 0,
            "percent_filtered": 0.0,
            "avg_rating_before": None,
            "avg_rating_after": None,
            "filter_reason_counts": {}
        }

    data = (
        df
        .with_columns(
            pl.col(rating_col)
              .cast(pl.Float64, strict=False)
              .alias("_rating"),
            pl.col(text_col)
              .cast(pl.Utf8, strict=False)
              .alias("_text"),
            pl.col(date_col)
              .cast(pl.Utf8, strict=False)
              .alias("_date_str"),
        )
        .with_columns(
            pl.col("_date_str")
              .str.slice(0, 7)
              .alias("_year_month")
        )
    )

#build filter conditions
    condition = pl.lit(True)
    reason_cols = []

#rating filters
    if min_rating is not None:
        data = data.with_columns(
            (pl.col("_rating") < min_rating)
            .fill_null(True)
            .alias("_drop_min_rating")
        )
        condition &= ~pl.col("_drop_min_rating")
        reason_cols.append("_drop_min_rating")

    if max_rating is not None:
        data = data.with_columns(
            (pl.col("_rating") > max_rating)
            .fill_null(True)
            .alias("_drop_max_rating")
        )
        condition &= ~pl.col("_drop_max_rating")
        reason_cols.append("_drop_max_rating")

#word count filters
    if min_text_length is not None:
        data = data.with_columns(
            (
                pl.col("_text")
                  .str.split(" ")
                  .list.len()
                  < min_text_length
            )
            .fill_null(True)
            .alias("_drop_text_length")
        )
        condition &= ~pl.col("_drop_text_length")
        reason_cols.append("_drop_text_length")

#exclude words filter        
    if exclude_words:
        lowered = pl.col("_text").str.to_lowercase()

        #build 'or' condition for excluded words
        bad_conditions = [
            lowered.str.contains(word.lower()) for word in exclude_words
            ]

        #combine them, review dropped if any of the listed words appear
        data = data.with_columns(
            (pl.any_horizontal(bad_conditions))
            .fill_null(True)
            .alias("_drop_excluded_words")
            )

        condition &= ~pl.col("_drop_excluded_words")
        reason_cols.append("_drop_excluded_words")


#date/month filters
    if start_month is not None:
        data = data.with_columns(
            (pl.col("_year_month") < start_month)
            .fill_null(True)
            .alias("_drop_before_start_month")
        )
        condition &= ~pl.col("_drop_before_start_month")
        reason_cols.append("_drop_before_start_month")

    if end_month is not None:
        data = data.with_columns(
            (pl.col("_year_month") > end_month)
            .fill_null(True)
            .alias("_drop_after_end_month")
        )
        condition &= ~pl.col("_drop_after_end_month")
        reason_cols.append("_drop_after_end_month")

#apply filter
    filtered = data.filter(condition)

#provide summary
    total_reviews = df.height
    filtered_reviews = filtered.height
    num_dropped = total_reviews - filtered_reviews

    percent_filtered = (
        0.0 if total_reviews == 0
        else num_dropped / total_reviews * 100.0
    )

#average ratings
    avg_before = (
        df.select(pl.col(rating_col).cast(pl.Float64, strict=False).mean())
          .to_series()[0]
    )

    avg_after = (
        filtered.select(pl.col(rating_col).cast(pl.Float64, strict=False).mean())
                .to_series()[0]
        if filtered_reviews > 0
        else None
    )

#count of reasons for filtering
    filter_reason_counts = {}
    if reason_cols:
        counts_row = (
            data
            .select([pl.count().alias("total")] +
                    [pl.col(c).sum().alias(c) for c in reason_cols])
            .to_dict(as_series=False)
        )
        for c in reason_cols:
            nice_name = {
                "_drop_min_rating": "Rating Below Min",
                "_drop_max_rating": "Rating Above Max",
                "_drop_text_length": "Text Too Short",
                "_drop_before_start_month": "Before Start Month",
                "_drop_after_end_month": "After End Month",
                "_drop_excluded_words": "Contains Excluded Words"
            }.get(c, c)
            filter_reason_counts[nice_name] = counts_row[c][0]

#drop helper columns from returned filtered Data Frame
    drop_cols = [
        c for c in filtered.columns
        if c.startswith("_drop_") or c in ("_rating", "_text", "_date_str", "_year_month")
    ]
    filtered_df = filtered.drop(drop_cols)

    summary = {
        "Total Reviews": total_reviews,
        "Filtered Reviews": filtered_reviews,
        "Percent Filtered": percent_filtered,
        "Avg Rating Before": avg_before,
        "Avg Rating After": avg_after,
        "Filter Reason Counts": filter_reason_counts
    }

#Pie chart of filtering reasons
    fig_pie = None
    if num_dropped > 0 and filter_reason_counts:
        #drop zero count reasons
        nonzero = {k: v for k, v in filter_reason_counts.items() if v > 0}

        if nonzero:
            labels = list(nonzero.keys())
            counts = list(nonzero.values())

            total_for_pie = sum(counts)
            percentages = [(c / total_for_pie) * 100 for c in counts]

            fig, ax = plt.subplots(figsize=(8, 6))
            wedges, _ = ax.pie(
                counts,
                startangle=140
            )

            legend_labels = [
                f"{labels[i]} — {percentages[i]:.1f}%" for i in range(len(labels))
            ]

            ax.legend(
                wedges,
                legend_labels,
                title="Filtering Reasons",
                loc="center left",
                bbox_to_anchor=(1, 0.5)
            )

            ax.set_title("Reasons Reviews Were Filtered Out")
            plt.tight_layout()
            fig_pie = fig

    return filtered_df, summary, fig_pie

