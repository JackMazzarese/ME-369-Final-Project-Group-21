#https://www.kaggle.com/datasets/kritanjalijain/amazon-reviews

#imports
import polars as pl
import matplotlib.pyplot as plt
from wordcloud import WordCloud

#loading data
raw_data = pl.read_csv('file path for data.csv')   #use file path for downloaded data.csv
data_for_analysis = raw_data.select(
    pl.col("name"),
    pl.col("reviews.date"),
    pl.col("reviews.rating"),
    pl.col("reviews.text"),
    pl.col("reviews.title"),
    pl.col("reviews.username")
)

#for sake of testing with a single item
amazon_tap_data = data_for_analysis.filter(pl.col("name") == "Amazon Tap - Alexa-Enabled Portable Bluetooth Speaker")

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
def filter_data(
    df: pl.DataFrame,
    *,
    min_rating: float | None = None,
    max_rating: float | None = None,
    min_text_length: int | None = None,   # word count
    start_month: str | None = None,       # 'YYYY-MM'
    end_month: str | None = None,         # 'YYYY-MM'
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
            plt.show()

    return filtered_df, summary


#test case
word_cloud(amazon_tap_data)
rating_distribution(amazon_tap_data)
review_timeline(amazon_tap_data)
filtered_tap, tap_summary = filter_data(
    amazon_tap_data,
    min_rating=2.0,   #can include min_rating and max_rating
    min_text_length=10,   #input minimum word count
    start_month="2015-01",      #'YYYY-MM'
    end_month="2020-01"
)
word_cloud(filtered_tap)
rating_distribution(filtered_tap)
review_timeline(filtered_tap)
print(tap_summary)

