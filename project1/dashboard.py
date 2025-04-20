import streamlit as st
import pandas as pd
import psycopg2
import altair as alt

# Connects Streamlit to the PostgreSQL database and visualizes YouTube trending data.

DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASSWORD = "airflow"
DB_HOST = "postgres"
DB_PORT = "5432"

# Connects to PostgreSQL and runs a given SQL query.
# Returns the result as a pandas DataFrame.
def load_data(query):
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        host=DB_HOST, port=DB_PORT
    )
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

st.set_page_config(page_title="YouTube Trending Dashboard", layout="wide")
st.title("YouTube Trending Dashboard")

region_query = "SELECT DISTINCT region FROM top_10_videos ORDER BY region;"
region_df = load_data(region_query)

region_options = region_df["region"].tolist()
region = st.selectbox("Select a region", region_options)

st.subheader("Top 10 Trending Videos")
query_videos = f"""
    SELECT title, channel, view_count
    FROM top_10_videos
    WHERE region = '{region}'
    ORDER BY view_count DESC
    LIMIT 10
"""
videos_df = load_data(query_videos)
video_data = videos_df[["title", "view_count"]]
video_chart = alt.Chart(video_data).mark_bar().encode(
    x=alt.X("title:N", sort="-y", title="Video Title"),
    y=alt.Y("view_count:Q", title="View Count"),
    tooltip=["title", "view_count"]
).properties(
    width=700,
    height=400
).configure_mark(
    strokeWidth=0
).configure_axisX(
    labelAngle=0 
)

st.altair_chart(video_chart, use_container_width=True)

st.subheader("Top 5 Categories by Average Views")
query_categories = f"""
    SELECT category, view_count
    FROM top_5_categories
    WHERE region = '{region}'
    ORDER BY view_count DESC
    LIMIT 5
"""
categories_df = load_data(query_categories)

categories_df["category"] = categories_df["category"].astype(int)
category_mapping = {
    1: "Film & Animation",
    2: "Autos & Vehicles",
    10: "Music",
    15: "Pets & Animals",
    17: "Sports",
    18: "Short Movies",
    19: "Travel & Events",
    20: "Gaming",
    21: "Videoblogging",
    22: "People & Blogs",
    23: "Comedy",
    24: "Entertainment",
    25: "News & Politics",
    26: "Howto & Style",
    27: "Education",
    28: "Science & Technology",
    29: "Nonprofits & Activism",
    30: "Movies",
    31: "Anime/Animation",
    32: "Action/Adventure",
    33: "Classics",
    34: "Comedy",
    35: "Documentary",
    36: "Drama",
    37: "Family",
    38: "Foreign",
    39: "Horror",
    40: "Sci-Fi/Fantasy",
    41: "Thriller",
    42: "Shorts",
    43: "Shows",
    44: "Trailers"
}

categories_df["category"] = categories_df["category"].map(category_mapping).fillna("Unknown")

bar_data = categories_df[["category", "view_count"]]
bar_chart = alt.Chart(bar_data).mark_bar().encode(
    x=alt.X("category:N", sort="-y", title="Category"),
    y=alt.Y("view_count:Q", title="View Count"),
    tooltip=["category", "view_count"]
).properties(
    width=700,
    height=400
).configure_mark(
    strokeWidth=0
).configure_axisX(
    labelAngle=0  # 👈 Horizontal labels!
)

st.altair_chart(bar_chart, use_container_width=True)