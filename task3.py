import mysql.connector
import pandas as pd
from mysql.connector import connect
from tasks import connect_to_db


def export_to_parquet():
    # Establish database connection
    conn = connect_to_db()

    # SQL query to fetch all shows
    show_query = 'SELECT * FROM shows'
    episode_query = 'SELECT * FROM episodes'

    # Convert SQL query to pandas DataFrame
    shows_df = pd.read_sql(show_query, conn)
    episodes_df = pd.read_sql(episode_query, conn)

    # Close the database connection
    conn.close()

    # Write data to Parquet file
    shows_df.to_parquet('shows.parquet')
    episodes_df.to_parquet('episodes.parquet')

    print('Data has been exported to Parquet files.')

if __name__ == "__main__":
    export_to_parquet()