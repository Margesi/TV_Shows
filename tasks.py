import requests
import mysql.connector
from mysql.connector import Error
from credentials import db_password, db_host, db_user

# TV shows to query
shows = ["Silo", "Reacher", "Yellowstone"]

# Database connection details

def connect_to_db(db_host=db_host, db_user=db_user, db_password=db_password):
    try:
        connection = mysql.connector.connect(
            host=db_host,
            database="shows",
            user=db_user,
            password=db_password
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print("Error while connecting to MySQL", e)
# Function to get show data from TVmaze API

# def create_tables(connection):
#     try:
#         cursor = connection.cursor()
#         create_show_table = """
#         CREATE TABLE IF NOT EXISTS shows (
#             id INT AUTO_INCREMENT PRIMARY KEY,
#             name VARCHAR(255),
#             genres VARCHAR(255),
#             premiered DATE
#         );
#         """
#         create_episode_table = """
#         CREATE TABLE IF NOT EXISTS episodes (
#             id INT AUTO_INCREMENT PRIMARY KEY,
#             show_id INT,
#             name VARCHAR(255),
#             duration INT,
#             airdate DATE,
#             FOREIGN KEY (show_id) REFERENCES shows(id)
#         );
#         """
#         cursor.execute(create_show_table)
#         cursor.execute(create_episode_table)
#         connection.commit()
#     except Error as e:
#         print("Error while creating tables", e)


def create_tables(connection):
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS shows (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            genres VARCHAR(255),
            premiered DATE
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS episodes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            show_id INT,
            name VARCHAR(255),
            duration INT,
            airdate DATE,
            FOREIGN KEY (show_id) REFERENCES shows(id)
        );
    """)
    connection.commit()


def fetch_and_store_show_data(connection, show_name):
    api_url = f'http://api.tvmaze.com/singlesearch/shows?q={show_name}&embed=episodes'
    response = requests.get(api_url)
    show_data = response.json()

    cursor = connection.cursor()
    genres = ', '.join(show_data['genres'])  # Korrekte Verarbeitung der Genres als Liste von Strings
    cursor.execute("INSERT INTO shows (name, genres, premiered) VALUES (%s, %s, %s)",
                   (show_data['name'], genres, show_data['premiered']))
    show_id = cursor.lastrowid

    for episode in show_data['_embedded']['episodes']:
        cursor.execute("INSERT INTO episodes (show_id, name, duration, airdate) VALUES (%s, %s, %s, %s)",
                       (show_id, episode['name'], episode['runtime'], episode['airdate']))

    connection.commit()

def main():
    connection = connect_to_db()
    create_tables(connection)
    shows = ['Silo', 'Reacher', 'Yellowstone']
    for show in shows:
        fetch_and_store_show_data(connection, show)


if __name__ == "__main__":
    main()