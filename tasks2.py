from flask import Flask, render_template, request, redirect, url_for
import statistics
from tasks import connect_to_db
app = Flask(__name__)

def ensure_database_structure(connect_to_db = connect_to_db):
    connection = connect_to_db()
    cursor = connection.cursor()
    # Überprüfen, ob die 'rating'-Spalte existiert
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name='shows' AND column_name='rating';
    """)
    result = cursor.fetchone()
    if not result:
        # Fügt die 'rating'-Spalte hinzu, wenn sie nicht existiert
        cursor.execute("""
            ALTER TABLE shows ADD COLUMN rating DECIMAL(3,1) NULL;
        """)
        print("Added 'rating' column to 'shows' table.")
    else:
        print("'rating' column already exists.")
    connection.commit()

@app.route('/')
def index():
    conn = connect_to_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute('SELECT * FROM shows')
    shows = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('index.html', shows=shows)


@app.route('/show_details')
def show_details():
    conn = connect_to_db()
    cursor = conn.cursor(dictionary=True)

    # Fetch all shows
    cursor.execute('SELECT * FROM shows')
    shows = cursor.fetchall()

    # Prepare data structure to include episodes and average duration for each show
    for show in shows:
        # Fetch episodes for each show
        cursor.execute('SELECT * FROM episodes WHERE show_id = %s', (show['id'],))
        episodes = cursor.fetchall()
        show['episodes'] = episodes

        # Calculate average duration if episodes are present
        if episodes:
            average_duration = sum(ep['duration'] for ep in episodes if ep['duration']) / len(
                [ep for ep in episodes if ep['duration']])
            median_duration = statistics.median(ep['duration'] for ep in episodes if ep['duration'])
            show['average_duration'] = average_duration
            show['median_duration'] = median_duration
        else:
            show['average_duration'] = None
            show['median_duration'] = None

    cursor.close()
    conn.close()

    # Pass all shows along with their episodes and calculated average durations
    return render_template('show_details.html', shows=shows)

@app.route('/rate_show', methods=['POST'])
def rate_show():
    ensure_database_structure()
    show_id = request.form['show_id']
    rating = request.form['rating']
    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute('UPDATE shows SET rating = %s WHERE id = %s', (rating, show_id))
    conn.commit()
    cursor.close()
    conn.close()
    return redirect(url_for('index'))

if __name__ == "__main__":
    app.run(debug=True)