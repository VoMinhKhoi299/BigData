# fetch_data.py
import mysql.connector
from mysql.connector import Error
from db import connect_db  


def get_spotify_data(limit=10):
    """Lấy dữ liệu từ bảng spotify_viral_chart"""
    try:
        conn = connect_db()
        cursor = conn.cursor(dictionary=True)
        query = f"SELECT * FROM spotify ORDER BY date DESC LIMIT {limit};"
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except Error as e:
        print("❌ Lỗi khi lấy dữ liệu Spotify:", e)
        return []
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def get_articles_data(limit=10):
    """Lấy dữ liệu từ bảng articles"""
    try:
        conn = connect_db()
        cursor = conn.cursor(dictionary=True)
        query = f"SELECT * FROM articles ORDER BY published_at DESC LIMIT {limit};"
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except Error as e:
        print("❌ Lỗi khi lấy dữ liệu Articles:", e)
        return []
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


if __name__ == "__main__":
    print("🎵 Spotify Viral Chart:")
    for row in get_spotify_data():
        print(row)

    print("\n📰 Articles:")
    for row in get_articles_data():
        print(row)
