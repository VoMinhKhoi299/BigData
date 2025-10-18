# db.py
import mysql.connector
from mysql.connector import Error
import configparser
import os

def connect_db():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), "config.ini")
    config.read(config_path)

    try:
        connection = mysql.connector.connect(
            host=config["mysql"]["host"],
            port=int(config["mysql"]["port"]),
            user=config["mysql"]["user"],
            password=config["mysql"]["password"],
            database=config["mysql"]["database"]
        )

        if connection.is_connected():
            print("✅ Đã kết nối MySQL!")
            return connection  

    except Error as e:
        print("❌ Lỗi kết nối MySQL:", e)
        return None
