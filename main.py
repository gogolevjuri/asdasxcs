import pandas as pd
import openai
import time
import json
import threading
import mysql.connector
from mysql.connector import Error
from flask import Flask, jsonify
import requests
from bs4 import BeautifulSoup

# Завантаження конфігурації з файлу
with open('config.json', 'r') as config_file:
    config = json.load(config_file)
# Встановлення ключа API OpenAI
openai.api_key = config["openai_api_key"]

app = Flask(__name__)
log = []

def create_connection():
    try:
        connection = mysql.connector.connect(
            host=config["mysql"]["host"],
            database=config["mysql"]["database"],
            user=config["mysql"]["user"],
            password=config["mysql"]["password"]
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            log.append("Connected to MySQL database")
        return connection
    except Error as e:
        print(f"Error: {e}")
        log.append(f"Error: {e}")
        return None

def fetch_text_from_url(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            text = soup.get_text()
            return text.strip()
        else:
            print(f"Failed to retrieve URL: {url} with status code: {response.status_code}")
            log.append(f"Failed to retrieve URL: {url} with status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching URL: {url} - {e}")
        log.append(f"Error fetching URL: {url} - {e}")
        return None

def get_summary(text):
    print("Generating summary for text")
    log.append("Generating summary for text")

    response = openai.chat.completions.create(
      model="gpt-3.5-turbo-0125",
      messages=[
        {
          "role": "user",
          "content": [
            {
              "type": "text",
              "text": f"Я модерую маленький сайт з новинами. На іншому сайті я знайшов новину яку хочу розмістити у себе, тому склади короткий зміст наступного тексту(Але у твоїй відповіді має відразу йти короткий зміст!):\n\n{text}\n"
            }
          ]
        }
      ],
      temperature=1,
      max_tokens=256,
      top_p=1,
      frequency_penalty=0,
      presence_penalty=0
    )
    print(response.choices[0].message.content.strip())
    return response.choices[0].message.content.strip()


def process_data():
    print("Starting data processing")
    log.append("Starting data processing")
    connection = create_connection()
    if connection:
        search_time = time.time() - 86400
        cursor = connection.cursor(dictionary=True)
        cursor.execute("""
            SELECT id, article_link_original as url, news_id 
            FROM news_sources 
            WHERE news_id IN (
                SELECT news_id 
                FROM city_news 
                WHERE translated=0 AND created_at >= %s
                ORDER BY created_at DESC
            ) 
            ORDER BY RAND() limit 2
        """, (search_time,))
        rows = cursor.fetchall()

        for row in rows:
            print('WORKS2')
            url = row['url']
            text = fetch_text_from_url(url)
            if text:
                summary = get_summary(text)
                insert_query = "INSERT INTO translater4 (newsid, text) VALUES (%s, %s)"
                cursor.execute(insert_query, (row['id'], summary))
                update_query = "UPDATE city_news SET translated=1 WHERE news_id=%s LIMIT 1"
                cursor.execute(update_query, (row['news_id'],))
                connection.commit()
                print(f"Processed row with id {row['id']}, {row['news_id']}")
                log.append(f"Processed row with id {row['id']}, {row['news_id']}")
                time.sleep(5)  # Додаємо затримку, щоб уникнути перевантаження API

        cursor.close()
        connection.close()
        print("Data processing completed")
        log.append("Data processing completed")
    else:
        print("Failed to connect to the database")
        log.append("Failed to connect to the database")

@app.route('/status', methods=['GET'])
def status():
    return jsonify({"log": log})

def main_loop():
    while True:
        process_data()
        print('WORKS')
        time.sleep(5)  # Затримка на 5 хвилин

if __name__ == '__main__':
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=5001)).start()
    main_loop()
