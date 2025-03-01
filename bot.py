import os
import requests
import schedule
import time
import hashlib
import sqlite3
import csv
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import openai
from dotenv import load_dotenv
import re

##############################################
# 1. Инициализация
##############################################
load_dotenv()

SCOPUS_API_KEY = os.getenv("SCOPUS_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TELEGRAM_THREAD_ID = os.getenv("TELEGRAM_THREAD_ID")

if not SCOPUS_API_KEY or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID or not OPENAI_API_KEY:
    raise ValueError("Не все ключи найдены в .env! Проверьте SCOPUS_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID, OPENAI_API_KEY.")

openai.api_key = OPENAI_API_KEY

bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# Подключение к базе данных
conn = sqlite3.connect("s_articles.db", check_same_thread=False)
cursor = conn.cursor()

# Создаём таблицу, если не существует
cursor.execute("""
CREATE TABLE IF NOT EXISTS published_articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hash TEXT UNIQUE,
    doi TEXT,
    eng_title TEXT,
    rus_title TEXT,
    first_author TEXT,
    pub_date TEXT
)
""")
conn.commit()

##############################################
# 2. Удаление HTML-тегов (например <inf>)
##############################################
def remove_html_tags(text: str) -> str:
    return re.sub(r"<[^>]*>", "", text or "")

##############################################
# 3. Поиск статей в Scopus
##############################################
def search_scopus():
    """
    Ищем новые статьи в Scopus по двум ключевым словам:
    hydrogen и ammonia. Берём последние 10 публикаций.
    """
    print("🔎 Проверка новых публикаций в Scopus...")

    keywords = ["hydrogen", "ammonia"]
    query_parts = [f"TITLE({kw})" for kw in keywords]
    query_str = " OR ".join(query_parts)

    url = f"https://api.elsevier.com/content/search/scopus?query={query_str}&sort=-coverDate&count=10"
    headers = {
        "X-ELS-APIKey": SCOPUS_API_KEY,
        "Accept": "application/json"
    }

    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print("❌ Ошибка Scopus API:", resp.status_code, resp.text)
        return

    data = resp.json()
    entries = data.get("search-results", {}).get("entry", [])
    if not entries:
        print("Нет статей для отображения.")
        return

    for entry in entries:
        # Извлекаем нужные данные
        doi = entry.get("prism:doi", "No DOI")
        eng_title = entry.get("dc:title", "No Title")
        pub_date = entry.get("prism:coverDate", "No Date")
        first_author = extract_first_author(entry)

        # Удаляем HTML-теги из английского заголовка
        eng_title_clean = remove_html_tags(eng_title)

        # Генерируем хэш (уникальный идентификатор)
        unique_str = f"{doi}-{eng_title_clean}-{pub_date}-{first_author}"
        rec_hash = hashlib.md5(unique_str.encode("utf-8")).hexdigest()

        # Проверяем, нет ли уже записи в БД
        cursor.execute("SELECT * FROM published_articles WHERE hash=?", (rec_hash,))
        if cursor.fetchone():
            # Уже публиковали
            continue

        print(f"Найдена новая статья: {eng_title_clean} (DOI: {doi})")

        # Переводим заголовок на русский язык через OpenAI
        rus_title = translate_title_openai(eng_title_clean)

        # Сохраняем в БД
        cursor.execute("""
            INSERT INTO published_articles (hash, doi, eng_title, rus_title, first_author, pub_date)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (rec_hash, doi, eng_title_clean, rus_title, first_author, pub_date))
        conn.commit()

        # Публикуем в Telegram
        send_to_telegram(rus_title, first_author, pub_date, doi)

def extract_first_author(entry):
    """
    Извлекаем имя первого автора из поля "dc:creator".
    Может быть 'Smith, John; Williams, Kate'.
    """
    creators = entry.get("dc:creator", "No Author")
    return creators.split(";")[0].strip()

##############################################
# 4. Перевод заголовка через OpenAI
##############################################
def translate_title_openai(eng_title: str) -> str:
    """
    Переводим заголовок статьи на русский язык через GPT-4 (или gpt-3.5-turbo).
    """
    if not eng_title or eng_title == "No Title":
        return "Нет заголовка"

    try:
        # ВНИМАНИЕ: если у вас нет gpt-4, используйте "gpt-3.5-turbo"
        completion = openai.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": "Ты — профессиональный переводчик. Переведи заголовок статьи на русский язык, сохрани стиль и смысл."
                },
                {
                    "role": "user",
                    "content": eng_title
                }
            ],
            temperature=0
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        print("Ошибка при обращении к OpenAI:", e)
        # Если не вышло, вернём исходный заголовок
        return eng_title

##############################################
# 5. Публикация в Telegram
##############################################
def send_to_telegram(rus_title, first_author, pub_date, doi):
    """
    Публикуем сообщение:
    - название статьи (на русском),
    - первый автор,
    - дата,
    - DOI (в виде ссылки),
    - инлайн-кнопка (Читать оригинал).
    """
    if doi == "No DOI":
        doi_link_markdown = "Без DOI"
        doi_url = None
    else:
        doi_link_markdown = f"[{doi}](https://doi.org/{doi})"
        doi_url = f"https://doi.org/{doi}"

    message_text = (
        f"*{rus_title}*\n"
        f"Автор: {first_author}\n"
        f"Дата: {pub_date}\n"
        f"DOI: {doi_link_markdown}"
    )

    markup = InlineKeyboardMarkup()
    if doi_url:
        markup.add(InlineKeyboardButton("Читать оригинал", url=doi_url))

    try:
        bot.send_message(
            chat_id=TELEGRAM_CHANNEL_ID,
            text=message_text,
            parse_mode="Markdown",
            reply_markup=markup,
            message_thread_id=TELEGRAM_THREAD_ID
        )
        print(f"✅ Опубликовано: {rus_title}")
    except Exception as e:
        print("❌ Ошибка при отправке в Telegram:", e)

##############################################
# 6. Выгрузка базы в CSV и отправка в Telegram
##############################################
def export_db_to_csv():
    """
    Выгружаем всю таблицу published_articles в CSV-файл.
    Добавляем колонку с ссылкой на оригинал (если DOI есть).
    """
    filename = "scopus_pub.csv"
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        # Заголовки столбцов
        writer.writerow([
            "ID", "Hash", "DOI", "English Title", "Russian Title",
            "First Author", "Publication Date", "Original Link"
        ])

        cursor.execute("SELECT id, hash, doi, eng_title, rus_title, first_author, pub_date FROM published_articles")
        rows = cursor.fetchall()
        for row in rows:
            id_, hash_, doi, eng_title, rus_title, first_author, pub_date = row
            if doi != "No DOI":
                original_link = f"https://doi.org/{doi}"
            else:
                original_link = "No link"

            # Записываем всё в строку CSV, включая ссылку
            writer.writerow([
                id_, hash_, doi, eng_title, rus_title,
                first_author, pub_date, original_link
            ])
    return filename

def send_csv_to_telegram():
    """
    Экспортируем БД в CSV и отправляем файл в Telegram-группу.
    """
    filename = export_db_to_csv()
    try:
        with open(filename, "rb") as f:
            bot.send_document(TELEGRAM_CHANNEL_ID, f, caption="Свод публикаций Scopus (CSV)", message_thread_id=TELEGRAM_THREAD_ID)
        print("✅ CSV-файл отправлен в Telegram!")
    except Exception as e:
        print("❌ Ошибка при отправке CSV-файла:", e)

##############################################
# 7. Планировщик
##############################################
# Поиск новых статей каждые 1 минуту (для теста)
schedule.every(1).minutes.do(search_scopus)

# Выгрузка БД (CSV) и отправка каждую субботу в 13:42
schedule.every().saturday.at("14:38").do(send_csv_to_telegram)

if __name__ == "__main__":
    print("🤖 Бот запущен. Ожидание нового контента...")
    # Первый запуск сразу
    search_scopus()

    while True:
        schedule.run_pending()
        time.sleep(10)
