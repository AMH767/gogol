from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import io
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
from time import sleep
import os
from datetime import datetime
import threading
import uuid
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from neighborhoods import get_subregions

app = Flask(__name__)

# Глобальное хранилище задач
tasks = {}

# Настройка базы данных PostgreSQL (Supabase)
DATABASE_URL = os.environ.get('DATABASE_URL')

def get_db_connection():
    if not DATABASE_URL:
        return None
    
    url = DATABASE_URL
    # Очистка строки подключения от pgbouncer, если он есть
    if 'pgbouncer=true' in url:
        url = url.replace('pgbouncer=true', '')
        url = url.replace('?&', '?').replace('&&', '&').strip('?&')
    
    # Принудительный SSL для Supabase
    if 'sslmode' not in url:
        if '?' in url:
            url += '&sslmode=require'
        else:
            url += '?sslmode=require'
    
    try:
        return psycopg2.connect(url)
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return None

def init_db():
    try:
        conn = get_db_connection()
        if not conn:
            print("⚠️ DATABASE_URL не задан или ошибка подключения. База данных не инициализирована.")
            return
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS results (
                id SERIAL PRIMARY KEY,
                task_id TEXT,
                name TEXT,
                address TEXT,
                phone TEXT,
                rating TEXT,
                website TEXT,
                url TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ База данных успешно инициализирована.")
    except Exception as e:
        print(f"❌ Ошибка при инициализации БД: {e}")

# Инициализируем БД при запуске
with app.app_context():
    init_db()

def save_to_db(task_id, data):
    try:
        conn = get_db_connection()
        if not conn: return
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO results (task_id, name, address, phone, rating, website, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (task_id, data.get('name', 'N/A'), data.get('address', 'N/A'), 
              data.get('phone', 'N/A'), data.get('rating', 'N/A'), 
              data.get('website', 'N/A'), data.get('url', 'N/A')))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ Ошибка сохранения в БД: {e}")

def log_message(task_id, msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    clean_msg = str(msg).strip()
    full_msg = f"[{timestamp}] {clean_msg}"
    print(full_msg)
    if task_id in tasks:
        tasks[task_id]['logs'].append(clean_msg)
        if len(tasks[task_id]['logs']) > 500:
            tasks[task_id]['logs'].pop(0)

class GoogleMapsParser:
    def __init__(self, task_id, query, many, lang='ru', region='RU', deep_search=True):
        self.task_id = task_id
        self.query = query
        self.many = many
        self.lang = lang
        self.region = region
        self.deep_search = deep_search
        self.max_workers = 5
        self.results_lock = threading.Lock()

    def create_driver(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1200,800")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-images")
        
        if os.path.exists("/usr/bin/chromium"):
            chrome_options.binary_location = "/usr/bin/chromium"
        elif os.path.exists("/usr/bin/chromium-browser"):
            chrome_options.binary_location = "/usr/bin/chromium-browser"
        
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        chrome_options.add_argument(f"user-agent={ua}")
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(30)
            stealth(driver,
                languages=[f"{self.lang}-{self.region}", self.lang, "en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
            )
            return driver
        except Exception as e:
            log_message(self.task_id, f"❌ Ошибка драйвера: {e}")
            return None

    def get_links_for_query(self, search_query, limit):
        if not search_query: return []
        driver = self.create_driver()
        if not driver: return []
        
        links = set()
        try:
            url = f"https://www.google.com/maps/search/{str(search_query).replace(' ', '+')}?hl={self.lang}&gl={self.region}"
            driver.get(url)
            
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//a[contains(@href, "/maps/place/")]'))
                )
            except:
                driver.quit()
                return []

            scrollable_div = None
            selectors = ['//div[@role="feed"]', '//div[contains(@aria-label, "Results for")]']
            for s in selectors:
                try:
                    scrollable_div = driver.find_element(By.XPATH, s)
                    if scrollable_div: break
                except: continue
            
            if not scrollable_div: 
                try:
                    scrollable_div = driver.find_element(By.TAG_NAME, "body")
                except: pass

            last_len = 0
            no_change = 0
            while len(links) < limit and no_change < 5:
                if scrollable_div:
                    driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
                sleep(2)
                found = driver.find_elements(By.XPATH, '//a[contains(@href, "/maps/place/")]')
                for f in found:
                    href = f.get_attribute('href')
                    if href: links.add(href)
                
                if len(links) == last_len: no_change += 1
                else: no_change = 0; last_len = len(links)
                
        except Exception as e:
            log_message(self.task_id, f"⚠️ Ошибка поиска '{search_query}': {e}")
        finally:
            driver.quit()
        return list(links)

    def parse_details(self, url):
        driver = self.create_driver()
        if not driver: return None
        
        try:
            driver.get(url)
            sleep(2)
            soup = BeautifulSoup(driver.page_source, 'lxml')
            data = {'name': 'N/A', 'address': 'N/A', 'phone': 'N/A', 'rating': 'N/A', 'website': 'N/A', 'url': url}
            
            title_el = soup.find('h1')
            if title_el: data['name'] = title_el.get_text(strip=True)
            
            rating_el = soup.select_one('span[aria-label*="star"], span[aria-label*="звезд"], span[aria-label*="оцен"]')
            if rating_el:
                rt = rating_el.get('aria-label', '')
                match = re.search(r'(\d[.,]\d)', rt)
                data['rating'] = match.group(1).replace(',', '.') if match else 'N/A'
            
            if data['rating'] == 'N/A':
                rating_val = soup.select_one('.MW4etd, .ce967p')
                if rating_val:
                    data['rating'] = rating_val.get_text(strip=True).replace(',', '.')
            
            items = soup.find_all(['button', 'a'], attrs={'data-item-id': True})
            for item in items:
                item_id = item.get('data-item-id', '')
                text = item.get_text(strip=True)
                clean_text = re.sub(r'^[^\w\s\(\)\+]+', '', text).strip()
                
                if 'address' in item_id:
                    data['address'] = clean_text
                elif 'phone' in item_id:
                    phone_match = re.search(r'tel:(\+\d+)', item_id)
                    data['phone'] = phone_match.group(1) if phone_match else clean_text
                elif 'authority' in item_id:
                    data['website'] = item.get('href', clean_text)
            
            if data['website'] == 'N/A':
                site_link = soup.select_one('a[aria-label*="website"], a[aria-label*="Сайт"]')
                if site_link: data['website'] = site_link.get('href', 'N/A')
                
            return data
        except Exception:
            return None
        finally:
            driver.quit()

    def run(self):
        try:
            all_links = set()
            log_message(self.task_id, f"📡 Запуск основного поиска: {self.query}")
            
            if not self.query:
                log_message(self.task_id, "❌ Ошибка: Пустой запрос")
                tasks[self.task_id]['status'] = 'error'
                return

            main_links = self.get_links_for_query(self.query, self.many)
            all_links.update(main_links)
            log_message(self.task_id, f"🔗 Найдено в основном поиске: {len(main_links)}")

            if self.deep_search and len(all_links) < self.many:
                subregions = get_subregions(self.query)
                if subregions:
                    log_message(self.task_id, f"🔍 Включен глубокий поиск. Найдено районов для '{self.query}': {len(subregions)}")
                    for sub in subregions:
                        if len(all_links) >= self.many: break
                        sub_query = f"{self.query} {sub}"
                        log_message(self.task_id, f"📍 Поиск в районе: {sub}")
                        sub_links = self.get_links_for_query(sub_query, self.many - len(all_links))
                        all_links.update(sub_links)
                        log_message(self.task_id, f"🔗 Всего ссылок собрано: {len(all_links)}")
                else:
                    log_message(self.task_id, "ℹ️ Районы для данного города не найдены в базе, продолжаем с основным списком.")

            links_to_parse = list(all_links)[:self.many]
            if not links_to_parse:
                log_message(self.task_id, "❌ Организации не найдены")
                tasks[self.task_id]['status'] = 'completed'
                return

            log_message(self.task_id, f"🚀 Начинаем детальный парсинг {len(links_to_parse)} организаций...")
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self.parse_details, url) for url in links_to_parse]
                for future in concurrent.futures.as_completed(futures):
                    res = future.result()
                    if res:
                        with self.results_lock:
                            tasks[self.task_id]['results'].append({"id": len(tasks[self.task_id]['results']) + 1, **res})
                            save_to_db(self.task_id, res)
                            log_message(self.task_id, f"✅ Спарсено:\n{res['name']}\n📍 {res['address']}\n📞 {res['phone']}\n🌐 {res['website']}\n---")

            tasks[self.task_id]['end_time'] = datetime.now()
            duration = tasks[self.task_id]['end_time'] - tasks[self.task_id]['start_time']
            minutes, seconds = divmod(duration.total_seconds(), 60)
            time_str = f"{int(minutes)}м {int(seconds)}с"
            log_message(self.task_id, f"✅ Сбор данных успешно завершен! Время выполнения: {time_str}")
            tasks[self.task_id]['status'] = 'completed'
        except Exception as e:
            tasks[self.task_id]['end_time'] = datetime.now()
            log_message(self.task_id, f"❌ Критическая ошибка: {e}")
            tasks[self.task_id]['status'] = 'error'

@app.route('/')
def index():
    return render_template('index_google.html')

@app.route('/parse', methods=['POST'])
def parse():
    data = request.json or {}
    task_id = str(uuid.uuid4())
    
    # Собираем запрос из организации и города (как в рабочей версии HF)
    org = data.get('org', '')
    city = data.get('city', '')
    query = f"{org} {city}".strip()
    many = int(data.get('many', 20))
    
    tasks[task_id] = {
        'status': 'running',
        'logs': [],
        'results': [],
        'start_time': datetime.now(),
        'query': query,
        'requested': many
    }
    
    parser = GoogleMapsParser(
        task_id, 
        query, 
        many,
        lang=data.get('lang', 'ru'),
        region=data.get('region', 'RU'),
        deep_search=data.get('deepSearch', True)
    )
    threading.Thread(target=parser.run).start()
    return jsonify({'task_id': task_id})

@app.route('/status/<task_id>')
def status(task_id):
    if task_id not in tasks:
        return jsonify({'error': 'Task not found'}), 404
    return jsonify({
        'status': tasks[task_id]['status'],
        'progress': len(tasks[task_id]['results']),
        'total': tasks[task_id]['requested'],
        'logs': tasks[task_id]['logs']
    })

@app.route('/history_view')
def history_view():
    return render_template('history.html')

@app.route('/history')
def history():
    conn = get_db_connection()
    if not conn: return "Database not connected", 500
    try:
        df = pd.read_sql_query("SELECT * FROM results ORDER BY timestamp DESC", conn)
        conn.close()
        if df.empty: return "История пуста", 404
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        return send_file(output, as_attachment=True, download_name="all_parsed_history.xlsx")
    except Exception as e:
        return f"Error: {e}", 500

@app.route('/download/<task_id>')
def download(task_id):
    if task_id not in tasks: return "Not found", 404
    df = pd.DataFrame(tasks[task_id]['results'])
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f"results_{task_id}.xlsx")

@app.route('/history_count')
def history_count():
    conn = get_db_connection()
    if not conn: return jsonify({'count': 0})
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM results")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return jsonify({'count': count})
    except:
        return jsonify({'count': 0})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
