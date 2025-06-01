import json
import logging
import random
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import os
import requests
from bs4 import BeautifulSoup
import re
from typing import Dict, List, Optional, Tuple

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CurrencyScraperError(Exception):
    """Кастомное исключение для ошибок скреппинга"""
    pass

class CurrencyScraper:
    """Класс для скреппинга курсов валют туроператоров"""
    
    def __init__(self):
        self.session = requests.Session()
        self.results = []
        self.errors = []
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
        ]
        
    def get_random_headers(self) -> Dict[str, str]:
        """Генерирует случайные заголовки для запроса"""
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    def make_request(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        """Выполняет HTTP запрос с повторными попытками"""
        for attempt in range(retries):
            try:
                headers = self.get_random_headers()
                response = self.session.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                
                # Случайная задержка между запросами
                time.sleep(random.uniform(1, 3))
                
                return BeautifulSoup(response.content, 'html.parser')
                
            except Exception as e:
                logger.warning(f"Попытка {attempt + 1} для {url} неудачна: {str(e)}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(2, 5))
                else:
                    self.errors.append(f"Не удалось получить данные с {url}: {str(e)}")
                    return None
    
    def extract_rate(self, text: str) -> Optional[str]:
        """Извлекает курс валюты из текста"""
        if not text or text.strip() == "-":
            return None
        
        # Различные паттерны для поиска курса
        patterns = [
            r'(\d+[.,]\d+)',  # 88.60 или 88,60
            r'(\d+\.\d+)',    # 88.60
            r'(\d+,\d+)',     # 88,60
            r'(\d+)'          # 88
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text.replace(' ', ''))
            if match:
                return match.group(1).replace(',', '.')
        
        return None
    
    def scrape_tour_kassa_site(self, url: str) -> List[Dict]:
        """Скреппинг сайта с основной таблицей курсов"""
        soup = self.make_request(url)
        if not soup:
            return []
        
        results = []
        
        # Конфигурация туроператоров
        operators = [
            {'id': 3153, 'sectionId': 539, 'name': 'EUR', 'touroperator': 'ЦБ РФ'},
            {'id': 3167, 'sectionId': 539, 'name': 'USD', 'touroperator': 'ЦБ РФ'},
            {'id': 3141, 'sectionId': 527, 'name': 'EUR', 'touroperator': 'Корал Трэвел'},
            {'id': 3155, 'sectionId': 527, 'name': 'USD', 'touroperator': 'Корал Трэвел'},
            {'id': 3147, 'sectionId': 531, 'name': 'EUR', 'touroperator': 'Санмар'},
            {'id': 3161, 'sectionId': 531, 'name': 'USD', 'touroperator': 'Санмар'},
            {'id': 3151, 'sectionId': 535, 'name': 'EUR', 'touroperator': 'Фан & Сан'},
            {'id': 3165, 'sectionId': 535, 'name': 'USD', 'touroperator': 'Фан & Сан'},
            {'id': 3129, 'sectionId': 521, 'name': 'EUR', 'touroperator': 'Анекс Тур'},
            {'id': 3131, 'sectionId': 521, 'name': 'USD', 'touroperator': 'Анекс Тур'},
            {'id': 3143, 'sectionId': 529, 'name': 'EUR', 'touroperator': 'Пегас Туристик'},
            {'id': 3157, 'sectionId': 529, 'name': 'USD', 'touroperator': 'Пегас Туристик'},
            {'id': 3145, 'sectionId': 537, 'name': 'EUR', 'touroperator': 'Русский Экспресс'},
            {'id': 3159, 'sectionId': 537, 'name': 'USD', 'touroperator': 'Русский Экспресс'},
            {'id': 3133, 'sectionId': 523, 'name': 'EUR', 'touroperator': 'Библио Глобус'},
            {'id': 3135, 'sectionId': 523, 'name': 'USD', 'touroperator': 'Библио Глобус'}
        ]
        
        # Поиск таблицы
        table = soup.find('table', class_='mod_rate_today')
        if not table:
            raise CurrencyScraperError("Таблица mod_rate_today не найдена")
        
        # Группировка операторов
        operator_groups = {}
        for op in operators:
            if op['touroperator'] not in operator_groups:
                operator_groups[op['touroperator']] = []
            operator_groups[op['touroperator']].append(op)
        
        # Обработка каждого оператора
        for operator_name, operator_items in operator_groups.items():
            operator_data = self._get_exchange_rates_by_operator(soup, operator_name)
            
            if operator_data:
                for item in operator_items:
                    currency_data = operator_data['eur'] if item['name'] == 'EUR' else operator_data['usd']
                    
                    results.append({
                        'id': item['id'],
                        'sectionId': item['sectionId'],
                        'name': item['name'],
                        'touroperator': item['touroperator'],
                        'rate': currency_data['rate'],
                        'percentToCB': currency_data['percentage'],
                        'delta': currency_data['delta']
                    })
        
        return results
    
    def _get_exchange_rates_by_operator(self, soup: BeautifulSoup, operator_name: str) -> Optional[Dict]:
        """Поиск курсов валют по названию оператора"""
        rows = soup.find_all('tr')
        
        for row in rows:
            operator_cell = row.find('td', class_='mod_rate_oper')
            if not operator_cell:
                continue
            
            # Извлечение названия оператора
            div_element = operator_cell.find('div')
            if not div_element:
                continue
            
            # Получение текста оператора
            operator_text = div_element.get_text(strip=True).split('\n')[0].strip()
            
            # Проверка совпадения
            if (operator_text == operator_name or 
                operator_name in operator_text or 
                operator_text in operator_name):
                
                cells = row.find_all('td')
                if len(cells) >= 7:
                    return {
                        'eur': {
                            'rate': self.extract_rate(cells[1].get_text(strip=True)),
                            'percentage': cells[2].get_text(strip=True),
                            'delta': cells[3].get_text(strip=True)
                        },
                        'usd': {
                            'rate': self.extract_rate(cells[4].get_text(strip=True)),
                            'percentage': cells[5].get_text(strip=True),
                            'delta': cells[6].get_text(strip=True)
                        }
                    }
        
        return None
    
    def scrape_paks_site(self, url: str) -> List[Dict]:
        """Скреппинг сайта ПАКС"""
        soup = self.make_request(url)
        if not soup:
            return []
        
        currency_div = soup.find('div', class_='page-header__currency')
        if not currency_div or not hasattr(currency_div, 'select_one'):
            raise CurrencyScraperError("Элемент page-header__currency не найден или не является тегом")
        
        eur_element = soup.select_one('div.page-header__currency ul li:nth-child(2) span.page-header__currency-value')
        usd_element = soup.select_one('div.page-header__currency ul li:nth-child(1) span.page-header__currency-value')
        
        return [
            {
                'id': 3727,
                'sectionId': 563,
                'name': 'EUR',
                'touroperator': 'ПАКС',
                'rate': self.extract_rate(eur_element.get_text() if eur_element else ''),
                'percentToCB': '',
                'delta': ''
            },
            {
                'id': 3729,
                'sectionId': 563,
                'name': 'USD',
                'touroperator': 'ПАКС',
                'rate': self.extract_rate(usd_element.get_text() if usd_element else ''),
                'percentToCB': '',
                'delta': ''
            }
        ]
    
    def scrape_pak_site(self, url: str) -> List[Dict]:
        """Скреппинг сайта ПАК"""
        soup = self.make_request(url)
        if not soup:
            return []
        
        exchange_div = soup.find('div', class_='mb-10 exchange-rates-block-items')
        if not exchange_div:
            raise CurrencyScraperError("Элемент exchange-rates-block-items не найден")
        
        # Используем soup.select_one с полным CSS-селектором относительно документа
        eur_element = soup.select_one('div.mb-10.exchange-rates-block-items div:nth-child(2) div div.exchange-rates__currencies div:nth-child(1) span:nth-child(1)')
        usd_element = soup.select_one('div.mb-10.exchange-rates-block-items div:nth-child(1) div div.exchange-rates__currencies div:nth-child(1) span:nth-child(1)')
        
        return [
            {
                'id': 3873,
                'sectionId': 565,
                'name': 'EUR',
                'touroperator': 'ПАК',
                'rate': self.extract_rate(eur_element.get_text() if eur_element else ''),
                'percentToCB': '',
                'delta': ''
            },
            {
                'id': 3875,
                'sectionId': 565,
                'name': 'USD',
                'touroperator': 'ПАК',
                'rate': self.extract_rate(usd_element.get_text() if usd_element else ''),
                'percentToCB': '',
                'delta': ''
            }
        ]
    
    def scrape_icstrvl_site(self, url: str) -> List[Dict]:
        """Скреппинг сайта ICS"""
        soup = self.make_request(url)
        if not soup:
            return []
        
        exchange_block = soup.find('td', class_='arriveCity')
        if not exchange_block:
            return []
        
        # Найти ближайший родительский тег table
        parent_table = exchange_block.find_parent('table')
        if not parent_table:
            return []
        
        eur_rate = parent_table.select('tbody tr td:nth-child(2) div b:nth-child(3)')
        usd_rate = parent_table.select('tbody tr td:nth-child(2) div b:nth-child(2)')
        
        eur_rate_text = eur_rate[0].get_text().strip() if eur_rate else ''
        usd_rate_text = usd_rate[0].get_text().strip() if usd_rate else ''
            
        return [
            {
                'id': 3991,
                'sectionId': 569,
                'name': 'EUR',
                'touroperator': 'ICS',
                'rate': eur_rate_text,
                'percentToCB': '',
                'delta': ''
            },
            {
                'id': 3993,
                'sectionId': 569,
                'name': 'USD',
                'touroperator': 'ICS',
                'rate': usd_rate_text,
                'percentToCB': '',
                'delta': ''
            }
        ]
    
    def scrape_arttour_site(self, url: str) -> List[Dict]:
        """Скреппинг сайта Арт Тур"""
        soup = self.make_request(url)
        if not soup:
            return []
        
        exchange_block = soup.select_one('#valuta-sl')
        if not exchange_block:
            return []
        
        eur_element = soup.select_one('#cur_rates_eur')
        usd_element = soup.select_one('#cur_rates_usd')
        eur_rate = eur_element.get_text().strip() if eur_element else ''
        usd_rate = usd_element.get_text().strip() if usd_element else ''
            
        return [
            {
                'id': 3995,
                'sectionId': 571,
                'name': 'EUR',
                'touroperator': 'Арт Тур',
                'rate': eur_rate,
                'percentToCB': '',
                'delta': ''
            },
            {
                'id': 3997,
                'sectionId': 571,
                'name': 'USD',
                'touroperator': 'Арт Тур',
                'rate': usd_rate,
                'percentToCB': '',
                'delta': ''
            }
        ]
    
    def scrape_all_sites(self) -> Dict:
        """Скреппинг всех сайтов"""
        sites_config = [
            # Полный список сайтов с их конфигурацией
            {
                'name': 'Tour_Kassa',
                'url': 'https://tour-kassa.ru/%D0%BA%D1%83%D1%80%D1%81%D1%8B-%D0%B2%D0%B0%D0%BB%D1%8E%D1%82-%D1%82%D1%83%D1%80%D0%BE%D0%BF%D0%B5%D1%80%D0%B0%D1%82%D0%BE%D1%80%D0%BE%D0%B2',
                'scraper': self.scrape_tour_kassa_site
            },
            {
                'name': 'ПАКС',
                'url': 'https://paks.ru/',  
                'scraper': self.scrape_paks_site
            },
            {
                'name': 'ПАК', 
                'url': 'https://www.pac.ru/',  
                'scraper': self.scrape_pak_site
            },
            {
                'name': 'АртТур', 
                'url': 'https://www.arttour.ru/',  
                'scraper': self.scrape_arttour_site
            },
            {
                'name': 'ICS', 
                'url': 'https://www.icstrvl.ru/index.html',  
                'scraper': self.scrape_icstrvl_site
            }
        ]
        
        total_sites = len(sites_config)
        successful_sites = 0
        
        for site_config in sites_config:
            try:
                logger.info(f"Обрабатываю сайт: {site_config['name']}")
                site_results = site_config['scraper'](site_config['url'])
                self.results.extend(site_results)
                successful_sites += 1
                logger.info(f"Успешно обработан сайт {site_config['name']}: {len(site_results)} записей")
                
            except Exception as e:
                error_msg = f"Ошибка при обработке сайта {site_config['name']}: {str(e)}"
                logger.error(error_msg)
                self.errors.append(error_msg)
        
        return {
            'data': self.results,
            'summary': {
                'total_sites': total_sites,
                'successful_sites': successful_sites,
                'failed_sites': total_sites - successful_sites,
                'total_records': len(self.results),
                'errors': self.errors
            }
        }

class EmailNotifier:
    """Класс для отправки email уведомлений"""
    
    def __init__(self, smtp_server: str, smtp_port: int, email: str, password: str):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.email = email
        self.password = password

    def send_notification(self, subject: str, body: str, to_email: str):
        """Отправка email уведомления"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email
            msg['To'] = to_email
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'html'))
            
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.email, self.password)
            text = msg.as_string()
            server.sendmail(self.email, to_email, text)
            server.quit()
            
            logger.info(f"Email отправлен на {to_email}")
            
        except Exception as e:
            logger.error(f"Ошибка отправки email: {str(e)}")

def send_results_to_api(data: Dict, api_url: str) -> bool:
    """Отправка результатов в API"""
    try:
        # Получение данных из запроса
        # body = json.loads(event.get('body', '{}'))
        body = data
        
        # Логирование полученных данных
        logging.info(f"Получено {len(body.get('data', []))} записей")
        logging.info(f"Статистика: {body.get('summary', {})}")

        # response = requests.post(api_url, json=data, timeout=30)
        # response.raise_for_status()
        # logger.info("Данные успешно отправлены в API")
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки в API: {str(e)}")
        return False

def handler(event, context):
    """Основная функция-обработчик для Yandex Cloud Functions"""
    
    # Получение переменных окружения
    outlook_email = os.getenv('OUTLOOK_EMAIL')
    outlook_password = os.getenv('OUTLOOK_PASSWORD')
    target_email = os.getenv('TARGET_EMAIL', 'andrey.koldayev@r-express.ru')
    api_url = os.getenv('API_URL')
    
    if not all([outlook_email, outlook_password, api_url]):
        error_msg = "Не все необходимые переменные окружения установлены"
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_msg})
        }
    
    # Явно указываем, что api_url теперь точно str, а не None
    api_url_str: str = str(api_url)
    
    start_time = datetime.now()
    
    try:
        # Инициализация скреппера
        scraper = CurrencyScraper()
        
        # Выполнение скреппинга
        logger.info("Начинаю скреппинг курсов валют")
        results = scraper.scrape_all_sites()
        
        # Отправка результатов в API
        api_success = send_results_to_api(results, api_url_str)
        
        # Подготовка отчета
        summary = results['summary']
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # Инициализация email notifier
        if outlook_email is None or outlook_password is None:
            raise ValueError("outlook_email and outlook_password environment variables must be set and not None")
        # Явно приводим тип outlook_email к str, так как выше уже проверили на None
        notifier = EmailNotifier(
            smtp_server='smtp-mail.outlook.com',
            smtp_port=587,
            email=str(outlook_email),
            password=str(outlook_password) if outlook_password is not None else ""
        )
        
        # Отправка уведомления при ошибках или всегда (в зависимости от настроек)
        if summary['failed_sites'] > 0 or summary['errors'].__len__() > 0 or not api_success:
            subject = f"⚠️ Ошибки при скреппинге курсов валют - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            
            body = f"""
            <html>
            <body>
            <h2>Отчет о скреппинге курсов валют</h2>
            <p><strong>Время выполнения:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>Длительность:</strong> {execution_time:.2f} секунд</p>
            
            <h3>Статистика:</h3>
            <ul>
                <li>Всего сайтов: {summary['total_sites']}</li>
                <li>Успешно обработано: {summary['successful_sites']}</li>
                <li>С ошибками: {summary['failed_sites']}</li>
                <li>Всего записей: {summary['total_records']}</li>
                <li>Отправка в API: {'✅ Успешно' if api_success else '❌ Ошибка'}</li>
            </ul>
            
            {f"<h3>Ошибки:</h3><ul>{''.join([f'<li>{error}</li>' for error in summary['errors']])}</ul>" if summary['errors'] else ""}
            </body>
            </html>
            """
            
            notifier.send_notification(subject, body, target_email)
        
        # Возврат результата
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Скреппинг завершен',
                'summary': summary,
                'execution_time': execution_time,
                'api_sent': api_success
            }, ensure_ascii=False)
        }
        
    except Exception as e:
        error_msg = f"Критическая ошибка: {str(e)}"
        logger.error(error_msg)
        
        # Отправка уведомления о критической ошибке
        try:
            notifier = EmailNotifier(
                smtp_server='smtp-mail.outlook.com',
                smtp_port=587,
                email=str(outlook_email),
                password=str(outlook_password) if outlook_password is not None else ""
            )
            
            subject = f"🚨 Критическая ошибка скреппинга - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            body = f"""<html><body><h2>Критическая ошибка</h2><p>{error_msg}</p><p>Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p></body></html>"""
            
            notifier.send_notification(subject, body, target_email)
        except:
            pass  # Если не удается отправить email, просто логируем
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_msg})
        }
    
if __name__ == "__main__":
    # Тестовый вызов функции для локального запуска
    handler({}, {})