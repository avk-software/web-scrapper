import json
import logging
import os
import smtplib
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TourOperatorScraper:
    def __init__(self):
        self.proxy_config = self._get_proxy_config()
        self.email_config = self._get_email_config()
        self.api_endpoint = os.environ.get('API_ENDPOINT', '')
        self.results = []
        self.errors = []
        
    # def _get_proxy_config(self) -> Dict:
    #    """Получение настроек прокси из переменных окружения"""
    #    return {
    #        'http': f"http://{os.environ.get('PROXY_USER')}:{os.environ.get('PROXY_PASS')}@{os.environ.get('PROXY_HOST')}:{os.environ.get('PROXY_PORT')}",
    #        'https': f"http://{os.environ.get('PROXY_USER')}:{os.environ.get('PROXY_PASS')}@{os.environ.get('PROXY_HOST')}:{os.environ.get('PROXY_PORT')}"
    #    }
    
    def _get_email_config(self) -> Dict:
        """Получение настроек email из переменных окружения"""
        return {
            'smtp_server': 'smtp-mail.outlook.com',
            'smtp_port': 587,
            'sender_email': os.environ.get('SENDER_EMAIL'),
            'sender_password': os.environ.get('SENDER_PASSWORD'),
            'recipient_email': os.environ.get('RECIPIENT_EMAIL')
        }
    
    def _make_request(self, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
        """Выполнение HTTP-запроса с повторными попытками"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    # proxies=self.proxy_config,
                    timeout=30,
                    verify=True
                )
                response.raise_for_status()
                response.encoding = response.apparent_encoding
                return BeautifulSoup(response.text, 'html.parser')
                
            except Exception as e:
                error_msg = f"Попытка {attempt + 1}/{max_retries} для {url}: {str(e)}"
                logger.warning(error_msg)
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Экспоненциальная задержка
                else:
                    self.errors.append(f"Не удалось получить данные с {url} после {max_retries} попыток: {str(e)}")
                    return None
        
        return None
    
    def scrape_rate_table_sites(self, url: str) -> List[Dict]:
        """Скреппинг сайтов с таблицей курсов (основной метод из оригинального кода)"""
        soup = self._make_request(url)
        if not soup:
            return []
        
        results = []
        
        # Определение туроператоров для поиска
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
        
        # Поиск таблицы с курсами
        rate_table = soup.find('table', class_='mod_rate_today')
        if not rate_table:
            return results
        
        # Группировка операторов по названию
        operator_groups = {}
        for operator in operators:
            name = operator['touroperator']
            if name not in operator_groups:
                operator_groups[name] = []
            operator_groups[name].append(operator)
        
        # Извлечение данных для каждого оператора
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
        """Поиск данных оператора в таблице курсов"""
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
            operator_text = div_element.get_text().strip().split('\n')[0].strip()
            
            # Проверка совпадения названий
            if (operator_text == operator_name or 
                operator_name in operator_text or 
                operator_text in operator_name):
                
                cells = row.find_all('td')
                if len(cells) >= 7:
                    return {
                        'eur': {
                            'rate': cells[1].get_text().strip(),
                            'percentage': cells[2].get_text().strip(),
                            'delta': cells[3].get_text().strip()
                        },
                        'usd': {
                            'rate': cells[4].get_text().strip(),
                            'percentage': cells[5].get_text().strip(),
                            'delta': cells[6].get_text().strip()
                        }
                    }
        
        return None
    
    def scrape_paks_site(self, url: str) -> List[Dict]:
        """Скреппинг сайта ПАКС"""
        soup = self._make_request(url)
        if not soup:
            return []
        
        # Поиск элементов с курсами валют
        currency_block = soup.find('div', class_='page-header__currency')
        if not currency_block:
            return []
        
        try:
            eur_rate = currency_block.select('ul li:nth-child(2) span.page-header__currency-value')[0].get_text().strip()
            usd_rate = currency_block.select('ul li:nth-child(1) span.page-header__currency-value')[0].get_text().strip()
            
            return [
                {
                    'id': 3727,
                    'sectionId': 563,
                    'name': 'EUR',
                    'touroperator': 'ПАКС',
                    'rate': eur_rate,
                    'percentToCB': '',
                    'delta': ''
                },
                {
                    'id': 3729,
                    'sectionId': 563,
                    'name': 'USD',
                    'touroperator': 'ПАКС',
                    'rate': usd_rate,
                    'percentToCB': '',
                    'delta': ''
                }
            ]
        except (IndexError, AttributeError) as e:
            self.errors.append(f"Ошибка извлечения данных с сайта ПАКС: {str(e)}")
            return []
    
    def scrape_pak_site(self, url: str) -> List[Dict]:
        """Скреппинг сайта ПАК"""
        soup = self._make_request(url)
        if not soup:
            return []
        
        exchange_block = soup.find('div', class_='mb-10 exchange-rates-block-items')
        if not exchange_block:
            return []
        
        try:
            eur_rate = exchange_block.select('div:nth-child(2) div div.exchange-rates__currencies div:nth-child(1) span:nth-child(1)')[0].get_text().strip()
            usd_rate = exchange_block.select('div:nth-child(1) div div.exchange-rates__currencies div:nth-child(1) span:nth-child(1)')[0].get_text().strip()
            
            return [
                {
                    'id': 3873,
                    'sectionId': 565,
                    'name': 'EUR',
                    'touroperator': 'ПАК',
                    'rate': eur_rate,
                    'percentToCB': '',
                    'delta': ''
                },
                {
                    'id': 3875,
                    'sectionId': 565,
                    'name': 'USD',
                    'touroperator': 'ПАК',
                    'rate': usd_rate,
                    'percentToCB': '',
                    'delta': ''
                }
            ]
        except (IndexError, AttributeError) as e:
            self.errors.append(f"Ошибка извлечения данных с сайта ПАК: {str(e)}")
            return []

    def scrape_icstrvl_site(self, url: str) -> List[Dict]:
        """Скреппинг сайта ICS Travel"""
        soup = self._make_request(url)
        if not soup:
            return []
        
        exchange_block = soup.find('td', class_='arriveCity')
        if not exchange_block:
            return []
        
        try:
            eur_rate = exchange_block.select('table tbody tr td:nth-child(2) div b:nth-child(3)')[0].get_text().strip()
            usd_rate = exchange_block.select('table tbody tr td:nth-child(2) div b:nth-child(2)')[0].get_text().strip()
            
            return [
                {
                    'id': 3991,
                    'sectionId': 569,
                    'name': 'EUR',
                    'touroperator': 'ICS',
                    'rate': eur_rate,
                    'percentToCB': '',
                    'delta': ''
                },
                {
                    'id': 3993,
                    'sectionId': 569,
                    'name': 'USD',
                    'touroperator': 'ICS',
                    'rate': usd_rate,
                    'percentToCB': '',
                    'delta': ''
                }
            ]
        except (IndexError, AttributeError) as e:
            self.errors.append(f"Ошибка извлечения данных с сайта ISC Travel: {str(e)}")
            return []

    def scrape_arttour_site(self, url: str) -> List[Dict]:
        """Скреппинг сайта Арт Тур"""
        soup = self._make_request(url)
        if not soup:
            return []
        
        exchange_block = soup.find('#valuta-sl')
        if not exchange_block:
            return []
        
        try:
            eur_rate = exchange_block.select('#cur_rates_eur')[0].get_text().strip()
            usd_rate = exchange_block.select('#cur_rates_usd')[0].get_text().strip()
            
            return [
                {
                    id: 3995,
                    sectionId: 571,
                    name: 'EUR',
                    touroperator: 'Арт Тур',
                    rate: eur_rate,
                    'percentToCB': '',
                    'delta': ''
                },
                {
                    id: 3997,
                    sectionId: 571,
                    name: 'USD',
                    touroperator: 'Арт Тур',
                    'rate': usd_rate,
                    'percentToCB': '',
                    'delta': ''
                }
            ]
        except (IndexError, AttributeError) as e:
            self.errors.append(f"Ошибка извлечения данных с сайта Арт Тур: {str(e)}")
            return []
    
    def scrape_all_sites(self) -> Dict:
        """Скреппинг всех сайтов"""
        # Список всех сайтов для обработки
        sites_config = [
            {'url': 'https://tour-kassa.ru/%D0%BA%D1%83%D1%80%D1%81%D1%8B-%D0%B2%D0%B0%D0%BB%D1%8E%D1%82-%D1%82%D1%83%D1%80%D0%BE%D0%BF%D0%B5%D1%80%D0%B0%D1%82%D0%BE%D1%80%D0%BE%D0%B2', 'method': 'scrape_rate_table_sites'},
            {'url': 'https://paks-site.com', 'method': 'scrape_paks_site'},
            {'url': 'https://pak-site.com', 'method': 'scrape_pak_site'},
            {'url': 'https://www.icstrvl.ru/index.html', 'method': 'scrape_icstrvl_site'},
            {'url': 'https://www.arttour.ru/', 'method': 'scrape_arttour_site'},
            {'url': 'https://www.space-travel.ru/', 'method': 'scrape_space-travel_site'},
            {'url': 'https://vand.ru/', 'method': 'scrape_vand_site'},
            {'url': 'https://www.amigo-tours.ru/', 'method': 'scrape_amigo-tours_site'},
            {'url': 'https://www.quinta.ru/', 'method': 'scrape_quinta_site'},
            {'url': 'https://www.bsigroup.ru/', 'method': 'scrape_bsigroup_site'},
            {'url': 'https://www.tourtrans.ru/', 'method': 'scrape_tourtrans_site'},
            {'url': 'https://spectrum.ru/turagentam/', 'method': 'scrape_spectrum_site'},
            {'url': 'https://www.cruclub.ru/agent/howto/book/#pay', 'method': 'scrape_cruclub_site'},
            {'url': 'https://www.panteon.ru/', 'method': 'scrape_panteon_site'},
            {'url': 'https://www.loti.ru/Currency', 'method': 'scrape_loti_site'},
            {'url': 'https://grand-travels.ru/', 'method': 'scrape_grand-travels_site'},
            {'url': 'https://intourist.ru/', 'method': 'scrape_intourist_site'},
            {'url': 'https://www.tez-tour.com/', 'method': 'scrape_tez-tour_site'},
            {'url': 'https://www.jettravel.ru/', 'method': 'scrape_jettravel_site'},
            {'url': 'https://webcache.googleusercontent.com/search?q=cache:https://www.ambotis.ru/turagentstvam/informatsiya/kurs-valyut/', 'method': 'scrape_google_site'},
            {'url': 'https://clickvoyage.ru/', 'method': 'scrape_clickvoyage_site'},
        ]
        
        all_results = []
        
        for site_config in sites_config:
            logger.info(f"Обработка сайта: {site_config['url']}")
            
            try:
                method = getattr(self, site_config['method'])
                site_results = method(site_config['url'])
                all_results.extend(site_results)
                
                logger.info(f"Получено {len(site_results)} записей с {site_config['url']}")
                
            except Exception as e:
                error_msg = f"Ошибка при обработке {site_config['url']}: {str(e)}"
                logger.error(error_msg)
                self.errors.append(error_msg)
            
            # Пауза между запросами
            time.sleep(1)
        
        return {
            'data': all_results,
            'timestamp': datetime.now().isoformat(),
            'total_records': len(all_results),
            'errors': self.errors
        }
    
    def send_results_to_api(self, results: Dict) -> bool:
        """Отправка результатов в API"""
        try:
            response = requests.post(
                self.api_endpoint,
                json=results,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            response.raise_for_status()
            logger.info("Результаты успешно отправлены в API")
            return True
            
        except Exception as e:
            error_msg = f"Ошибка отправки результатов в API: {str(e)}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            return False
    
    def send_email_log(self, results: Dict) -> None:
        """Отправка email с логами"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config['sender_email']
            msg['To'] = self.email_config['recipient_email']
            msg['Subject'] = f"Отчет о скреппинге курсов валют - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            
            # Формирование тела письма
            body = f"""
Отчет о выполнении скреппинга курсов валют

Время выполнения: {results['timestamp']}
Всего записей получено: {results['total_records']}
Количество ошибок: {len(self.errors)}

"""
            
            if self.errors:
                body += "ОШИБКИ:\n" + "\n".join(f"- {error}" for error in self.errors)
            else:
                body += "Ошибок не обнаружено. Все данные получены успешно."
            
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
            
            # Отправка email
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['sender_email'], self.email_config['sender_password'])
            text = msg.as_string()
            server.sendmail(self.email_config['sender_email'], self.email_config['recipient_email'], text)
            server.quit()
            
            logger.info("Email отчет отправлен успешно")
            
        except Exception as e:
            logger.error(f"Ошибка отправки email: {str(e)}")

def handler(event, context):
    """Основная функция-обработчик для Yandex Cloud Functions"""
    scraper = TourOperatorScraper()
    
    try:
        # Выполнение скреппинга
        results = scraper.scrape_all_sites()
        
        # Отправка результатов в API
        api_success = scraper.send_results_to_api(results)
        
        # Отправка email отчета
        scraper.send_email_log(results)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Скреппинг выполнен успешно',
                'total_records': results['total_records'],
                'errors_count': len(scraper.errors),
                'api_sent': api_success
            }, ensure_ascii=False)
        }
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Критическая ошибка при выполнении скреппинга',
                'error': str(e)
            }, ensure_ascii=False)
        }
