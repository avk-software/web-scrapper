# Инструкция по развертыванию Web Scraper в Yandex Cloud Functions

## 1. Подготовка кода

### Структура проекта:
```
currency-scraper/
├── index.py          # Основной код функции
├── requirements.txt  # Зависимости Python
└── README.md        # Документация
```

## 2. Настройка Gmail для отправки уведомлений

### Включение двухфакторной аутентификации:
1. Перейдите в настройки аккаунта Google
2. Включите двухфакторную аутентификацию
3. Создайте пароль приложения:
   - Настройки → Безопасность → Пароли приложений
   - Выберите "Почта" и "Другое устройство"
   - Сохраните сгенерированный пароль

## 3. Создание функции в Yandex Cloud

### Через веб-консоль:

1. **Создание функции:**
   ```bash
   # Войдите в консоль Yandex Cloud
   # Перейдите в Cloud Functions
   # Нажмите "Создать функцию"
   ```

2. **Настройки функции:**
   - **Имя:** `currency-scraper`
   - **Среда выполнения:** `Python 3.11`
   - **Точка входа:** `index.handler`
   - **Таймаут:** `600` (10 минут)
   - **Память:** `512 МБ`

3. **Загрузка кода:**
   - Метод: ZIP-архив
   - Создайте ZIP с файлами `index.py` и `requirements.txt`
   - Загрузите архив

### Через Yandex CLI:

```bash
# Установка CLI
curl https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash

# Инициализация
yc init

# Создание функции
yc serverless function create --name currency-scraper

# Создание версии функции
yc serverless function version create \
  --function-name currency-scraper \
  --runtime python311 \
  --entrypoint index.handler \
  --memory 512m \
  --execution-timeout 600s \
  --source-path ./currency-scraper.zip
```

## 4. Настройка переменных окружения

В консоли Yandex Cloud или через CLI:

```bash
# Через CLI
yc serverless function version create \
  --function-name currency-scraper \
  --runtime python311 \
  --entrypoint index.handler \
  --memory 512m \
  --execution-timeout 600s \
  --source-path ./currency-scraper.zip \
  --environment GMAIL_EMAIL=your-email@gmail.com \
  --environment GMAIL_PASSWORD=your-app-password \
  --environment TARGET_EMAIL=andrey.koldayev.onex.kz@gmail.com \
  --environment API_URL=https://your-api-endpoint.com/webhook
```

### Переменные окружения:
- `OUTLOOK_EMAIL` - ваш Outlook адрес
- `OUTLOOK_PASSWORD` - пароль приложения Outlook
- `TARGET_EMAIL` - email для уведомлений (andrey.koldayev@r-express.ru)
- `API_URL` - URL другой Cloud Function для отправки данных

## 5. Настройка триггера по расписанию

### Через консоль:
1. Перейдите в "Триггеры" → "Создать триггер"
2. **Тип:** Таймер
3. **Имя:** `currency-scraper-schedule`
4. **Расписание:** 
   - `10 0 * * *` (00:10 ежедневно)
   - `0 9 * * *` (09:00 ежедневно)  
   - `45 16 * * *` (16:45 ежедневно)

### Через CLI:
```bash
# Триггер в 00:10
yc serverless trigger create timer \
  --name currency-scraper-00-10 \
  --cron-expression "10 0 * * *" \
  --invoke-function-name currency-scraper

# Триггер в 09:00
yc serverless trigger create timer \
  --name currency-scraper-09-00 \
  --cron-expression "0 9 * * *" \
  --invoke-function-name currency-scraper

# Триггер в 16:45
yc serverless trigger create timer \
  --name currency-scraper-16-45 \
  --cron-expression "45 16 * * *" \
  --invoke-function-name currency-scraper
```

## 6. Создание функции-получателя данных

Пример простой функции для получения данных:

```python
import json
import logging

def handler(event, context):
    """Функция для получения данных от скреппера"""
    
    try:
        # Получение данных из запроса
        body = json.loads(event.get('body', '{}'))
        
        # Логирование полученных данных
        logging.info(f"Получено {len(body.get('data', []))} записей")
        logging.info(f"Статистика: {body.get('summary', {})}")
        
        # Здесь можно добавить обработку данных:
        # - сохранение в базу данных
        # - отправка в другие системы
        # - дополнительная обработка
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Данные успешно получены'})
        }
        
    except Exception as e:
        logging.error(f"Ошибка обработки данных: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
```

## 7. Мониторинг и логи

### Просмотр логов:
```bash
# Через CLI
yc logging read --group-id <log-group-id> --filter 'resource.id = "<function-id>"'

# Или в веб-консоли:
# Cloud Functions → Ваша функция → Логи
```

### Мониторинг:
- Переходите в "Мониторинг" в консоли функции
- Отслеживайте количество вызовов, ошибки, время выполнения

## 8. Тестирование

### Ручной запуск:
```bash
# Через CLI
yc serverless function invoke --name currency-scraper

# Через консоль:
# Cloud Functions → Ваша функция → Тестирование
```

## 9. Безопасность

### Рекомендации:
1. **Используйте сервисные аккаунты** для доступа к ресурсам
2. **Ограничьте права доступа** только необходимыми
3. **Регулярно обновляйте пароли** приложений
4. **Мониторьте логи** на предмет подозрительной активности

## 10...