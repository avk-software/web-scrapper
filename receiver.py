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