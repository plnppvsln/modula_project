import json
import psycopg2
from pgvector.psycopg2 import register_vector
import logging
import sys
import numpy as np

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('doc_loader.log')
    ]
)

class DocLoader:
    def __init__(self, db_params):
        self.conn = None
        try:
            self.conn = psycopg2.connect(**db_params)
            register_vector(self.conn)
            logging.info("Успешное подключение к базе данных")
        except psycopg2.OperationalError as e:
            logging.error(f"Ошибка подключения к БД: {e}")
            raise ConnectionError("Не удалось подключиться к базе данных") from e
        except Exception as e:
            logging.error(f"Неизвестная ошибка: {e}")
            raise

    def get_or_create_module(self, module_name, module_label):
        """Создает или получает ID модуля"""
        with self.conn.cursor() as cur:
            try:
                cur.execute("SELECT id FROM modules WHERE name = %s", (module_name,))
                module = cur.fetchone()
                
                if module:
                    logging.info(f"Модуль {module_name} найден, ID: {module[0]}")
                    return module[0]
                
                # Создаем вектор из 384 нулей
                default_embedding = [0.0] * 384
                cur.execute("""
                    INSERT INTO modules (
                        id, name, label, description, 
                        auth_type, categories, embedding
                    )
                    VALUES (
                        gen_random_uuid(),
                        %s, 
                        %s,
                        'Автоматически создан при импорте документации',
                        'OAUTH2',
                        ARRAY['default'],
                        %s::vector(384)
                    )
                    RETURNING id
                """, (module_name, module_label, default_embedding))
                module_id = cur.fetchone()[0]
                self.conn.commit()
                logging.info(f"Создан новый модуль: {module_name}, ID: {module_id}")
                return module_id
                
            except Exception as e:
                logging.error(f"Ошибка при работе с модулем {module_name}: {e}")
                self.conn.rollback()
                raise

    def load_docs(self, file_path, module_name, module_label):
        """Загружает документацию в БД"""
        try:
            module_id = self.get_or_create_module(module_name, module_label)
            loaded_count = 0
            
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f, self.conn.cursor() as cur:
                for line in f:
                    try:
                        data = json.loads(line)
                        embedding = data.get('embedding', [])

                        # Проверка размерности
                        if len(embedding) != 384:
                            logging.warning(f"Неверная размерность эмбеддинга: {len(embedding)} в чанке {data.get('id', 'unknown')}")
                            continue

                        cur.execute("""
                            INSERT INTO api_docs (
                                module_id, 
                                source_url, 
                                content, 
                                embedding,
                                chunk_id
                            )
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            module_id,
                            data.get('source', ''),
                            data['text'],
                            embedding,  
                            data['id']  
                        ))
                        loaded_count += 1
                        
                        # Логирование прогресса
                        if loaded_count % 100 == 0:
                            logging.info(f"Обработано {loaded_count} документов")
                            
                    except json.JSONDecodeError:
                        logging.warning(f"Ошибка декодирования JSON в строке: {line[:100]}...")
                    except KeyError as e:
                        logging.warning(f"Отсутствует обязательное поле в данных: {e}")
                    except Exception as e:
                        logging.warning(f"Ошибка обработки строки: {e}")
                
                # Обновляем эмбеддинг модуля
                cur.execute("""
                    UPDATE modules
                    SET embedding = COALESCE(
                        (SELECT AVG(embedding) FROM api_docs WHERE module_id = %s),
                        %s::vector(384)
                    )
                    WHERE id = %s
                """, (module_id, [0.0] * 384, module_id))
                
                self.conn.commit()
                logging.info(f"Загружено документов: {loaded_count} для модуля {module_name}")
                return loaded_count
                
        except FileNotFoundError:
            logging.error(f"Файл не найден: {file_path}")
            return 0
        except Exception as e:
            logging.error(f"Критическая ошибка при загрузке {file_path}: {e}")
            self.conn.rollback()
            return 0

    def close(self):
        """Закрывает соединение с БД"""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logging.info("Соединение с БД закрыто")


if __name__ == "__main__":
    # Параметры подключения
    db_params = {
        "dbname": "pgvector_db",
        "user": "postgres",
        "password": "postgres_ai_agent",
        "host": "82.202.142.56",
        "port": 5480  # Порт как число
    }
    
    loader = None
    try:
        loader = DocLoader(db_params)
        
        # Загрузка Яндекс.Трекера
        result = loader.load_docs(
            "embeddings/embeddings_yandex_tracker.jsonl",
            "yandex_tracker",
            "Яндекс Трекер"
        )
        if not result:
            logging.error("Не удалось загрузить документы для Яндекс.Трекера")
        
        # Загрузка Google Drive
        result = loader.load_docs(
            "embeddings/embeddings_google_drive.jsonl",
            "google_drive",
            "Google Drive"
        )
        if not result:
            logging.error("Не удалось загрузить документы для Google Drive")
        
    except ConnectionError:
        logging.critical("Не удалось установить соединение с БД. Проверьте параметры подключения.")
    except Exception as e:
        logging.critical(f"Фатальная ошибка: {e}", exc_info=True)
    finally:
        if loader:
            loader.close()