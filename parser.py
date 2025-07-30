import json
import psycopg2
from pgvector.psycopg2 import register_vector
import logging
import sys
import os
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
        """Создает или получает ID модуля с учётом новой структуры таблицы"""
        with self.conn.cursor() as cur:
            try:
                # Ищем по имени модуля (как в БД)
                cur.execute("SELECT id FROM modules WHERE name = %s", (module_name,))
                module = cur.fetchone()
                
                if module:
                    logging.info(f"Модуль {module_name} найден, ID: {module[0]}")
                    return module[0]
                
                # Создаем новый модуль с минимально необходимыми полями
                cur.execute("""
                    INSERT INTO modules (
                        id, 
                        name, 
                        label, 
                        description,
                        auth_type,
                        is_public
                    )
                    VALUES (
                        gen_random_uuid(),
                        %s, 
                        %s,
                        'Автоматически создан при импорте документации',
                        'OAUTH2',
                        TRUE
                    )
                    RETURNING id
                """, (module_name, module_label))
                module_id = cur.fetchone()[0]
                self.conn.commit()
                logging.info(f"Создан новый модуль: {module_name}, ID: {module_id}")
                return module_id
                
            except Exception as e:
                logging.error(f"Ошибка при работе с модулем {module_name}: {e}")
                self.conn.rollback()
                raise

    def load_docs(self, file_path, module_name, module_label):
        """Загружает документацию в БД с обработкой особенностей файлов"""
        try:
            # Проверка существования файла
            if not os.path.exists(file_path):
                logging.error(f"Файл не найден: {file_path}")
                return 0
                
            module_id = self.get_or_create_module(module_name, module_label)
            loaded_count = 0
            skipped_count = 0
            total_bytes = os.path.getsize(file_path)
            processed_bytes = 0
            
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f, self.conn.cursor() as cur:
                for line in f:
                    try:
                        processed_bytes += len(line)
                        # Логирование прогресса каждые 10%
                        if processed_bytes % max(1, total_bytes // 10) < 100:
                            progress = processed_bytes / total_bytes * 100
                            logging.info(f"Прогресс: {progress:.1f}% ({processed_bytes}/{total_bytes} байт)")
                        
                        data = json.loads(line)
                        embedding = data.get('embedding', [])
                        
                        # Проверка и нормализация эмбеддинга
                        if len(embedding) == 0:
                            skipped_count += 1
                            continue
                            
                        # Конвертация в список float если необходимо
                        if isinstance(embedding, np.ndarray):
                            embedding = embedding.tolist()
                            
                        # Проверка размерности
                        if len(embedding) != 384:
                            logging.warning(
                                f"Неверная размерность эмбеддинга: {len(embedding)} "
                                f"в чанке {data.get('id', 'unknown')}. Исправление до 384."
                            )
                            # Дополняем или обрезаем до 384 измерений
                            if len(embedding) < 384:
                                embedding += [0.0] * (384 - len(embedding))
                            else:
                                embedding = embedding[:384]
                        
                        # Вставка данных
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
                            data.get('text', ''),
                            embedding,
                            data.get('id', '')
                        ))
                        loaded_count += 1
                        
                        # Пакетный коммит каждые 100 записей
                        if loaded_count % 100 == 0:
                            self.conn.commit()
                            logging.info(f"Загружено {loaded_count} документов")
                            
                    except json.JSONDecodeError:
                        logging.warning(f"Ошибка декодирования JSON в строке: {line[:100]}...")
                    except KeyError as e:
                        logging.warning(f"Отсутствует обязательное поле в данных: {e}")
                    except Exception as e:
                        logging.warning(f"Ошибка обработки строки: {e}")
                
                self.conn.commit()
                logging.info(f"Загружено документов: {loaded_count} для модуля {module_name}")
                if skipped_count > 0:
                    logging.warning(f"Пропущено {skipped_count} записей с пустыми эмбеддингами")
                return loaded_count
                
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
        "port": 5480
    }
    
    # Соответствие файлов и имен модулей в БД
    MODULE_MAPPING = {
        "embeddings_yandex_tracker.jsonl": ("YandexTracker", "Яндекс Трекер"),
        "embeddings_google_drive.jsonl": ("google_drive", "Google Drive"),
        "embeddings_Bitrix24.jsonl": ("bitrix", "Bitrix24")
    }
    
    loader = None
    try:
        loader = DocLoader(db_params)
        
        # Обработка всех файлов в папке embeddings
        embeddings_dir = "embeddings"
        for filename in os.listdir(embeddings_dir):
            if filename.endswith(".jsonl") and filename in MODULE_MAPPING:
                file_path = os.path.join(embeddings_dir, filename)
                module_name, module_label = MODULE_MAPPING[filename]
                
                logging.info(f"Начало загрузки: {filename} → {module_name}")
                result = loader.load_docs(file_path, module_name, module_label)
                
                if result > 0:
                    logging.info(f"Успешно загружено {result} документов для {module_name}")
                else:
                    logging.error(f"Не удалось загрузить документы для {module_name}")
        
    except ConnectionError:
        logging.critical("Не удалось установить соединение с БД. Проверьте параметры подключения.")
    except Exception as e:
        logging.critical(f"Фатальная ошибка: {e}", exc_info=True)
    finally:
        if loader:
            loader.close()