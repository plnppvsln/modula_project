import json
import psycopg2
from pgvector.psycopg2 import register_vector

class YandexTrackerDocParser:
    def __init__(self, db_params):
        self.conn = psycopg2.connect(**db_params)
        register_vector(self.conn)
        self.module_name = "yandex_tracker"
        self.module_id = self._get_or_create_module()
    
    def _get_or_create_module(self):
        """Получаем ID модуля или создаем заглушку"""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM modules WHERE name = %s",
                (self.module_name,)
            )
            module = cur.fetchone()
            
            if module:
                return module[0]
            
            # Создаем модуль-заглушку
            cur.execute("""
                INSERT INTO modules (
                    id, name, label, description, 
                    auth_type, categories, embedding
                )
                VALUES (
                    gen_random_uuid(),
                    'yandex_tracker', 
                    'Яндекс Трекер',
                    'TEMPORARY: Автоматически создан при импорте документации',
                    'OAUTH2',
                    ARRAY['tracker','project_management'],
                    '[0]'::vector
                )
                RETURNING id
            """)
            return cur.fetchone()[0]

    def parse_and_load(self, file_path):
        """Парсинг JSONL и загрузка в БД"""
        with open(file_path, 'r') as f, self.conn.cursor() as cur:
            for line in f:
                data = json.loads(line)
                
                # Вставляем документацию
                cur.execute("""
                    INSERT INTO api_docs (
                        module_id, 
                        external_id, 
                        source_url, 
                        content, 
                        embedding
                    )
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    self.module_id,
                    data['id'],
                    data['source'],
                    data['text'],
                    data['embedding']
                ))
            
            # Обновляем эмбеддинг модуля как среднее всех его чанков
            cur.execute("""
                UPDATE modules
                SET embedding = (
                    SELECT AVG(embedding) 
                    FROM api_docs 
                    WHERE module_id = %s
                )
                WHERE id = %s
            """, (self.module_id, self.module_id))
        
        self.conn.commit()

def load_google_drive_docs(db_params, file_path):
    """Загрузка документации Google Drive в БД"""
    conn = psycopg2.connect(**db_params)
    register_vector(conn)
    
    with conn.cursor() as cur, open(file_path, 'r') as f:
        for line in f:
            data = json.loads(line)
            
            cur.execute("""
                INSERT INTO api_docs (
                    module_name, 
                    source_url, 
                    content, 
                    embedding
                )
                VALUES (%s, %s, %s, %s)
            """, (
                "google_drive",
                data['source'],
                data['text'],
                data['embedding']
            ))
    
    conn.commit()
    conn.close()

# Пример использования
if __name__ == "__main__":
    db_params = {
        "dbname": "pgvector_db",
        "user": "postgres",
        "password": "postgres_ai_agent",
        "host": "82.202.142.56:5480"
    }
    
    parser = YandexTrackerDocParser(db_params)
    parser.parse_and_load("embeddings/embeddings_yandex_tracker.jsonl")

    load_google_drive_docs(
        db_params, 
        "embeddings/embeddings_google_drive.jsonl"
    )