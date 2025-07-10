import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

db_params = {
    "dbname": "pgvector_db",
    "user": "postgres",
    "password": "postgres_ai_agent",
    "host": "82.202.142.56",
    "port": 5480
}

sql_commands = [
    # Создание расширения pgvector
    "CREATE EXTENSION IF NOT EXISTS vector",
    
    # Таблица modules
    """
    CREATE TABLE IF NOT EXISTS modules (
        id UUID PRIMARY KEY,
        name VARCHAR(255) NOT NULL UNIQUE,
        label VARCHAR(255),
        description TEXT,
        auth_type VARCHAR(50),
        categories VARCHAR(255)[],
        embedding VECTOR(384)
    )
    """,
    
    # Таблица api_docs
    """
    CREATE TABLE IF NOT EXISTS api_docs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        module_id UUID REFERENCES modules(id),
        source_url TEXT,
        content TEXT,
        embedding VECTOR(384) NOT NULL,
        chunk_id VARCHAR(255)
    )
    """,
    
    # Таблица actions
    """
    CREATE TABLE IF NOT EXISTS actions (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        module_id UUID REFERENCES modules(id),
        name TEXT NOT NULL,
        label TEXT NOT NULL,
        description TEXT,
        method TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        parameters JSONB,
        embedding VECTOR(384)
    )
    """,
    
    # Таблица triggers
    """
    CREATE TABLE IF NOT EXISTS triggers (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        module_id UUID REFERENCES modules(id),
        name TEXT NOT NULL,
        label TEXT NOT NULL,
        description TEXT,
        event_type TEXT NOT NULL,
        payload_schema JSONB,
        embedding VECTOR(384))
    """,
    
    # Таблица connections
    """
    CREATE TABLE IF NOT EXISTS connections (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        trigger_id UUID REFERENCES triggers(id),
        action_id UUID REFERENCES actions(id),
        mapping JSONB,
        created_at TIMESTAMP DEFAULT NOW())
    """,
    
    # Индексы для api_docs
    "CREATE INDEX IF NOT EXISTS idx_api_docs_ivfflat ON api_docs USING ivfflat (embedding) WITH (lists = 100)",
    "CREATE INDEX IF NOT EXISTS idx_api_docs_hnsw ON api_docs USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)"
]

def setup_database():
    try:
        conn = psycopg2.connect(**db_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Проверяем существование расширения pgvector
        cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
        if not cursor.fetchone():
            print("Установка расширения pgvector...")
            cursor.execute("CREATE EXTENSION vector")
        
        # Выполняем все SQL команды
        for command in sql_commands:
            cursor.execute(command)
            print(f"Выполнено: {command.split()[0]}...")
        
        print("\nБаза данных успешно инициализирована!")
        
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    setup_database()