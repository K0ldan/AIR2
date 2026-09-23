# AIR2 - Apache Airflow Project

Локальный стенд Apache Airflow с CeleryExecutor, Redis и PostgreSQL для разработки и тестирования ETL-пайплайнов.

## 🚀 Быстрый старт

### 1. Подготовка окружения

```bash
# Скопируйте пример конфигурации
cp .env.example .env

# Отредактируйте .env и заполните ВСЕ обязательные переменные
# (см. раздел "Переменные окружения" ниже)
```

### 2. Генерация секретов

Выполните следующие команды для генерации криптографически стойких ключей:

```bash
# Fernet ключ (для шифрования паролей соединений в БД Airflow)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# JWT секрет (для API аутентификации)
python -c "import secrets; print(secrets.token_urlsafe(32))"

# Flask секрет (для сессий веб-интерфейса)
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

**Важно:** Каждый ключ должен быть уникальным. Не используйте один и тот же ключ для разных целей.

### 3. Запуск

```bash
# Запуск всех сервисов
docker-compose up -d

# Проверка статуса
docker-compose ps

# Логи
docker-compose logs -f airflow-scheduler
```

Веб-интерфейс Airflow будет доступен по адресу: http://localhost:8080

Логин/пароль: те, что указаны в `.env` (`_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`)

### 4. Остановка

```bash
docker-compose down

# С удалением томов (очистка БД)
docker-compose down -v
```

---

## 🔐 Переменные окружения

Создайте файл `.env` на основе `.env.example` и заполните **все** обязательные переменные:

| Переменная | Описание | Обязательна | Пример генерации |
|------------|----------|-------------|------------------|
| `AIRFLOW_UID` | UID пользователя в контейнерах (Linux) | Да (Linux) | `id -u` |
| `_AIRFLOW_WWW_USER_USERNAME` | Имя админа веб-интерфейса | **Да** | `airflow_admin` |
| `_AIRFLOW_WWW_USER_PASSWORD` | Пароль админа веб-интерфейса | **Да** | `Str0ngP@ssw0rd!` |
| `FERNET_KEY` | Ключ шифрования паролей соединений | **Да** | `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Секрет для JWT токенов API | **Да** | `python -c "import secrets; print(secrets.token_urlsafe(32))"` |
| `AIRFLOW__API__SECRET_KEY` | Секрет Flask для сессий | **Да** | `python -c "import secrets; print(secrets.token_urlsafe(32))"` |
| `POSTGRES_PASSWORD` | Пароль PostgreSQL | **Да** | `openssl rand -base64 24` |
| `REDIS_PASSWORD` | Пароль Redis (опционально) | Нет | `openssl rand -base64 24` |
| `_PIP_ADDITIONAL_REQUIREMENTS` | Доп. Python пакеты при старте | Нет | `pandas numpy` |

> ⚠️ **Никогда не коммитьте файл `.env` в Git!** Он добавлен в `.gitignore`.

---

## 📁 Структура проекта

```
AIR2/
├── .env                    # Секреты (НЕ в git!)
├── .env.example            # Шаблон с плейсхолдерами
├── .gitignore              # Исключения для git
├── docker-compose.yaml     # Основная конфигурация сервисов
├── config/                 # Конфиги Airflow (airflow.cfg генерируется автоматически)
├── dags/                   # DAG-файлы (пользовательские пайплайны)
│   └── weather_etl.py      # Пример ETL: погода → PostgreSQL
├── logs/                   # Логи Airflow (НЕ в git!)
├── plugins/                # Пользовательские плагины Airflow
└── README.md               # Этот файл
```

---

## 🛠 Полезные команды

```bash
# Создание кастомного образа (если меняли _PIP_ADDITIONAL_REQUIREMENTS)
docker-compose build

# Вход в контейнер scheduler
docker-compose exec airflow-scheduler bash

# CLI Airflow
docker-compose exec airflow-scheduler airflow dags list
docker-compose exec airflow-scheduler airflow tasks test weather_etl extract 2026-01-01

# Подключение к PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow

# Подключение к Redis
docker-compose exec redis redis-cli -a "$REDIS_PASSWORD"
```

---

## 📦 DAG: weather_etl

Пример ETL-пайплайна (`dags/weather_etl.py`):
1. **Extract** — получение погоды для городов через open-meteo.com (бесплатно, без ключа)
2. **Transform** — округление значений
3. **Load** — запись в PostgreSQL таблицу `weather_data`

Таблица создаётся автоматически при первом запуске (через Airflow Connection `postgres_default`).

---



## 📄 Лицензия

Apache License 2.0 — см. заголовок `docker-compose.yaml`.
