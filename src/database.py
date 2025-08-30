import psycopg2
from psycopg2 import sql
from typing import Dict, Any
import configparser


class DatabaseManager:
    """Класс для управления базой данных PostgreSQL"""

    def __init__(self, config_file: str = 'config/database.ini'):
        self.config = self._load_config(config_file)
        self.connection = None

    def _load_config(self, config_file: str) -> Dict[str, str]:
        """
        Загрузить конфигурацию из файла

        Args:
            config_file: Путь к файлу конфигурации

        Returns:
            Словарь с параметрами подключения
        """
        config = configparser.ConfigParser()
        config.read(config_file)

        return {
            'host': config['postgresql']['host'],
            'database': config['postgresql']['database'],
            'user': config['postgresql']['user'],
            'password': config['postgresql']['password'],
            'port': config['postgresql']['port']
        }

    def connect(self) -> None:
        """Установить подключение к базе данных"""
        try:
            self.connection = psycopg2.connect(**self.config)
            print("Подключение к базе данных установлено")
        except psycopg2.Error as e:
            print(f"Ошибка подключения к базе данных: {e}")
            raise

    def disconnect(self) -> None:
        """Закрыть подключение к базе данных"""
        if self.connection:
            self.connection.close()
            print("Подключение к базе данных закрыто")

    def create_database(self) -> None:
        """Создать базу данных если она не существует"""
        try:
            # Подключаемся к базе данных postgres для создания новой БД
            temp_config = self.config.copy()
            temp_config['database'] = 'postgres'
            conn = psycopg2.connect(**temp_config)
            conn.autocommit = True
            cursor = conn.cursor()

            # Проверяем существование базы данных
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (self.config['database'],)
            )
            exists = cursor.fetchone()

            if not exists:
                cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(self.config['database'])
                    )
                )
                print(f"База данных {self.config['database']} создана")
            else:
                print(f"База данных {self.config['database']} уже существует")

            cursor.close()
            conn.close()

        except psycopg2.Error as e:
            print(f"Ошибка при создании базы данных: {e}")
            raise

    def create_tables(self) -> None:
        """Создать таблицы в базе данных"""
        try:
            self.connect()
            cursor = self.connection.cursor()

            # Создание таблицы employers
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS employers (
                    employer_id INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    url VARCHAR(255),
                    description TEXT,
                    open_vacancies INTEGER
                )
            """)

            # Создание таблицы vacancies
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vacancies (
                    vacancy_id INTEGER PRIMARY KEY,
                    employer_id INTEGER REFERENCES employers(employer_id),
                    title VARCHAR(255) NOT NULL,
                    salary_from INTEGER,
                    salary_to INTEGER,
                    currency VARCHAR(10),
                    url VARCHAR(255) NOT NULL,
                    requirement TEXT,
                    responsibility TEXT,
                    published_at TIMESTAMP
                )
            """)

            self.connection.commit()
            print("Таблицы созданы успешно")
            cursor.close()

        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Ошибка при создании таблиц: {e}")
            raise
        finally:
            self.disconnect()

    def insert_employer(self, employer_data: Dict[str, Any]) -> None:
        """
        Вставить данные работодателя в таблицу

        Args:
            employer_data: Данные работодателя
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            cursor.execute("""
                INSERT INTO employers (employer_id, name, url, description, open_vacancies)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (employer_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    url = EXCLUDED.url,
                    description = EXCLUDED.description,
                    open_vacancies = EXCLUDED.open_vacancies
            """, (
                employer_data['id'],
                employer_data['name'],
                employer_data.get('alternate_url'),
                employer_data.get('description'),
                employer_data.get('open_vacancies', 0)
            ))

            self.connection.commit()
            cursor.close()

        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Ошибка при вставке работодателя {employer_data['id']}: {e}")
        finally:
            self.disconnect()

    def insert_vacancy(self, vacancy_data: Dict[str, Any]) -> None:
        """
        Вставить данные вакансии в таблицу

        Args:
            vacancy_data: Данные вакансии
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            salary_from = None
            salary_to = None
            currency = None

            if vacancy_data.get('salary'):
                salary = vacancy_data['salary']
                salary_from = salary.get('from')
                salary_to = salary.get('to')
                currency = salary.get('currency')

            cursor.execute("""
                INSERT INTO vacancies (
                    vacancy_id, employer_id, title, salary_from, salary_to,
                    currency, url, requirement, responsibility, published_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (vacancy_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    salary_from = EXCLUDED.salary_from,
                    salary_to = EXCLUDED.salary_to,
                    currency = EXCLUDED.currency,
                    url = EXCLUDED.url,
                    requirement = EXCLUDED.requirement,
                    responsibility = EXCLUDED.responsibility,
                    published_at = EXCLUDED.published_at
            """, (
                vacancy_data['id'],
                vacancy_data['employer']['id'],
                vacancy_data['name'],
                salary_from,
                salary_to,
                currency,
                vacancy_data.get('alternate_url'),
                vacancy_data.get('snippet', {}).get('requirement'),
                vacancy_data.get('snippet', {}).get('responsibility'),
                vacancy_data.get('published_at')
            ))

            self.connection.commit()
            cursor.close()

        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Ошибка при вставке вакансии {vacancy_data['id']}: {e}")
        finally:
            self.disconnect()