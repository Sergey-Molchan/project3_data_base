import pytest
from unittest.mock import Mock, patch
from src.database import DatabaseManager


class TestDatabaseManager:
    """Тесты для DatabaseManager"""

    @patch('database.psycopg2.connect')
    def test_connect_success(self, mock_connect):
        """Тест успешного подключения к БД"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        db_manager = DatabaseManager()
        db_manager.config = {
            'host': 'localhost',
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass',
            'port': '5432'
        }

        db_manager.connect()

        mock_connect.assert_called_once()
        assert db_manager.connection == mock_conn

    @patch('database.psycopg2.connect')
    def test_insert_employer(self, mock_connect, sample_employer_data):
        """Тест добавления работодателя"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        db_manager = DatabaseManager()
        db_manager.connection = mock_conn

        db_manager.insert_employer(sample_employer_data)

        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

    @patch('database.psycopg2.connect')
    def test_insert_vacancy(self, mock_connect, sample_vacancy_data):
        """Тест добавления вакансии"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        db_manager = DatabaseManager()
        db_manager.connection = mock_conn

        db_manager.insert_vacancy(sample_vacancy_data)

        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

    def test_load_config_success(self, tmp_path):
        """Тест загрузки конфигурации"""
        config_file = tmp_path / "test.ini"
        config_file.write_text("""
[postgresql]
host=test_host
database=test_db
user=test_user
password=test_pass
port=5432
        """)

        db_manager = DatabaseManager()
        config = db_manager._load_config(str(config_file))

        assert config['host'] == 'test_host'
        assert config['database'] == 'test_db'

    def test_load_config_file_not_found(self):
        """Тест отсутствия config файла"""
        db_manager = DatabaseManager()
        with pytest.raises(FileNotFoundError):
            db_manager._load_config('nonexistent.ini')