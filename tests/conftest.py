import pytest
from unittest.mock import Mock, patch
import sys
from pathlib import Path

# Добавляем src в путь Python
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

@pytest.fixture
def mock_config():
    """Фикстура с мок-конфигом для БД"""
    return {
        'host': 'localhost',
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_password',
        'port': '5432'
    }

@pytest.fixture
def sample_employer_data():
    """Пример данных работодателя"""
    return {
        'id': 12345,
        'name': 'Test Company',
        'alternate_url': 'https://test.com',
        'description': 'Test description',
        'open_vacancies': 10
    }

@pytest.fixture
def sample_vacancy_data():
    """Пример данных вакансии"""
    return {
        'id': 67890,
        'employer': {'id': 12345},
        'name': 'Python Developer',
        'salary': {'from': 100000, 'to': 150000, 'currency': 'RUR'},
        'alternate_url': 'https://test.com/vacancy/67890',
        'snippet': {'requirement': 'Python', 'responsibility': 'Code'},
        'published_at': '2023-12-01T10:00:00+0300'
    }