import pytest
from unittest.mock import Mock, patch
from src.api import HHAPI


class TestHHAPI:
    """Тесты для HHAPI"""

    @patch('requests.Session.get')
    def test_get_employer_success(self, mock_get):
        """Тест получения работодателя"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'id': 123, 'name': 'Test'}
        mock_get.return_value = mock_response

        api = HHAPI()
        result = api.get_employer(123)

        assert result is not None
        mock_get.assert_called_once()

    @patch('requests.Session.get')
    def test_get_employer_not_found(self, mock_get):
        """Тест отсутствия работодателя"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        api = HHAPI()
        result = api.get_employer(999)

        assert result is None

    @patch('requests.Session.get')
    def test_get_vacancies_empty(self, mock_get):
        """Тест пустых вакансий"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'items': [], 'pages': 0, 'found': 0}
        mock_get.return_value = mock_response

        api = HHAPI()
        result = api.get_vacancies_by_employer(123)

        assert result == []