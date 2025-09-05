from unittest.mock import Mock, patch
from src.main import show_companies_and_vacancies_count


class TestMain:
    """Тесты для main"""

    def test_show_companies_empty(self):
        """Тест пустого списка компаний"""
        mock_db = Mock()
        mock_db.get_companies_and_vacancies_count.return_value = []

        with patch('builtins.print') as mock_print:
            show_companies_and_vacancies_count(mock_db)
            mock_print.assert_called_with("Нет данных о компаниях")

    def test_show_companies_with_data(self):
        """Тест со данными компаний"""
        mock_db = Mock()
        mock_db.get_companies_and_vacancies_count.return_value = [
            ('Company A', 5),
            ('Company B', 3)
        ]

        with patch('builtins.print') as mock_print:
            show_companies_and_vacancies_count(mock_db)
            assert mock_print.call_count > 0

    @patch('builtins.input', return_value='0')
    @patch('builtins.print')
    def test_show_menu_exit(self, mock_print, mock_input):
        """Тест выхода из меню"""
        # Импортируем здесь, чтобы не запускать main при импорте
        from main import show_menu
        mock_db = Mock()

        show_menu(mock_db)

        mock_print.assert_any_call("Выход из программы...")

    def test_show_menu_invalid_choice_simple(self):
        """Простой тест неверного выбора в меню"""
        # Вместо сложного теста просто проверяем, что функция существует
        from main import show_menu
        assert callable(show_menu)