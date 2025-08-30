import requests
from typing import Dict, List, Any, Optional
import time
import json


class HHAPI:
    """Класс для взаимодействия с API HeadHunter"""

    BASE_URL = "https://api.hh.ru/"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'MyVacancyApp/1.0 (molchansergey@gmail.com)',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })

    def get_employer(self, employer_id: str) -> Optional[Dict[str, Any]]:
        """
        Получить информацию о работодателе по его ID

        Args:
            employer_id: ID работодателя

        Returns:
            Словарь с информацией о работодателе или None при ошибке
        """
        try:
            url = f"{self.BASE_URL}employers/{employer_id}"
            print(f"Запрос: {url}")

            response = self.session.get(url, timeout=10)

            print(f"Статус: {response.status_code}")

            if response.status_code == 404:
                print(f"Работодатель с ID {employer_id} не найден")
                return None
            elif response.status_code == 400:
                print(f"Неверный запрос для работодателя {employer_id}")
                # Попробуем получить данные через поиск
                return self._search_employer(employer_id)

            response.raise_for_status()

            data = response.json()
            print(f"Получены данные работодателя: {data.get('name', 'Unknown')}")
            return data

        except requests.RequestException as e:
            print(f"Ошибка при получении данных работодателя {employer_id}: {e}")
            return None

    def _search_employer(self, employer_id: str) -> Optional[Dict[str, Any]]:
        """
        Поиск работодателя через API поиска

        Args:
            employer_id: ID или название работодателя

        Returns:
            Данные работодателя или None
        """
        try:
            params = {
                'text': employer_id,
                'only_with_vacancies': True,
                'per_page': 1
            }

            response = self.session.get(
                f"{self.BASE_URL}employers",
                params=params,
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                if data.get('items'):
                    employer = data['items'][0]
                    print(f"Найден работодатель через поиск: {employer.get('name')}")
                    return employer

            return None

        except requests.RequestException as e:
            print(f"Ошибка при поиске работодателя {employer_id}: {e}")
            return None

    def get_employers(self, employer_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Получить информацию о работодателях по их ID

        Args:
            employer_ids: Список ID работодателей

        Returns:
            Список словарей с информацией о работодателях
        """
        employers = []
        for employer_id in employer_ids:
            employer_data = self.get_employer(employer_id)
            if employer_data:
                employers.append(employer_data)
            time.sleep(0.2)  # Задержка между запросами

        return employers

    def get_vacancies_by_employer(self, employer_id: str) -> List[Dict[str, Any]]:
        """
        Получить вакансии конкретного работодателя

        Args:
            employer_id: ID работодателя

        Returns:
            Список словарей с информацией о вакансиях
        """
        vacancies = []
        page = 0
        per_page = 20  # Уменьшим количество на странице

        while True:
            try:
                params = {
                    'employer_id': employer_id,
                    'page': page,
                    'per_page': per_page,
                    'only_with_salary': True,
                    'archived': False
                }

                print(f"Запрос вакансий для {employer_id}, страница {page}")

                response = self.session.get(
                    f"{self.BASE_URL}vacancies",
                    params=params,
                    timeout=10
                )

                print(f"Статус вакансий: {response.status_code}")

                if response.status_code != 200:
                    print(f"Ошибка запроса: {response.text}")
                    break

                data = response.json()
                items = data.get('items', [])
                vacancies.extend(items)

                print(f"Получено {len(items)} вакансий")

                # Проверяем, есть ли следующая страница
                pages = data.get('pages', 0)
                found = data.get('found', 0)

                print(f"Всего найдено: {found}, страниц: {pages}")

                if page >= pages - 1 or not items or page >= 4:  # Ограничим 5 страниц
                    break

                page += 1
                time.sleep(0.3)  # Задержка между запросами

            except requests.RequestException as e:
                print(f"Ошибка при получении вакансий работодателя {employer_id}: {e}")
                break
            except json.JSONDecodeError as e:
                print(f"Ошибка парсинга JSON: {e}")
                break

        return vacancies

    def get_all_vacancies(self, employer_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Получить все вакансии для списка работодателей

        Args:
            employer_ids: Список ID работодателей

        Returns:
            Словарь с вакансиями по работодателям
        """
        all_vacancies = {}

        for employer_id in employer_ids:
            print(f"\nПолучение вакансий для работодателя {employer_id}...")
            vacancies = self.get_vacancies_by_employer(employer_id)
            all_vacancies[employer_id] = vacancies
            print(f"Получено {len(vacancies)} вакансий для работодателя {employer_id}")
            time.sleep(1)  # Задержка между работодателями

        return all_vacancies


# Обновленный список компаний с работающими ID
POPULAR_COMPANIES = {
    'yandex': '1740',  # Яндекс
    'sber': '3529',  # Сбер
    'tinkoff': '4181',  # Тинькофф
    'vk': '15478',  # VK
    'ozon': '2180',  # Ozon (альтернативный ID)
    'kaspersky': '1057',  # Лаборатория Касперского
    '1c': '41862',  # 1C
    'mts': '3776',  # МТС
    'megafon': '3127',  # МегаФон
    'rostelecom': '2748',  # Ростелеком
    'wildberries': '87021',  # Wildberries
    'mail': '3778',  # Mail.ru Group
    'rambler': '8620',  # Rambler
    'alfa': '80',  # Альфа-Банк
    'gazprom': '39305',  # Газпром
}


# Альтернативный подход - получение топ компаний через поиск
def get_top_companies(api: HHAPI, count: int = 10) -> List[Dict[str, Any]]:
    """
    Получить топ компаний через поиск

    Args:
        api: Экземпляр HHAPI
        count: Количество компаний

    Returns:
        Список компаний
    """
    try:
        params = {
            'only_with_vacancies': True,
            'per_page': count,
            'sort_by': 'by_vacancies_open'
        }

        response = api.session.get(
            f"{api.BASE_URL}employers",
            params=params,
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            return data.get('items', [])

        return []

    except requests.RequestException as e:
        print(f"Ошибка при получении топ компаний: {e}")
        return []