import requests
from typing import Dict, List, Any, Optional


class HHAPI:
    """Класс для взаимодействия с API HeadHunter"""

    BASE_URL = "https://api.hh.ru/"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'HH-API-App/1.0 (sergey.molchan@example.com)'
        })

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
            try:
                response = self.session.get(
                    f"{self.BASE_URL}employers/{employer_id}"
                )
                response.raise_for_status()
                employers.append(response.json())
            except requests.RequestException as e:
                print(f"Ошибка при получении данных работодателя {employer_id}: {e}")

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
        per_page = 100

        while True:
            try:
                params = {
                    'employer_id': employer_id,
                    'page': page,
                    'per_page': per_page,
                    'only_with_salary': True
                }

                response = self.session.get(
                    f"{self.BASE_URL}vacancies",
                    params=params
                )
                response.raise_for_status()

                data = response.json()
                vacancies.extend(data.get('items', []))

                # Проверяем, есть ли следующая страница
                pages = data.get('pages', 0)
                if page >= pages - 1:
                    break

                page += 1

            except requests.RequestException as e:
                print(f"Ошибка при получении вакансий работодателя {employer_id}: {e}")
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
            print(f"Получение вакансий для работодателя {employer_id}...")
            vacancies = self.get_vacancies_by_employer(employer_id)
            all_vacancies[employer_id] = vacancies
            print(f"Получено {len(vacancies)} вакансий")

        return all_vacancies