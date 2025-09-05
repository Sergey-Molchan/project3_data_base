import psycopg2
from typing import List, Dict, Any
from src.database import DatabaseManager


class DBManager(DatabaseManager):
    """Класс для работы с данными в БД с дополнительными методами анализа"""

    def get_companies_and_vacancies_count(self) -> List[Dict[str, Any]]:
        """
        Получить список всех компаний и количество вакансий у каждой компании

        Returns:
            Список словарей с информацией о компаниях и количестве вакансий
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            cursor.execute("""
                SELECT e.name, COUNT(v.vacancy_id) as vacancy_count
                FROM employers e
                LEFT JOIN vacancies v ON e.employer_id = v.employer_id
                GROUP BY e.employer_id, e.name
                ORDER BY vacancy_count DESC
            """)

            result = []
            for row in cursor.fetchall():
                result.append({
                    'company': row[0],
                    'vacancies_count': row[1]
                })

            cursor.close()
            return result

        except psycopg2.Error as e:
            print(f"Ошибка при получении данных: {e}")
            return []
        finally:
            self.disconnect()

    def get_all_vacancies(self) -> List[Dict[str, Any]]:
        """
        Получить список всех вакансий с указанием названия компании,
        названия вакансии, зарплаты и ссылки на вакансию

        Returns:
            Список словарей с информацией о вакансиях
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            cursor.execute("""
                SELECT 
                    e.name as company_name,
                    v.title as vacancy_title,
                    v.salary_from,
                    v.salary_to,
                    v.currency,
                    v.url
                FROM vacancies v
                JOIN employers e ON v.employer_id = e.employer_id
                ORDER BY e.name, v.title
            """)

            result = []
            for row in cursor.fetchall():
                salary_info = ""
                if row[2] or row[3]:
                    salary_from = row[2] or "не указано"
                    salary_to = row[3] or "не указано"
                    salary_info = f"{salary_from}-{salary_to} {row[4] or ''}"
                else:
                    salary_info = "не указана"

                result.append({
                    'company': row[0],
                    'title': row[1],
                    'salary': salary_info,
                    'url': row[5]
                })

            cursor.close()
            return result

        except psycopg2.Error as e:
            print(f"Ошибка при получении данных: {e}")
            return []
        finally:
            self.disconnect()

    def get_avg_salary(self) -> float:
        """
        Получить среднюю зарплату по вакансиям

        Returns:
            Средняя зарплата
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            cursor.execute("""
                SELECT AVG((COALESCE(salary_from, 0) + COALESCE(salary_to, 0)) / 2) as avg_salary
                FROM vacancies
                WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL
            """)

            result = cursor.fetchone()[0] or 0
            cursor.close()
            return round(float(result), 2)

        except psycopg2.Error as e:
            print(f"Ошибка при получении данных: {e}")
            return 0.0
        finally:
            self.disconnect()

    def get_vacancies_with_higher_salary(self) -> List[Dict[str, Any]]:
        """
        Получить список всех вакансий, у которых зарплата выше средней по всем вакансиям

        Returns:
            Список вакансий с зарплатой выше средней
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            cursor.execute("""
                SELECT 
                    e.name as company_name,
                    v.title,
                    v.salary_from,
                    v.salary_to,
                    v.currency,
                    v.url
                FROM vacancies v
                JOIN employers e ON v.employer_id = e.employer_id
                WHERE (COALESCE(v.salary_from, 0) + COALESCE(v.salary_to, 0)) / 2 > 
                      (SELECT AVG((COALESCE(salary_from, 0) + COALESCE(salary_to, 0)) / 2)
                       FROM vacancies 
                       WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL)
                ORDER BY (COALESCE(v.salary_from, 0) + COALESCE(v.salary_to, 0)) / 2 DESC
            """)

            result = []
            for row in cursor.fetchall():
                salary_info = f"{row[2] or 'не указано'}-{row[3] or 'не указано'} {row[4] or ''}"
                result.append({
                    'company': row[0],
                    'title': row[1],
                    'salary': salary_info,
                    'url': row[5]
                })

            cursor.close()
            return result

        except psycopg2.Error as e:
            print(f"Ошибка при получении данных: {e}")
            return []
        finally:
            self.disconnect()

    def get_vacancies_with_keyword(self, keyword: str) -> List[Dict[str, Any]]:
        """
        Получить список всех вакансий, в названии которых содержатся переданные слова

        Args:
            keyword: Ключевое слово для поиска

        Returns:
            Список вакансий, содержащих ключевое слово в названии
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            cursor.execute("""
                SELECT 
                    e.name as company_name,
                    v.title,
                    v.salary_from,
                    v.salary_to,
                    v.currency,
                    v.url
                FROM vacancies v
                JOIN employers e ON v.employer_id = e.employer_id
                WHERE LOWER(v.title) LIKE %s
                ORDER BY e.name, v.title
            """, (f'%{keyword.lower()}%',))

            result = []
            for row in cursor.fetchall():
                salary_info = ""
                if row[2] or row[3]:
                    salary_from = row[2] or "не указано"
                    salary_to = row[3] or "не указано"
                    salary_info = f"{salary_from}-{salary_to} {row[4] or ''}"
                else:
                    salary_info = "не указана"

                result.append({
                    'company': row[0],
                    'title': row[1],
                    'salary': salary_info,
                    'url': row[5]
                })

            cursor.close()
            return result

        except psycopg2.Error as e:
            print(f"Ошибка при получении данных: {e}")
            return []
        finally:
            self.disconnect()

    def get_top_vacancies_by_salary(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Получить топ вакансий по зарплате

        Args:
            limit: Количество вакансий для возврата

        Returns:
            Список вакансий с самой высокой зарплатой
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            cursor.execute("""
                SELECT 
                    e.name as company_name,
                    v.title,
                    v.salary_from,
                    v.salary_to,
                    v.currency,
                    v.url,
                    (COALESCE(v.salary_from, 0) + COALESCE(v.salary_to, 0)) / 2 as avg_salary
                FROM vacancies v
                JOIN employers e ON v.employer_id = e.employer_id
                WHERE v.salary_from IS NOT NULL OR v.salary_to IS NOT NULL
                ORDER BY avg_salary DESC
                LIMIT %s
            """, (limit,))

            result = []
            for row in cursor.fetchall():
                salary_info = f"{row[2] or 'не указано'}-{row[3] or 'не указано'} {row[4] or ''}"
                result.append({
                    'company': row[0],
                    'title': row[1],
                    'salary': salary_info,
                    'url': row[5],
                    'avg_salary': row[6]
                })

            cursor.close()
            return result

        except psycopg2.Error as e:
            print(f"Ошибка при получении данных: {e}")
            return []
        finally:
            self.disconnect()