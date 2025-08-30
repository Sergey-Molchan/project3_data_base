from src.api import HHAPI, POPULAR_COMPANIES, get_top_companies
from src.database import DatabaseManager
from src.db_manager import DBManager



def main():
    """Основная функция для запуска приложения"""

    # Инициализация API
    api = HHAPI()

    print("=== Система сбора и анализа вакансий с hh.ru ===")

    # Попробуем получить топ компаний через поиск
    print("1. Поиск популярных компаний...")
    top_companies = get_top_companies(api, 10)

    if top_companies:
        employer_ids = [str(company['id']) for company in top_companies]
        company_names = [company['name'] for company in top_companies]

        print("Найдены компании:")
        for i, name in enumerate(company_names, 1):
            print(f"  {i}. {name}")
    else:
        # Используем запасной список
        print("Используем запасной список компаний")
        employer_ids = list(POPULAR_COMPANIES.values())[:10]
        company_names = list(POPULAR_COMPANIES.keys())[:10]

    # Инициализация менеджера базы данных
    try:
        db_manager = DatabaseManager()
        analysis_db = DBManager()

        print("2. Создание базы данных и таблиц...")

        # Создание базы данных и таблиц
        db_manager.create_database()
        db_manager.create_tables()

        print("3. Получение данных о работодателях...")
        employers = api.get_employers(employer_ids)

        print(f"Успешно получено данных о {len(employers)} работодателях")

        if employers:
            print("4. Сохранение данных работодателей...")
            for employer in employers:
                db_manager.insert_employer(employer)
                print(f"Сохранен работодатель: {employer.get('name')}")

            print("5. Получение вакансий...")
            all_vacancies = api.get_all_vacancies(employer_ids)

            print("6. Сохранение вакансий...")
            total_vacancies = 0
            for employer_id, vacancies in all_vacancies.items():
                for vacancy in vacancies:
                    db_manager.insert_vacancy(vacancy)
                total_vacancies += len(vacancies)
                print(f"Сохранено {len(vacancies)} вакансий")

            print(f"Всего сохранено {total_vacancies} вакансий")

            if total_vacancies > 0:
                # Интерфейс взаимодействия с пользователем
                user_interface(analysis_db)
            else:
                print("Не удалось получить вакансии. Возможно, проблемы с API.")
                demo_interface(analysis_db)
        else:
            print("Не удалось получить данные работодателей.")
            demo_interface(analysis_db)

    except Exception as e:
        print(f"Произошла ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Работа программы завершена")


def demo_interface(db_manager: DBManager) -> None:
    """
    Демонстрационный интерфейс для тестирования
    """
    print("\nЗапуск в демонстрационном режиме...")

    # Проверяем, есть ли данные в базе
    companies = db_manager.get_companies_and_vacancies_count()
    if companies:
        print("\nДанные в базе:")
        for company in companies:
            print(f"{company['company']}: {company['vacancies_count']} вакансий")

        user_interface(db_manager)
    else:
        print("В базе нет данных. Добавьте тестовые данные или проверьте подключение к API.")


def user_interface(db_manager: DBManager) -> None:
    """
    Интерфейс взаимодействия с пользователем
    """
    while True:
        print("\n" + "=" * 50)
        print("МЕНЮ АНАЛИЗА ВАКАНСИЙ")
        print("1. Список компаний и количество вакансий")
        print("2. Список всех вакансий")
        print("3. Средняя зарплата по вакансиям")
        print("4. Вакансии с зарплатой выше средней")
        print("5. Поиск вакансий по ключевому слову")
        print("6. Показать топ-10 вакансий по зарплате")
        print("0. Выход")
        print("=" * 50)

        choice = input("Выберите опцию (0-6): ").strip()

        if choice == '0':
            break

        elif choice == '1':
            print("\n--- Компании и количество вакансий ---")
            companies = db_manager.get_companies_and_vacancies_count()
            if companies:
                for company in companies:
                    print(f"{company['company']}: {company['vacancies_count']} вакансий")
            else:
                print("Нет данных о компаниях")

        elif choice == '2':
            print("\n--- Все вакансии ---")
            vacancies = db_manager.get_all_vacancies()
            if vacancies:
                for i, vacancy in enumerate(vacancies[:10], 1):
                    print(f"{i}. {vacancy['company']} - {vacancy['title']}")
                    print(f"   Зарплата: {vacancy['salary']}")
                    print(f"   Ссылка: {vacancy['url']}")
                    print()
                if len(vacancies) > 10:
                    print(f"... и еще {len(vacancies) - 10} вакансий")
            else:
                print("Нет данных о вакансиях")

        elif choice == '3':
            avg_salary = db_manager.get_avg_salary()
            print(f"\nСредняя зарплата по всем вакансиям: {avg_salary:,.2f} руб.")

        elif choice == '4':
            print("\n--- Вакансии с зарплатой выше средней ---")
            high_salary_vacancies = db_manager.get_vacancies_with_higher_salary()
            if high_salary_vacancies:
                for i, vacancy in enumerate(high_salary_vacancies[:10], 1):
                    print(f"{i}. {vacancy['company']} - {vacancy['title']}")
                    print(f"   Зарплата: {vacancy['salary']}")
                    print(f"   Ссылка: {vacancy['url']}")
                    print()
                if len(high_salary_vacancies) > 10:
                    print(f"... и еще {len(high_salary_vacancies) - 10} вакансий")
            else:
                print("Вакансий с зарплатой выше средней не найдено")

        elif choice == '5':
            keyword = input("Введите ключевое слово для поиска: ").strip()
            if keyword:
                print(f"\n--- Результаты поиска по слову '{keyword}' ---")
                found_vacancies = db_manager.get_vacancies_with_keyword(keyword)
                if found_vacancies:
                    for i, vacancy in enumerate(found_vacancies, 1):
                        print(f"{i}. {vacancy['company']} - {vacancy['title']}")
                        print(f"   Зарплата: {vacancy['salary']}")
                        print(f"   Ссылка: {vacancy['url']}")
                        print()
                else:
                    print(f"Вакансий с ключевым словом '{keyword}' не найдено")
            else:
                print("Ключевое слово не может быть пустым")

        elif choice == '6':
            print("\n--- Топ-10 вакансий по зарплате ---")
            top_vacancies = db_manager.get_top_vacancies_by_salary(10)
            if top_vacancies:
                for i, vacancy in enumerate(top_vacancies, 1):
                    print(f"{i}. {vacancy['company']} - {vacancy['title']}")
                    print(f"   Зарплата: {vacancy['salary']}")
                    print(f"   Ссылка: {vacancy['url']}")
                    print()
            else:
                print("Нет данных о вакансиях")

        else:
            print("Неверный выбор. Попробуйте снова.")


if __name__ == "__main__":
    main()