import time
from database import DatabaseManager
from api import HHAPI


def main():
    """Основная функция программы"""
    print("=== Система сбора и анализа вакансий с hh.ru ===")

    try:
        # 1. Инициализация менеджера БД
        print("1. Инициализация базы данных...")
        db_manager = DatabaseManager()

        # 2. Создание базы данных и таблиц
        print("2. Создание базы данных и таблиц...")
        db_manager.create_database()
        db_manager.create_tables()

        # 3. Инициализация API
        print("3. Инициализация API...")
        api = HHAPI()

        # 4. Используем заранее известные ID популярных компаний
        print("4. Загрузка данных популярных компаний...")
        popular_companies = {
            'Яндекс': 1740,
            'Сбер': 3529,
            'Тинькофф': 78638,
            'Ozon': 2180,
            'VK': 15478,
            'МТС': 3776,
            'Билайн': 49357,
            'Газпром': 39305,
            'Росатом': 64174,
            'СберТех': 906557
        }

        employer_ids = list(popular_companies.values())
        employers_data = []

        # 5. Получение подробных данных о работодателях
        print("5. Получение подробных данных о работодателях...")
        for company_name, employer_id in popular_companies.items():
            employer_detail = api.get_employer(employer_id)
            if employer_detail:
                employers_data.append(employer_detail)
                print(f"   Получены данные: {employer_detail['name']}")
            else:
                print(f"   ❌ Не удалось получить данные для {company_name}")
            time.sleep(0.2)

        if not employers_data:
            print("Не удалось получить данные о компаниях")
            return

        # 6. Сохранение данных работодателей
        print("6. Сохранение данных работодателей...")
        for employer in employers_data:
            db_manager.insert_employer(employer)
            print(f"   Сохранен работодатель: {employer['name']}")

        # 7. Получение вакансий
        print("7. Получение вакансий...")
        all_vacancies = []

        for employer_id in employer_ids:
            # Получаем название компании для красивого вывода
            company_name = next((name for name, id_ in popular_companies.items() if id_ == employer_id),
                                str(employer_id))
            print(f"   Получение вакансий для {company_name}...")

            # Получаем вакансии (без ограничения max_vacancies, так как метод его не принимает)
            vacancies = api.get_vacancies_by_employer(employer_id)

            # Ограничиваем количество вакансий вручную
            limited_vacancies = vacancies[:20]  # Берем первые 20 вакансий
            all_vacancies.extend(limited_vacancies)
            print(f"   Получено {len(limited_vacancies)} вакансий")
            time.sleep(0.5)

        # 8. Сохранение вакансий
        print("8. Сохранение вакансий...")
        for i, vacancy in enumerate(all_vacancies, 1):
            db_manager.insert_vacancy(vacancy)
            if i % 10 == 0:
                print(f"   Сохранено {i} вакансий")

        print(f"   Всего сохранено {len(all_vacancies)} вакансий")

        # 9. Запуск пользовательского меню
        print("\n=== Данные успешно загружены в базу данных ===")
        show_menu(db_manager)

    except Exception as e:
        print(f"Произошла ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Работа программы завершена")


def show_menu(db_manager):
    """Показать меню взаимодействия с пользователем"""
    while True:
        print("\n" + "=" * 50)
        print("МЕНЮ АНАЛИЗА ВАКАНСИЙ")
        print("=" * 50)
        print("1. Показать компании и количество вакансий")
        print("2. Показать все вакансии")
        print("3. Показать среднюю зарплату")
        print("4. Показать вакансии с зарплатой выше средней")
        print("5. Поиск вакансий по ключевому слову")
        print("0. Выход")
        print("=" * 50)

        choice = input("Выберите опцию (0-5): ").strip()

        if choice == "1":
            show_companies_and_vacancies_count(db_manager)
        elif choice == "2":
            show_all_vacancies(db_manager)
        elif choice == "3":
            show_avg_salary(db_manager)
        elif choice == "4":
            show_vacancies_with_higher_salary(db_manager)
        elif choice == "5":
            search_vacancies_by_keyword(db_manager)
        elif choice == "0":
            print("Выход из программы...")
            break
        else:
            print("❌ Неверный выбор. Попробуйте еще раз.")

        input("\nНажмите Enter для продолжения...")


def show_companies_and_vacancies_count(db_manager):
    """Показать компании и количество вакансий"""
    print("\n" + "=" * 60)
    print("КОМПАНИИ И КОЛИЧЕСТВО ВАКАНСИЙ")
    print("=" * 60)

    companies = db_manager.get_companies_and_vacancies_count()

    if not companies:
        print("Нет данных о компаниях")
        return

    for i, (company, count) in enumerate(companies, 1):
        print(f"{i:2d}. {company:<30} - {count:3d} вакансий")


def show_all_vacancies(db_manager):
    """Показать все вакансии"""
    print("\n" + "=" * 80)
    print("ВСЕ ВАКАНСИИ")
    print("=" * 80)

    vacancies = db_manager.get_all_vacancies()

    if not vacancies:
        print("Нет данных о вакансиях")
        return

    for i, (company, title, salary_from, salary_to, currency, url) in enumerate(vacancies, 1):
        if salary_from or salary_to:
            salary_info = f"{salary_from or '?'}-{salary_to or '?'} {currency}"
        else:
            salary_info = "зарплата не указана"

        print(f"{i:2d}. {company}")
        print(f"    Должность: {title}")
        print(f"    Зарплата: {salary_info}")
        print(f"    Ссылка: {url}")
        print("-" * 80)


def show_avg_salary(db_manager):
    """Показать среднюю зарплату"""
    print("\n" + "=" * 40)
    print("СРЕДНЯЯ ЗАРПЛАТА ПО ВАКАНСИЯМ")
    print("=" * 40)

    avg_salary = db_manager.get_avg_salary()

    if avg_salary:
        print(f"Средняя зарплата: {avg_salary:,.0f} руб.")
    else:
        print("Недостаточно данных для расчета средней зарплаты")


def show_vacancies_with_higher_salary(db_manager):
    """Показать вакансии с зарплатой выше средней"""
    print("\n" + "=" * 80)
    print("ВАКАНСИИ С ЗАРПЛАТОЙ ВЫШЕ СРЕДНЕЙ")
    print("=" * 80)

    vacancies = db_manager.get_vacancies_with_higher_salary()

    if not vacancies:
        print("Нет вакансий с зарплатой выше средней")
        return

    for i, (company, title, salary_from, salary_to, currency, url) in enumerate(vacancies, 1):
        avg_salary = (salary_from + salary_to) / 2 if salary_from and salary_to else salary_from or salary_to
        print(f"{i:2d}. {company}")
        print(f"    Должность: {title}")
        print(f"    Зарплата: {salary_from or '?'}-{salary_to or '?'} {currency}")
        print(f"    Средняя: {avg_salary:,.0f} {currency}")
        print(f"    Ссылка: {url}")
        print("-" * 80)


def search_vacancies_by_keyword(db_manager):
    """Поиск вакансий по ключевому слову"""
    print("\n" + "=" * 60)
    print("ПОИСК ВАКАНСИЙ ПО КЛЮЧЕВОМУ СЛОВу")
    print("=" * 60)

    keyword = input("Введите ключевое слово для поиска: ").strip()

    if not keyword:
        print("❌ Не введено ключевое слово")
        return

    vacancies = db_manager.get_vacancies_with_keyword(keyword)

    if not vacancies:
        print(f"❌ Вакансии по запросу '{keyword}' не найдены")
        return

    print(f"\nНайдено {len(vacancies)} вакансий по запросу '{keyword}':")
    print("=" * 80)

    for i, (company, title, salary_from, salary_to, currency, url) in enumerate(vacancies, 1):
        if salary_from or salary_to:
            salary_info = f"{salary_from or '?'}-{salary_to or '?'} {currency}"
        else:
            salary_info = "зарплата не указана"

        print(f"{i:2d}. {company}")
        print(f"    Должность: {title}")
        print(f"    Зарплата: {salary_info}")
        print(f"    Ссылка: {url}")
        print("-" * 80)


if __name__ == "__main__":
    main()