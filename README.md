# 🗃️ Система сбора и анализа вакансий с hh.ru

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-blue)](https://www.postgresql.org/)
[![HH API](https://img.shields.io/badge/HH.ru-API-green)](https://api.hh.ru/)

Система для автоматического сбора данных о вакансиях с платформы hh.ru, их сохранения в базу данных PostgreSQL и последующего анализа.

## 📋 Оглавление

- [Функциональности](#-функциональности)
- [Технологический стек](#-технологический-стек)
- [Установка и запуск](#-установка-и-запуск)
- [Структура проекта](#-структура-проекта)
- [Примеры использования](#-примеры-использования)
- [База данных](#-база-данных)
- [API методы](#-api-методы)

## 🚀 Функциональности

### 📊 Сбор данных
- Автоматический поиск компаний на hh.ru
- Получение подробной информации о работодателях
- Сбор вакансий с полной информацией (зарплата, требования, условия)

### 💾 Хранение данных
- Автоматическое создание базы данных PostgreSQL
- Оптимизированная схема таблиц с отношениями
- Массовая вставка данных с обработкой ошибок

### 📈 Анализ данных
- **Компании и вакансии**: список компаний с количеством вакансий
- **Все вакансии**: полная информация о вакансиях с фильтрацией
- **Анализ зарплат**: средняя зарплата, вакансии с зарплатой выше средней
- **Поиск**: вакансии по ключевым словам в названии

## 🛠️ Технологический стек

- **Python 3.8+** - основной язык программирования
- **PostgreSQL** - система управления базами данных
- **psycopg2** - адаптер PostgreSQL для Python
- **requests** - HTTP-запросы к API hh.ru
- **configparser** - работа с конфигурационными файлами

## 📦 Установка и запуск

### 1. Клонирование репозитория
```bash
git clone https://github.com/Sergey-Molchan/project3_data_base.git
cd project3_data_base
2. Создание виртуального окружения


python -m venv venv
source venv/bin/activate  # Linux/Mac
# или
venv\Scripts\activate     # Windows
3. Установка зависимостей


pip install -r requirements.txt
4. Настройка базы данных

Создайте файл config/database.ini:

ini
[postgresql]
host=localhost
database=hh_vacancies
user=your_username
password=your_password
port=5432
5. Запуск приложения


python src/main.py
📁 Структура проекта

text
project3_data_base/
├── config/
│   └── database.ini          # Конфигурация БД
├── src/
│   ├── main.py              # Основной скрипт
│   ├── database.py          # Класс для работы с БД
│   └── api.py               # Класс для работы с API hh.ru
├── requirements.txt         # Зависимости проекта
└── README.md               # Документация
🎯 Примеры использования

Запуск сбора данных

Программа автоматически:

Создаст базу данных и таблицы
Найдет 10 популярных компаний
Соберет данные о вакансиях
Сохранит всё в PostgreSQL
Интерактивное меню

После сбора данных доступно меню:

text
=== МЕНЮ АНАЛИЗА ВАКАНСИЙ ===
1. Показать компании и количество вакансий
2. Показать все вакансии
3. Показать среднюю зарплату
4. Показать вакансии с зарплатой выше средней
5. Поиск вакансий по ключевому слову
0. Выход
🗃️ База данных

Схема таблиц

Таблица employers

sql
CREATE TABLE employers (
    employer_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    url VARCHAR(255),
    description TEXT,
    open_vacancies INTEGER
)
Таблица vacancies

sql
CREATE TABLE vacancies (
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
🔌 API методы

Класс HHAPI

get_employer(employer_id) - получение данных о работодателе
get_vacancies_by_employer(employer_id) - получение вакансий работодателя
search_employers(keyword) - поиск работодателей по названию
Класс DatabaseManager

get_companies_and_vacancies_count() - компании и количество вакансий
get_all_vacancies() - все вакансии с информацией о компаниях
get_avg_salary() - средняя зарплата по вакансиям
get_vacancies_with_higher_salary() - вакансии с зарплатой выше средней
get_vacancies_with_keyword(keyword) - поиск вакансий по ключевому слову
👨‍💻 Разработчик

Сергей Молчан - GitHub
📄 Лицензия

Этот проект создан в учебных целях в рамках курса по базам данных.

⭐ Если проект был полезен, поставьте звезду на GitHub!