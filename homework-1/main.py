"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

import psycopg2


def read_csv_file(path: str) -> list:
    """
    Функция чтения файлов .cvs
    """
    data = []
    with open(path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for item in reader:
            data.append(item)
    return data


def write_db():
    """
    Функция записи данных в БД north
    """
    data_employees = read_csv_file('north_data/employees_data.csv')
    data_customers = read_csv_file('north_data/customers_data.csv')
    data_orders = read_csv_file('north_data/orders_data.csv')

    conn = psycopg2.connect(host="localhost", database="north", user="postgres", password="12345",
                            options="-c client_encoding=utf8")

    with conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM orders")
            cur.execute("DELETE FROM employees")
            cur.execute("DELETE FROM customers")

            for employ in data_employees:
                cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", (
                    employ["employee_id"], employ["first_name"], employ["last_name"], employ["title"],
                    employ["birth_date"], employ["notes"]))

            for customer in data_customers:
                cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", (
                    customer["customer_id"], customer["company_name"], customer["contact_name"]))

            for order in data_orders:
                cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", (
                    order["order_id"], order["customer_id"], order["employee_id"], order["order_date"],
                    order["ship_city"]))

    conn.close()


if __name__ == '__main__':
    write_db()
