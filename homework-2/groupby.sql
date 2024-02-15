-- Напишите запросы, которые выводят следующую информацию:
-- 1. заказы, отправленные в города, заканчивающиеся на 'burg'. Вывести без повторений две колонки (город, страна) (см. таблица orders, колонки ship_city, ship_country)
SELECT DISTINCT ship_city, ship_country
FROM orders
WHERE ship_city ILIKE '%burg';

-- 2. SELECT order_id, customer_id, freight, ship_country
FROM orders
WHERE ship_country ILIKE 'P%'
ORDER BY freight DESC
LIMIT 10;

-- 3. имя, фамилию и телефон сотрудников, у которых в данных отсутствует регион (см таблицу employees)
SELECT first_name, last_name, home_phone
FROM employees
WHERE region IS NULL;

-- 4. количество поставщиков (suppliers) в каждой из стран. Результат отсортировать по убыванию количества поставщиков в стране
SELECT country, COUNT(*) AS supplier_count
FROM suppliers
GROUP BY country
ORDER BY supplier_count DESC;

-- 5. суммарный вес заказов (в которых известен регион) по странам, но вывести только те результаты, где суммарный вес на страну больше 2750. Отсортировать по убыванию суммарного веса (см таблицу orders, колонки ship_region, ship_country, freight)
SELECT ship_country, SUM(freight) AS total_weight
FROM orders
WHERE ship_region IS NOT NULL
GROUP BY ship_country
HAVING SUM(freight) > 2750
ORDER BY total_weight DESC;

-- 6. страны, в которых зарегистрированы и заказчики (customers) и поставщики (suppliers) и работники (employees).
SELECT country
FROM (
    SELECT country FROM customers
    INTERSECT
    SELECT country FROM suppliers
    INTERSECT
    SELECT country FROM employees
) AS all_countries
GROUP BY country;

-- 7. страны, в которых зарегистрированы и заказчики (customers) и поставщики (suppliers), но не зарегистрированы работники (employees).
SELECT country
FROM (
    SELECT country FROM customers
    INTERSECT
    SELECT country FROM suppliers
) AS customer_supplier_countries
WHERE country NOT IN (SELECT country FROM employees);
