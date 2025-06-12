"""End-to-end tests for text-to-SQL functionality using MySQL with synthetic data."""

import os
import pytest
import sqlite3
from typing import Dict, List
from unittest.mock import patch, MagicMock

from app.modules.database_connection.models import DatabaseConnection
from app.modules.prompt.models import Prompt
from app.modules.sql_generation.models import SQLGeneration, LLMConfig
from app.modules.sql_generation.services import SQLGenerationService
from app.api.requests import SQLGenerationRequest


class TestTextToSQLMySQLEndToEnd:
    """End-to-end tests for text-to-SQL functionality with MySQL and synthetic data."""

    @pytest.fixture(scope="class")
    def test_db_path(self, tmp_path_factory):
        """Create a temporary SQLite database for testing (simulating MySQL structure)."""
        db_path = tmp_path_factory.mktemp("e2e_test") / "ecommerce_mysql_test.db"
        return str(db_path)

    @pytest.fixture(scope="class") 
    def synthetic_database(self, test_db_path):
        """Create and populate a synthetic e-commerce database with realistic data."""
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()

        # Create tables with MySQL-compatible syntax
        cursor.executescript("""
            -- Customers table
            CREATE TABLE customers (
                customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                phone VARCHAR(20),
                city VARCHAR(100),
                state VARCHAR(50),
                country VARCHAR(100),
                registration_date DATE,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Products table
            CREATE TABLE products (
                product_id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_name VARCHAR(200) NOT NULL,
                category VARCHAR(100) NOT NULL,
                brand VARCHAR(100),
                price DECIMAL(10,2) NOT NULL,
                cost DECIMAL(10,2) NOT NULL,
                stock_quantity INTEGER DEFAULT 0,
                is_active BOOLEAN DEFAULT 1,
                created_date DATE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Orders table
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                order_date DATE NOT NULL,
                status VARCHAR(50) NOT NULL,
                total_amount DECIMAL(10,2) NOT NULL,
                shipping_address TEXT,
                payment_method VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            );

            -- Order items table
            CREATE TABLE order_items (
                order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price DECIMAL(10,2) NOT NULL,
                total_price DECIMAL(10,2) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (order_id) REFERENCES orders(order_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            );

            -- Reviews table
            CREATE TABLE reviews (
                review_id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                customer_id INTEGER NOT NULL,
                rating INTEGER CHECK(rating >= 1 AND rating <= 5),
                review_text TEXT,
                review_date DATE,
                helpful_votes INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(product_id),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            );

            -- Sales summary table (for analytics)
            CREATE TABLE sales_summary (
                summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
                summary_date DATE NOT NULL,
                total_orders INTEGER DEFAULT 0,
                total_revenue DECIMAL(12,2) DEFAULT 0,
                avg_order_value DECIMAL(10,2) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Insert realistic synthetic data
        # Customers (more diverse data)
        customers_data = [
            (1, "John", "Smith", "john.smith@email.com", "555-0101", "New York", "NY", "USA", "2023-01-15", 1),
            (2, "Sarah", "Johnson", "sarah.j@email.com", "555-0102", "Los Angeles", "CA", "USA", "2023-02-20", 1),
            (3, "Michael", "Brown", "m.brown@email.com", "555-0103", "Chicago", "IL", "USA", "2023-03-10", 1),
            (4, "Emily", "Davis", "emily.davis@email.com", "555-0104", "Houston", "TX", "USA", "2023-04-05", 1),
            (5, "David", "Wilson", "d.wilson@email.com", "555-0105", "Phoenix", "AZ", "USA", "2023-05-12", 1),
            (6, "Lisa", "Garcia", "lisa.garcia@email.com", "555-0106", "Philadelphia", "PA", "USA", "2023-06-18", 1),
            (7, "James", "Miller", "james.miller@email.com", "555-0107", "San Antonio", "TX", "USA", "2023-07-22", 1),
            (8, "Jennifer", "Martinez", "j.martinez@email.com", "555-0108", "San Diego", "CA", "USA", "2023-08-30", 0),
            (9, "Robert", "Anderson", "r.anderson@email.com", "555-0109", "Dallas", "TX", "USA", "2023-09-14", 1),
            (10, "Ashley", "Taylor", "ashley.t@email.com", "555-0110", "San Jose", "CA", "USA", "2023-10-01", 1),
            (11, "Christopher", "Thomas", "chris.thomas@email.com", "555-0111", "Austin", "TX", "USA", "2023-11-05", 1),
            (12, "Amanda", "Jackson", "amanda.j@email.com", "555-0112", "Jacksonville", "FL", "USA", "2023-11-10", 1),
        ]
        cursor.executemany(
            "INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, state, country, registration_date, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            customers_data
        )

        # Products (more categories and realistic pricing)
        products_data = [
            (1, "Wireless Bluetooth Headphones", "Electronics", "TechBrand", 99.99, 45.00, 150, 1, "2023-01-01"),
            (2, "Smartphone Case", "Electronics", "ProtectCo", 24.99, 8.50, 300, 1, "2023-01-01"),
            (3, "Running Shoes", "Sports", "SportMax", 89.99, 35.00, 75, 1, "2023-01-15"),
            (4, "Coffee Maker", "Home & Kitchen", "BrewMaster", 149.99, 65.00, 50, 1, "2023-02-01"),
            (5, "Yoga Mat", "Sports", "FitLife", 29.99, 12.00, 200, 1, "2023-02-15"),
            (6, "Laptop Stand", "Electronics", "DeskPro", 79.99, 25.00, 80, 1, "2023-03-01"),
            (7, "Water Bottle", "Sports", "HydroMax", 19.99, 7.50, 400, 1, "2023-03-15"),
            (8, "Desk Lamp", "Home & Kitchen", "LightCorp", 45.99, 18.00, 120, 1, "2023-04-01"),
            (9, "Gaming Mouse", "Electronics", "GameTech", 59.99, 22.00, 90, 1, "2023-04-15"),
            (10, "Kitchen Knife Set", "Home & Kitchen", "ChefTools", 129.99, 55.00, 40, 1, "2023-05-01"),
            (11, "Fitness Tracker", "Electronics", "HealthTech", 199.99, 80.00, 60, 1, "2023-06-01"),
            (12, "Camping Tent", "Outdoor", "AdventureGear", 299.99, 120.00, 25, 1, "2023-06-15"),
            (13, "Cookware Set", "Home & Kitchen", "KitchenPro", 179.99, 75.00, 35, 1, "2023-07-01"),
            (14, "Basketball", "Sports", "SportMax", 39.99, 15.00, 100, 1, "2023-07-15"),
            (15, "Backpack", "Outdoor", "TravelGear", 69.99, 28.00, 85, 1, "2023-08-01"),
        ]
        cursor.executemany(
            "INSERT INTO products (product_id, product_name, category, brand, price, cost, stock_quantity, is_active, created_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            products_data
        )

        # Orders (spanning multiple months with various statuses)
        orders_data = [
            (1, 1, "2023-10-01", "completed", 124.98, "123 Main St, New York, NY", "credit_card"),
            (2, 2, "2023-10-02", "completed", 89.99, "456 Oak Ave, Los Angeles, CA", "paypal"),
            (3, 3, "2023-10-03", "completed", 179.98, "789 Pine St, Chicago, IL", "credit_card"),
            (4, 1, "2023-10-15", "completed", 29.99, "123 Main St, New York, NY", "debit_card"),
            (5, 4, "2023-10-20", "shipped", 149.99, "321 Elm St, Houston, TX", "credit_card"),
            (6, 5, "2023-11-01", "completed", 259.98, "654 Maple Dr, Phoenix, AZ", "paypal"),
            (7, 2, "2023-11-03", "cancelled", 45.99, "456 Oak Ave, Los Angeles, CA", "credit_card"),
            (8, 6, "2023-11-05", "completed", 199.98, "987 Cedar Ln, Philadelphia, PA", "credit_card"),
            (9, 7, "2023-11-08", "processing", 79.99, "147 Birch St, San Antonio, TX", "debit_card"),
            (10, 3, "2023-11-10", "completed", 19.99, "789 Pine St, Chicago, IL", "paypal"),
            (11, 8, "2023-11-12", "completed", 299.99, "555 Sunset Blvd, San Diego, CA", "credit_card"),
            (12, 9, "2023-11-15", "completed", 119.98, "777 Main St, Dallas, TX", "paypal"),
            (13, 10, "2023-11-18", "shipped", 69.99, "888 Oak St, San Jose, CA", "credit_card"),
            (14, 11, "2023-11-20", "completed", 339.98, "999 Pine Ave, Austin, TX", "debit_card"),
            (15, 12, "2023-11-22", "processing", 179.99, "111 Elm Dr, Jacksonville, FL", "credit_card"),
        ]
        cursor.executemany(
            "INSERT INTO orders (order_id, customer_id, order_date, status, total_amount, shipping_address, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?)",
            orders_data
        )

        # Order items (realistic quantities and pricing)
        order_items_data = [
            (1, 1, 1, 1, 99.99, 99.99),    # Headphones
            (2, 1, 2, 1, 24.99, 24.99),    # Phone case
            (3, 2, 3, 1, 89.99, 89.99),    # Running shoes
            (4, 3, 4, 1, 149.99, 149.99),  # Coffee maker
            (5, 3, 5, 1, 29.99, 29.99),    # Yoga mat
            (6, 4, 5, 1, 29.99, 29.99),    # Yoga mat
            (7, 5, 4, 1, 149.99, 149.99),  # Coffee maker
            (8, 6, 11, 1, 199.99, 199.99), # Fitness tracker
            (9, 6, 9, 1, 59.99, 59.99),    # Gaming mouse
            (10, 7, 8, 1, 45.99, 45.99),   # Desk lamp
            (11, 8, 1, 2, 99.99, 199.98),  # 2x Headphones
            (12, 9, 6, 1, 79.99, 79.99),   # Laptop stand
            (13, 10, 7, 1, 19.99, 19.99),  # Water bottle
            (14, 11, 12, 1, 299.99, 299.99), # Camping tent
            (15, 12, 14, 3, 39.99, 119.97), # 3x Basketball
            (16, 13, 15, 1, 69.99, 69.99),  # Backpack
            (17, 14, 11, 1, 199.99, 199.99), # Fitness tracker
            (18, 14, 13, 1, 179.99, 179.99), # Cookware set
            (19, 15, 13, 1, 179.99, 179.99), # Cookware set
        ]
        cursor.executemany(
            "INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, total_price) VALUES (?, ?, ?, ?, ?, ?)",
            order_items_data
        )

        # Reviews (varied ratings and helpful content)
        reviews_data = [
            (1, 1, 1, 5, "Excellent sound quality and comfort! Best headphones I've owned.", "2023-10-05", 15),
            (2, 3, 2, 4, "Great shoes for running, very comfortable. Good value for money.", "2023-10-08", 8),
            (3, 4, 4, 5, "Perfect coffee maker, makes great coffee every time. Highly recommend!", "2023-10-25", 22),
            (4, 1, 6, 4, "Good headphones for the price. Sound quality is solid.", "2023-11-08", 5),
            (5, 5, 1, 5, "Love this yoga mat, great quality and non-slip surface!", "2023-11-12", 12),
            (6, 9, 5, 3, "Mouse is okay, nothing special. Expected more features.", "2023-11-18", 3),
            (7, 2, 3, 5, "Perfect phone case, saved my phone from multiple drops!", "2023-11-20", 18),
            (8, 7, 10, 4, "Good water bottle, keeps drinks cold for hours.", "2023-11-23", 7),
            (9, 11, 8, 5, "Amazing fitness tracker! Accurate and great battery life.", "2023-11-25", 25),
            (10, 12, 9, 2, "Tent quality is disappointing. Not as waterproof as advertised.", "2023-11-28", 4),
            (11, 13, 11, 4, "Cookware set is good quality. Non-stick coating works well.", "2023-12-01", 9),
            (12, 14, 12, 5, "Best basketball for outdoor play. Great grip and durability.", "2023-12-03", 14),
        ]
        cursor.executemany(
            "INSERT INTO reviews (review_id, product_id, customer_id, rating, review_text, review_date, helpful_votes) VALUES (?, ?, ?, ?, ?, ?, ?)",
            reviews_data
        )

        # Sales summary data (for business intelligence queries)
        summary_data = [
            (1, "2023-10-01", 4, 389.95, 97.49),
            (2, "2023-11-01", 11, 1459.88, 132.72),
        ]
        cursor.executemany(
            "INSERT INTO sales_summary (summary_id, summary_date, total_orders, total_revenue, avg_order_value) VALUES (?, ?, ?, ?, ?)",
            summary_data
        )

        conn.commit()
        conn.close()
        return test_db_path

    @pytest.fixture
    def storage_mock(self):
        """Mock storage for tests."""
        mock = MagicMock()
        mock.insert = MagicMock(return_value=None)
        mock.update = MagicMock(return_value=None) 
        mock.find_by = MagicMock(return_value=[])
        mock.find_by_id = MagicMock(return_value=None)
        mock.delete_collection = MagicMock(return_value=None)
        mock._get_existing_collections = MagicMock(return_value=[])
        return mock

    @pytest.fixture
    def db_connection(self, synthetic_database):
        """Create a MySQL database connection for the synthetic database."""
        return DatabaseConnection(
            id="test-mysql-conn",
            alias="ecommerce_mysql_test",
            dialect="mysql",
            connection_uri="mysql://testuser:testpass@localhost:3306/ecommerce_test",
            schemas=["ecommerce_test"],
            metadata={"test": True, "local_db_path": synthetic_database}
        )

    @pytest.fixture
    def test_prompts(self) -> List[Dict]:
        """Advanced test prompts for comprehensive SQL generation scenarios."""
        return [
            {
                "text": "How many active customers do we have?",
                "expected_tables": ["customers"],
                "expected_sql_contains": ["COUNT", "customers", "is_active"],
                "category": "basic_aggregation"
            },
            {
                "text": "What are the top 5 products by total sales revenue?",
                "expected_tables": ["products", "order_items"],
                "expected_sql_contains": ["SUM", "order_items", "products", "GROUP BY", "ORDER BY", "LIMIT 5"],
                "category": "complex_aggregation"
            },
            {
                "text": "Show me customers from Texas with their total order amounts",
                "expected_tables": ["customers", "orders"],
                "expected_sql_contains": ["customers", "orders", "TX", "Texas", "SUM", "JOIN"],
                "category": "join_with_filter"
            },
            {
                "text": "Which products have an average rating above 4.0?",
                "expected_tables": ["products", "reviews"],
                "expected_sql_contains": ["AVG", "rating", "products", "reviews", "HAVING", "> 4"],
                "category": "aggregation_with_having"
            },
            {
                "text": "List all completed orders from November 2023 with customer information",
                "expected_tables": ["orders", "customers"],
                "expected_sql_contains": ["orders", "customers", "2023-11", "completed", "JOIN"],
                "category": "date_filter_join"
            },
            {
                "text": "What is the total revenue and average order value for each product category?",
                "expected_tables": ["products", "order_items", "orders"],
                "expected_sql_contains": ["SUM", "AVG", "category", "GROUP BY", "products", "order_items"],
                "category": "multiple_aggregations"
            },
            {
                "text": "Find customers who have placed more than 2 orders",
                "expected_tables": ["customers", "orders"],
                "expected_sql_contains": ["customers", "orders", "COUNT", "GROUP BY", "HAVING", "> 2"],
                "category": "having_with_count"
            },
            {
                "text": "Show the monthly sales trend for 2023",
                "expected_tables": ["orders"],
                "expected_sql_contains": ["SUM", "orders", "GROUP BY", "2023", "order_date"],
                "category": "time_series"
            },
            {
                "text": "Which brands have the highest average product rating?",
                "expected_tables": ["products", "reviews"],
                "expected_sql_contains": ["AVG", "rating", "brand", "products", "reviews", "GROUP BY", "ORDER BY"],
                "category": "brand_analysis"
            },
            {
                "text": "Get order details with product names and customer names for orders over $200",
                "expected_tables": ["orders", "customers", "order_items", "products"],
                "expected_sql_contains": ["orders", "customers", "order_items", "products", "JOIN", "> 200", "total_amount"],
                "category": "multi_table_join"
            }
        ]

    @pytest.fixture
    def mock_llm_response(self):
        """Mock LLM response for SQL generation with MySQL syntax."""
        def _mock_response(prompt_text: str) -> str:
            """Generate mock MySQL SQL based on prompt text."""
            prompt_lower = prompt_text.lower()
            
            if "how many active customers" in prompt_lower:
                return "SELECT COUNT(*) as active_customer_count FROM customers WHERE is_active = 1;"
                
            elif "top 5 products by total sales" in prompt_lower:
                return """
                SELECT 
                    p.product_name, 
                    SUM(oi.total_price) as total_revenue
                FROM products p
                INNER JOIN order_items oi ON p.product_id = oi.product_id
                GROUP BY p.product_id, p.product_name
                ORDER BY total_revenue DESC
                LIMIT 5;
                """
                
            elif "customers from texas" in prompt_lower:
                return """
                SELECT 
                    c.customer_id,
                    c.first_name, 
                    c.last_name, 
                    c.email, 
                    c.state, 
                    COALESCE(SUM(o.total_amount), 0) as total_order_amount
                FROM customers c
                LEFT JOIN orders o ON c.customer_id = o.customer_id
                WHERE c.state = 'TX'
                GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.state;
                """
                
            elif "average rating above 4" in prompt_lower:
                return """
                SELECT 
                    p.product_name, 
                    AVG(r.rating) as avg_rating,
                    COUNT(r.review_id) as review_count
                FROM products p
                INNER JOIN reviews r ON p.product_id = r.product_id
                GROUP BY p.product_id, p.product_name
                HAVING AVG(r.rating) > 4.0
                ORDER BY avg_rating DESC;
                """
                
            elif "completed orders from november 2023" in prompt_lower:
                return """
                SELECT 
                    o.order_id, 
                    o.order_date, 
                    o.total_amount, 
                    o.status,
                    c.first_name, 
                    c.last_name, 
                    c.email,
                    c.city,
                    c.state
                FROM orders o
                INNER JOIN customers c ON o.customer_id = c.customer_id
                WHERE o.order_date >= '2023-11-01' 
                    AND o.order_date < '2023-12-01'
                    AND o.status = 'completed'
                ORDER BY o.order_date DESC;
                """
                
            elif "total revenue and average order value for each product category" in prompt_lower:
                return """
                SELECT 
                    p.category,
                    SUM(oi.total_price) as total_revenue,
                    AVG(oi.total_price) as avg_order_value,
                    COUNT(DISTINCT oi.order_id) as order_count
                FROM products p
                INNER JOIN order_items oi ON p.product_id = oi.product_id
                INNER JOIN orders o ON oi.order_id = o.order_id
                WHERE o.status = 'completed'
                GROUP BY p.category
                ORDER BY total_revenue DESC;
                """
                
            elif "customers who have placed more than 2 orders" in prompt_lower:
                return """
                SELECT 
                    c.customer_id,
                    c.first_name,
                    c.last_name,
                    c.email,
                    COUNT(o.order_id) as order_count,
                    SUM(o.total_amount) as total_spent
                FROM customers c
                INNER JOIN orders o ON c.customer_id = o.customer_id
                GROUP BY c.customer_id, c.first_name, c.last_name, c.email
                HAVING COUNT(o.order_id) > 2
                ORDER BY order_count DESC;
                """
                
            elif "monthly sales trend for 2023" in prompt_lower:
                return """
                SELECT 
                    DATE_FORMAT(order_date, '%Y-%m') as month,
                    SUM(total_amount) as monthly_revenue,
                    COUNT(*) as order_count,
                    AVG(total_amount) as avg_order_value
                FROM orders
                WHERE YEAR(order_date) = 2023
                    AND status = 'completed'
                GROUP BY DATE_FORMAT(order_date, '%Y-%m')
                ORDER BY month;
                """
                
            elif "brands have the highest average product rating" in prompt_lower:
                return """
                SELECT 
                    p.brand,
                    AVG(r.rating) as avg_rating,
                    COUNT(r.review_id) as review_count,
                    COUNT(DISTINCT p.product_id) as product_count
                FROM products p
                INNER JOIN reviews r ON p.product_id = r.product_id
                GROUP BY p.brand
                HAVING COUNT(r.review_id) >= 2
                ORDER BY avg_rating DESC, review_count DESC;
                """
                
            elif "order details with product names and customer names for orders over" in prompt_lower:
                return """
                SELECT 
                    o.order_id,
                    o.order_date,
                    o.total_amount,
                    o.status,
                    c.first_name,
                    c.last_name,
                    c.email,
                    p.product_name,
                    p.category,
                    oi.quantity,
                    oi.unit_price,
                    oi.total_price
                FROM orders o
                INNER JOIN customers c ON o.customer_id = c.customer_id
                INNER JOIN order_items oi ON o.order_id = oi.order_id
                INNER JOIN products p ON oi.product_id = p.product_id
                WHERE o.total_amount > 200
                ORDER BY o.total_amount DESC, o.order_date DESC;
                """
            else:
                return "SELECT 1 as test_query;"  # Fallback SQL
                
        return _mock_response

    def test_database_setup_and_schema_validation(self, synthetic_database, db_connection):
        """Test that the synthetic database is properly set up with correct schema."""
        # Verify database file exists
        assert os.path.exists(synthetic_database)
        
        # Test connection and schema
        conn = sqlite3.connect(synthetic_database)
        cursor = conn.cursor()
        
        # Verify all expected tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        expected_tables = ['customers', 'products', 'orders', 'order_items', 'reviews', 'sales_summary']
        
        for table in expected_tables:
            assert table in tables, f"Table {table} not found in database"
        
        # Verify data integrity and counts
        cursor.execute("SELECT COUNT(*) FROM customers")
        customer_count = cursor.fetchone()[0]
        assert customer_count == 12, f"Expected 12 customers, got {customer_count}"
        
        cursor.execute("SELECT COUNT(*) FROM products")
        product_count = cursor.fetchone()[0]
        assert product_count == 15, f"Expected 15 products, got {product_count}"
        
        cursor.execute("SELECT COUNT(*) FROM orders")
        order_count = cursor.fetchone()[0]
        assert order_count == 15, f"Expected 15 orders, got {order_count}"
        
        # Verify foreign key relationships
        cursor.execute("""
            SELECT COUNT(*) FROM orders o 
            LEFT JOIN customers c ON o.customer_id = c.customer_id 
            WHERE c.customer_id IS NULL
        """)
        orphaned_orders = cursor.fetchone()[0]
        assert orphaned_orders == 0, "Found orders without valid customers"
        
        conn.close()

    def test_realistic_sql_queries_execution(self, synthetic_database):
        """Test that realistic business intelligence queries execute correctly."""
        conn = sqlite3.connect(synthetic_database)
        cursor = conn.cursor()
        
        # Test complex analytical queries that the system should generate
        business_queries = [
            {
                "name": "Revenue by Category",
                "sql": """
                    SELECT 
                        p.category,
                        SUM(oi.total_price) as total_revenue,
                        COUNT(DISTINCT o.customer_id) as unique_customers
                    FROM products p
                    JOIN order_items oi ON p.product_id = oi.product_id
                    JOIN orders o ON oi.order_id = o.order_id
                    WHERE o.status = 'completed'
                    GROUP BY p.category
                    ORDER BY total_revenue DESC
                """,
                "expected_rows": lambda rows: len(rows) > 0 and rows[0][1] > 0
            },
            {
                "name": "Customer Lifetime Value",
                "sql": """
                    SELECT 
                        c.customer_id,
                        c.first_name,
                        c.last_name,
                        COUNT(o.order_id) as total_orders,
                        SUM(o.total_amount) as lifetime_value,
                        AVG(o.total_amount) as avg_order_value
                    FROM customers c
                    LEFT JOIN orders o ON c.customer_id = o.customer_id
                    GROUP BY c.customer_id, c.first_name, c.last_name
                    HAVING total_orders > 0
                    ORDER BY lifetime_value DESC
                """,
                "expected_rows": lambda rows: len(rows) >= 5  # Should have customers with orders
            },
            {
                "name": "Product Performance Analysis",
                "sql": """
                    SELECT 
                        p.product_name,
                        p.category,
                        p.price,
                        COALESCE(SUM(oi.quantity), 0) as total_sold,
                        COALESCE(SUM(oi.total_price), 0) as revenue,
                        COALESCE(AVG(r.rating), 0) as avg_rating,
                        COUNT(r.review_id) as review_count
                    FROM products p
                    LEFT JOIN order_items oi ON p.product_id = oi.product_id
                    LEFT JOIN reviews r ON p.product_id = r.product_id
                    GROUP BY p.product_id, p.product_name, p.category, p.price
                    ORDER BY revenue DESC
                """,
                "expected_rows": lambda rows: len(rows) == 15  # All products should be included
            },
            {
                "name": "Monthly Revenue Trend",
                "sql": """
                    SELECT 
                        strftime('%Y-%m', order_date) as month,
                        COUNT(*) as order_count,
                        SUM(total_amount) as monthly_revenue,
                        AVG(total_amount) as avg_order_value
                    FROM orders
                    WHERE status IN ('completed', 'shipped')
                    GROUP BY strftime('%Y-%m', order_date)
                    ORDER BY month
                """,
                "expected_rows": lambda rows: len(rows) >= 2  # Should have data for multiple months
            }
        ]
        
        for query_info in business_queries:
            try:
                cursor.execute(query_info["sql"])
                results = cursor.fetchall()
                
                # Validate results
                assert query_info["expected_rows"](results), f"Query '{query_info['name']}' returned unexpected results"
                
                # Ensure no SQL injection or malformed queries
                assert len(results) >= 0, f"Query '{query_info['name']}' should return results"
                
            except Exception as e:
                pytest.fail(f"Business query '{query_info['name']}' failed: {str(e)}")
        
        conn.close()

    @patch('app.modules.sql_generation.services.SQLAgent')
    @patch('app.modules.prompt.repositories.PromptRepository')
    @patch('app.modules.database_connection.repositories.DatabaseConnectionRepository')
    def test_end_to_end_sql_generation_workflow(
        self,
        mock_db_repo,
        mock_prompt_repo,
        mock_sql_agent,
        storage_mock,
        db_connection,
        synthetic_database,
        test_prompts,
        mock_llm_response
    ):
        """Test the complete end-to-end SQL generation workflow with realistic scenarios."""
        
        # Setup mocks for the complete workflow
        def mock_generate_response(user_prompt, database_connection, metadata=None):
            sql = mock_llm_response(user_prompt.text)
            return SQLGeneration(
                prompt_id=user_prompt.id,
                sql=sql,
                status="VALID",
                input_tokens_used=150,
                output_tokens_used=75,
                llm_config=LLMConfig(model_name="gpt-4o-mini"),
                metadata=metadata or {}
            )
        
        mock_sql_agent.return_value.generate_response.side_effect = mock_generate_response
        mock_db_repo.return_value.find_by_id.return_value = db_connection
        
        # Test each category of prompts
        categories_tested = set()
        
        for i, prompt_data in enumerate(test_prompts):
            # Create test prompt
            prompt = Prompt(
                id=f"test-prompt-{i}",
                text=prompt_data["text"],
                db_connection_id=db_connection.id,
                metadata={"category": prompt_data["category"], "test": True}
            )
            
            mock_prompt_repo.return_value.find_by_id.return_value = prompt
            
            # Create SQL generation service
            service = SQLGenerationService(storage_mock)
            
            # Create request
            request = SQLGenerationRequest(
                llm_config=LLMConfig(model_name="gpt-4o-mini"),
                evaluate=False,
                metadata={"test": True, "category": prompt_data["category"]}
            )
            
            # Generate SQL
            result = service.create_sql_generation(prompt.id, request)
            
            # Validate basic result structure
            assert result is not None, f"No result for prompt: {prompt_data['text']}"
            assert result.sql is not None, f"No SQL generated for prompt: {prompt_data['text']}"
            assert result.status == "VALID", f"Invalid SQL status for prompt: {prompt_data['text']}"
            assert result.prompt_id == prompt.id
            
            # Test SQL execution against synthetic database
            conn = sqlite3.connect(synthetic_database)
            cursor = conn.cursor()
            
            try:
                # Clean and execute the generated SQL
                clean_sql = " ".join(result.sql.strip().split())
                cursor.execute(clean_sql)
                results = cursor.fetchall()
                
                # Category-specific validations
                if prompt_data["category"] == "basic_aggregation":
                    assert len(results) == 1, "Count queries should return single result"
                    assert isinstance(results[0][0], int), "Count should return integer"
                    
                elif prompt_data["category"] == "complex_aggregation":
                    assert len(results) <= 5, "Top 5 queries should return at most 5 results"
                    if len(results) > 1:
                        # Results should be ordered (descending revenue)
                        assert results[0][1] >= results[1][1], "Results should be ordered by revenue"
                        
                elif prompt_data["category"] == "join_with_filter":
                    # Should return customers from Texas
                    assert len(results) >= 0, "Should handle Texas filter correctly"
                    
                elif prompt_data["category"] == "aggregation_with_having":
                    # Should only return products with rating > 4
                    for row in results:
                        if len(row) > 1:  # Has rating column
                            assert row[1] > 4.0, f"Rating should be > 4.0, got {row[1]}"
                
                # Track categories tested
                categories_tested.add(prompt_data["category"])
                
            except Exception as e:
                pytest.fail(f"Generated SQL failed for prompt '{prompt_data['text']}' (category: {prompt_data['category']}): {str(e)}\nSQL: {result.sql}")
            finally:
                conn.close()
        
        # Ensure we tested all categories
        expected_categories = {prompt["category"] for prompt in test_prompts}
        assert categories_tested == expected_categories, f"Not all categories tested. Missing: {expected_categories - categories_tested}"

    def test_sql_generation_error_handling(self, storage_mock, db_connection):
        """Test error handling in SQL generation workflow."""
        service = SQLGenerationService(storage_mock)
        
        # Test with non-existent prompt
        storage_mock.find_by_id.return_value = None
        
        request = SQLGenerationRequest(
            llm_config=LLMConfig(model_name="gpt-4o-mini"),
            evaluate=False
        )
        
        with pytest.raises(Exception):  # Should raise HTTPException
            service.create_sql_generation("non-existent-prompt", request)

    def test_performance_benchmarks(self, synthetic_database):
        """Test performance characteristics of generated SQL."""
        conn = sqlite3.connect(synthetic_database)
        cursor = conn.cursor()
        
        # Add indexes for performance testing
        performance_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_customers_state ON customers(state)",
            "CREATE INDEX IF NOT EXISTS idx_orders_date_status ON orders(order_date, status)",
            "CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id)",
            "CREATE INDEX IF NOT EXISTS idx_reviews_product_rating ON reviews(product_id, rating)"
        ]
        
        for index_sql in performance_indexes:
            cursor.execute(index_sql)
        
        # Test query performance
        import time
        
        performance_queries = [
            {
                "name": "Simple Aggregation",
                "sql": "SELECT COUNT(*) FROM customers WHERE is_active = 1",
                "max_time": 0.1
            },
            {
                "name": "Complex Join with Aggregation",
                "sql": """
                    SELECT p.category, SUM(oi.total_price) as revenue
                    FROM products p
                    JOIN order_items oi ON p.product_id = oi.product_id
                    JOIN orders o ON oi.order_id = o.order_id
                    WHERE o.status = 'completed'
                    GROUP BY p.category
                    ORDER BY revenue DESC
                """,
                "max_time": 0.5
            }
        ]
        
        for query_info in performance_queries:
            start_time = time.time()
            cursor.execute(query_info["sql"])
            results = cursor.fetchall()
            execution_time = time.time() - start_time
            
            assert execution_time < query_info["max_time"], f"Query '{query_info['name']}' took too long: {execution_time}s"
            assert len(results) >= 0, f"Query '{query_info['name']}' should return results"
        
        conn.close()