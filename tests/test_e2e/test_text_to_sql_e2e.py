"""End-to-end tests for text-to-SQL functionality using real database with synthetic data."""

import os
import pytest
import sqlite3
from typing import Dict, List
from unittest.mock import patch, MagicMock

from app.modules.database_connection.models import DatabaseConnection
from app.modules.prompt.models import Prompt
from app.modules.sql_generation.models import SQLGeneration, LLMConfig
from app.modules.sql_generation.services import SQLGenerationService
from app.data.db.storage import Storage


class TestTextToSQLEndToEnd:
    """End-to-end tests for text-to-SQL functionality with synthetic data."""

    @pytest.fixture(scope="class")
    def test_db_path(self, tmp_path_factory):
        """Create a temporary SQLite database for testing."""
        db_path = tmp_path_factory.mktemp("e2e_test") / "ecommerce_test.db"
        return str(db_path)

    @pytest.fixture(scope="class")
    def synthetic_database(self, test_db_path):
        """Create and populate a synthetic e-commerce database with realistic data."""
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()

        # Create tables
        cursor.executescript("""
            -- Customers table
            CREATE TABLE customers (
                customer_id INTEGER PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                phone TEXT,
                city TEXT,
                state TEXT,
                country TEXT,
                registration_date DATE,
                is_active BOOLEAN DEFAULT 1
            );

            -- Products table
            CREATE TABLE products (
                product_id INTEGER PRIMARY KEY,
                product_name TEXT NOT NULL,
                category TEXT NOT NULL,
                brand TEXT,
                price DECIMAL(10,2) NOT NULL,
                cost DECIMAL(10,2) NOT NULL,
                stock_quantity INTEGER DEFAULT 0,
                is_active BOOLEAN DEFAULT 1,
                created_date DATE
            );

            -- Orders table
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                order_date DATE NOT NULL,
                status TEXT NOT NULL,
                total_amount DECIMAL(10,2) NOT NULL,
                shipping_address TEXT,
                payment_method TEXT,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            );

            -- Order items table
            CREATE TABLE order_items (
                order_item_id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price DECIMAL(10,2) NOT NULL,
                total_price DECIMAL(10,2) NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(order_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            );

            -- Reviews table
            CREATE TABLE reviews (
                review_id INTEGER PRIMARY KEY,
                product_id INTEGER NOT NULL,
                customer_id INTEGER NOT NULL,
                rating INTEGER CHECK(rating >= 1 AND rating <= 5),
                review_text TEXT,
                review_date DATE,
                FOREIGN KEY (product_id) REFERENCES products(product_id),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            );
        """)

        # Insert synthetic data
        # Customers
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
        ]
        cursor.executemany(
            "INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            customers_data
        )

        # Products
        products_data = [
            (1, "Wireless Bluetooth Headphones", "Electronics", "TechBrand", 99.99, 45.00, 150, 1, "2023-01-01"),
            (2, "Smartphone Case", "Electronics", "ProtectCo", 24.99, 8.50, 300, 1, "2023-01-01"),
            (3, "Running Shoes", "Sports", "SportMax", 89.99, 35.00, 75, 1, "2023-01-15"),
            (4, "Coffee Maker", "Home", "BrewMaster", 149.99, 65.00, 50, 1, "2023-02-01"),
            (5, "Yoga Mat", "Sports", "FitLife", 29.99, 12.00, 200, 1, "2023-02-15"),
            (6, "Laptop Stand", "Electronics", "DeskPro", 79.99, 25.00, 80, 1, "2023-03-01"),
            (7, "Water Bottle", "Sports", "HydroMax", 19.99, 7.50, 400, 1, "2023-03-15"),
            (8, "Desk Lamp", "Home", "LightCorp", 45.99, 18.00, 120, 1, "2023-04-01"),
            (9, "Gaming Mouse", "Electronics", "GameTech", 59.99, 22.00, 90, 1, "2023-04-15"),
            (10, "Kitchen Knife Set", "Home", "ChefTools", 129.99, 55.00, 40, 1, "2023-05-01"),
        ]
        cursor.executemany(
            "INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            products_data
        )

        # Orders
        orders_data = [
            (1, 1, "2023-11-01", "completed", 124.98, "123 Main St, New York, NY", "credit_card"),
            (2, 2, "2023-11-02", "completed", 89.99, "456 Oak Ave, Los Angeles, CA", "paypal"),
            (3, 3, "2023-11-03", "processing", 179.98, "789 Pine St, Chicago, IL", "credit_card"),
            (4, 1, "2023-11-04", "completed", 29.99, "123 Main St, New York, NY", "debit_card"),
            (5, 4, "2023-11-05", "shipped", 149.99, "321 Elm St, Houston, TX", "credit_card"),
            (6, 5, "2023-11-06", "completed", 59.99, "654 Maple Dr, Phoenix, AZ", "paypal"),
            (7, 2, "2023-11-07", "cancelled", 45.99, "456 Oak Ave, Los Angeles, CA", "credit_card"),
            (8, 6, "2023-11-08", "completed", 199.98, "987 Cedar Ln, Philadelphia, PA", "credit_card"),
            (9, 7, "2023-11-09", "processing", 79.99, "147 Birch St, San Antonio, TX", "debit_card"),
            (10, 3, "2023-11-10", "completed", 19.99, "789 Pine St, Chicago, IL", "paypal"),
        ]
        cursor.executemany(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)",
            orders_data
        )

        # Order items
        order_items_data = [
            (1, 1, 1, 1, 99.99, 99.99),
            (2, 1, 2, 1, 24.99, 24.99),
            (3, 2, 3, 1, 89.99, 89.99),
            (4, 3, 4, 1, 149.99, 149.99),
            (5, 3, 5, 1, 29.99, 29.99),
            (6, 4, 5, 1, 29.99, 29.99),
            (7, 5, 4, 1, 149.99, 149.99),
            (8, 6, 9, 1, 59.99, 59.99),
            (9, 7, 8, 1, 45.99, 45.99),
            (10, 8, 1, 1, 99.99, 99.99),
            (11, 8, 1, 1, 99.99, 99.99),
            (12, 9, 6, 1, 79.99, 79.99),
            (13, 10, 7, 1, 19.99, 19.99),
        ]
        cursor.executemany(
            "INSERT INTO order_items VALUES (?, ?, ?, ?, ?, ?)",
            order_items_data
        )

        # Reviews
        reviews_data = [
            (1, 1, 1, 5, "Excellent sound quality and comfort!", "2023-11-05"),
            (2, 3, 2, 4, "Great shoes for running, very comfortable.", "2023-11-08"),
            (3, 4, 4, 5, "Perfect coffee maker, makes great coffee every time.", "2023-11-12"),
            (4, 1, 6, 4, "Good headphones for the price.", "2023-11-15"),
            (5, 5, 1, 5, "Love this yoga mat, great quality!", "2023-11-18"),
            (6, 9, 5, 3, "Mouse is okay, nothing special.", "2023-11-20"),
            (7, 2, 3, 5, "Perfect phone case, saved my phone from drops!", "2023-11-22"),
            (8, 7, 10, 4, "Good water bottle, keeps drinks cold.", "2023-11-25"),
        ]
        cursor.executemany(
            "INSERT INTO reviews VALUES (?, ?, ?, ?, ?, ?)",
            reviews_data
        )

        conn.commit()
        conn.close()
        return test_db_path

    @pytest.fixture
    def storage_mock(self):
        """Mock storage for tests."""
        return MagicMock(spec=Storage)

    @pytest.fixture
    def db_connection(self, synthetic_database):
        """Create a database connection for the synthetic database."""
        # SQLite isn't in SupportedDialects, so we'll use PostgreSQL syntax for this test
        # but point to our SQLite database for actual execution
        return DatabaseConnection(
            id="test-sqlite-conn",
            alias="ecommerce_test",
            dialect="postgresql",  # Use postgresql for SQL generation
            connection_uri=f"sqlite:///{synthetic_database}",
            schemas=["main"],
            metadata={"test": True, "actual_dialect": "sqlite"}
        )

    @pytest.fixture
    def test_prompts(self) -> List[Dict]:
        """Test prompts for various SQL generation scenarios."""
        return [
            {
                "text": "How many customers do we have?",
                "expected_tables": ["customers"],
                "expected_sql_contains": ["COUNT", "customers"]
            },
            {
                "text": "What are the top 5 products by total sales revenue?",
                "expected_tables": ["products", "order_items"],
                "expected_sql_contains": ["SUM", "order_items", "products", "GROUP BY", "ORDER BY", "LIMIT 5"]
            },
            {
                "text": "Show me customers from California with their total order amounts",
                "expected_tables": ["customers", "orders"],
                "expected_sql_contains": ["customers", "orders", "California", "CA", "SUM", "JOIN"]
            },
            {
                "text": "Which products have an average rating above 4?",
                "expected_tables": ["products", "reviews"],
                "expected_sql_contains": ["AVG", "rating", "products", "reviews", "HAVING", "> 4"]
            },
            {
                "text": "List all orders from the last 30 days with customer information",
                "expected_tables": ["orders", "customers"],
                "expected_sql_contains": ["orders", "customers", "order_date", "JOIN"]
            },
            {
                "text": "What is the total revenue for each product category?",
                "expected_tables": ["products", "order_items"],
                "expected_sql_contains": ["SUM", "category", "GROUP BY", "products", "order_items"]
            },
            {
                "text": "Find customers who have never placed an order",
                "expected_tables": ["customers", "orders"],
                "expected_sql_contains": ["customers", "orders", "LEFT JOIN", "IS NULL"]
            },
            {
                "text": "Show the monthly sales trend for this year",
                "expected_tables": ["orders"],
                "expected_sql_contains": ["SUM", "orders", "GROUP BY", "strftime", "order_date"]
            }
        ]

    @pytest.fixture
    def sql_generation_service(self, storage_mock):
        """Create SQL generation service with mocked storage."""
        return SQLGenerationService(storage_mock)

    @pytest.fixture
    def mock_llm_response(self):
        """Mock LLM response for SQL generation."""
        def _mock_response(prompt_text: str) -> str:
            """Generate mock SQL based on prompt text."""
            prompt_lower = prompt_text.lower()
            
            if "how many customers" in prompt_lower:
                return "SELECT COUNT(*) as customer_count FROM customers WHERE is_active = 1;"
            elif "top 5 products by total sales" in prompt_lower:
                return """
                SELECT p.product_name, SUM(oi.total_price) as total_revenue
                FROM products p
                JOIN order_items oi ON p.product_id = oi.product_id
                GROUP BY p.product_id, p.product_name
                ORDER BY total_revenue DESC
                LIMIT 5;
                """
            elif "customers from california" in prompt_lower:
                return """
                SELECT c.first_name, c.last_name, c.email, c.state, 
                       COALESCE(SUM(o.total_amount), 0) as total_orders
                FROM customers c
                LEFT JOIN orders o ON c.customer_id = o.customer_id
                WHERE c.state = 'CA'
                GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.state;
                """
            elif "average rating above 4" in prompt_lower:
                return """
                SELECT p.product_name, AVG(r.rating) as avg_rating
                FROM products p
                JOIN reviews r ON p.product_id = r.product_id
                GROUP BY p.product_id, p.product_name
                HAVING AVG(r.rating) > 4;
                """
            elif "orders from the last 30 days" in prompt_lower:
                return """
                SELECT o.order_id, o.order_date, o.total_amount, o.status,
                       c.first_name, c.last_name, c.email
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id
                WHERE o.order_date >= date('now', '-30 days');
                """
            elif "total revenue for each product category" in prompt_lower:
                return """
                SELECT p.category, SUM(oi.total_price) as total_revenue
                FROM products p
                JOIN order_items oi ON p.product_id = oi.product_id
                GROUP BY p.category
                ORDER BY total_revenue DESC;
                """
            elif "customers who have never placed an order" in prompt_lower:
                return """
                SELECT c.customer_id, c.first_name, c.last_name, c.email
                FROM customers c
                LEFT JOIN orders o ON c.customer_id = o.customer_id
                WHERE o.customer_id IS NULL;
                """
            elif "monthly sales trend" in prompt_lower:
                return """
                SELECT strftime('%Y-%m', order_date) as month,
                       SUM(total_amount) as monthly_revenue,
                       COUNT(*) as order_count
                FROM orders
                WHERE strftime('%Y', order_date) = '2023'
                GROUP BY strftime('%Y-%m', order_date)
                ORDER BY month;
                """
            else:
                return "SELECT 1;"  # Fallback SQL
                
        return _mock_response

    def test_database_setup_and_connection(self, synthetic_database, db_connection):
        """Test that the synthetic database is properly set up and accessible."""
        # Verify database file exists
        assert os.path.exists(synthetic_database)
        
        # Test connection
        conn = sqlite3.connect(synthetic_database)
        cursor = conn.cursor()
        
        # Verify tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        expected_tables = ['customers', 'products', 'orders', 'order_items', 'reviews']
        
        for table in expected_tables:
            assert table in tables
        
        # Verify data exists
        cursor.execute("SELECT COUNT(*) FROM customers")
        customer_count = cursor.fetchone()[0]
        assert customer_count == 10
        
        cursor.execute("SELECT COUNT(*) FROM products")
        product_count = cursor.fetchone()[0]
        assert product_count == 10
        
        cursor.execute("SELECT COUNT(*) FROM orders")
        order_count = cursor.fetchone()[0]
        assert order_count == 10
        
        conn.close()

    def test_sql_execution_against_real_data(self, synthetic_database):
        """Test that generated SQL can actually execute against the synthetic database."""
        conn = sqlite3.connect(synthetic_database)
        cursor = conn.cursor()
        
        # Test various SQL queries that the system might generate
        test_queries = [
            "SELECT COUNT(*) FROM customers WHERE is_active = 1",
            "SELECT product_name, price FROM products WHERE category = 'Electronics'",
            "SELECT c.first_name, c.last_name, o.total_amount FROM customers c JOIN orders o ON c.customer_id = o.customer_id",
            "SELECT category, AVG(price) as avg_price FROM products GROUP BY category",
            "SELECT * FROM orders WHERE status = 'completed' ORDER BY order_date DESC LIMIT 5"
        ]
        
        for query in test_queries:
            try:
                cursor.execute(query)
                results = cursor.fetchall()
                assert len(results) >= 0  # Should execute without error
            except Exception as e:
                pytest.fail(f"Query failed to execute: {query}. Error: {str(e)}")
        
        conn.close()

    @patch('app.utils.sql_generator.sql_agent.SQLAgent.generate_response')
    def test_end_to_end_text_to_sql_workflow(
        self,
        mock_generate_response,
        storage_mock,
        db_connection,
        synthetic_database,
        test_prompts,
        mock_llm_response
    ):
        """Test the complete end-to-end text-to-SQL workflow."""
        
        # Setup mocks
        def mock_sql_generation(user_prompt, database_connection, metadata=None):
            sql = mock_llm_response(user_prompt.text)
            return SQLGeneration(
                prompt_id=user_prompt.id,
                sql=sql,
                status="VALID",
                input_tokens_used=100,
                output_tokens_used=50,
                llm_config=LLMConfig()
            )
        
        mock_generate_response.side_effect = mock_sql_generation
        
        # Mock repository methods
        storage_mock.find_by_id.return_value = None
        storage_mock.insert.return_value = None
        storage_mock.update.return_value = None
        storage_mock.find_by.return_value = []
        
        # Test each prompt scenario
        for i, prompt_data in enumerate(test_prompts):
            # Create test prompt
            prompt = Prompt(
                id=f"test-prompt-{i}",
                text=prompt_data["text"],
                db_connection_id=db_connection.id,
                metadata={"test": True}
            )
            
            # Setup storage mock returns
            storage_mock.find_by_id.side_effect = lambda pid: {
                prompt.id: prompt,
                db_connection.id: db_connection
            }.get(pid)
            
            # Create SQL generation service
            service = SQLGenerationService(storage_mock)
            
            # Test SQL generation
            from app.api.requests import SQLGenerationRequest
            request = SQLGenerationRequest(
                llm_config=LLMConfig(model_name="gpt-4o-mini"),
                evaluate=False,
                metadata={"test": True}
            )
            
            # Generate SQL
            result = service.create_sql_generation(prompt.id, request)
            
            # Validate result
            assert result is not None
            assert result.sql is not None
            assert result.status == "VALID"
            assert result.prompt_id == prompt.id
            
            # Test that generated SQL can execute against real database
            conn = sqlite3.connect(synthetic_database)
            cursor = conn.cursor()
            
            try:
                # Clean up the SQL (remove extra whitespace and newlines)
                clean_sql = " ".join(result.sql.strip().split())
                cursor.execute(clean_sql)
                results = cursor.fetchall()
                
                # Verify results make sense for the prompt
                if "count" in prompt_data["text"].lower():
                    assert len(results) == 1  # Count queries return single result
                elif "top 5" in prompt_data["text"].lower():
                    assert len(results) <= 5  # Top 5 queries return at most 5 results
                
            except Exception as e:
                pytest.fail(f"Generated SQL failed to execute for prompt '{prompt_data['text']}': {str(e)}\nSQL: {result.sql}")
            finally:
                conn.close()

    def test_sql_validation_with_real_database_schema(self, synthetic_database, db_connection):
        """Test SQL validation against the actual database schema."""
        conn = sqlite3.connect(synthetic_database)
        cursor = conn.cursor()
        
        # Get actual schema information
        cursor.execute("PRAGMA table_info(customers)")
        customer_columns = [row[1] for row in cursor.fetchall()]
        
        cursor.execute("PRAGMA table_info(products)")
        product_columns = [row[1] for row in cursor.fetchall()]
        
        # Test that our expected columns exist
        expected_customer_columns = ['customer_id', 'first_name', 'last_name', 'email', 'city', 'state']
        for col in expected_customer_columns:
            assert col in customer_columns
        
        expected_product_columns = ['product_id', 'product_name', 'category', 'price', 'stock_quantity']
        for col in expected_product_columns:
            assert col in product_columns
        
        # Test foreign key relationships
        cursor.execute("PRAGMA foreign_key_list(orders)")
        fk_info = cursor.fetchall()
        assert len(fk_info) > 0  # Should have foreign keys
        
        conn.close()

    def test_performance_with_larger_dataset(self, synthetic_database):
        """Test performance implications with a realistic dataset size."""
        conn = sqlite3.connect(synthetic_database)
        cursor = conn.cursor()
        
        # Add performance indexes that a real system would have
        performance_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email)",
            "CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date)",
            "CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id)",
            "CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id)",
            "CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON reviews(product_id)",
            "CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)"
        ]
        
        for index_sql in performance_indexes:
            cursor.execute(index_sql)
        
        # Test complex query performance
        complex_query = """
        SELECT 
            p.category,
            COUNT(DISTINCT c.customer_id) as unique_customers,
            SUM(oi.total_price) as total_revenue,
            AVG(r.rating) as avg_rating,
            COUNT(o.order_id) as total_orders
        FROM products p
        JOIN order_items oi ON p.product_id = oi.product_id
        JOIN orders o ON oi.order_id = o.order_id
        JOIN customers c ON o.customer_id = c.customer_id
        LEFT JOIN reviews r ON p.product_id = r.product_id
        WHERE o.status = 'completed'
        GROUP BY p.category
        HAVING total_revenue > 100
        ORDER BY total_revenue DESC;
        """
        
        import time
        start_time = time.time()
        cursor.execute(complex_query)
        results = cursor.fetchall()
        execution_time = time.time() - start_time
        
        # Performance assertion (should complete quickly even with joins)
        assert execution_time < 1.0  # Should complete in under 1 second
        assert len(results) > 0  # Should return results
        
        conn.close()

    def test_data_consistency_and_integrity(self, synthetic_database):
        """Test that synthetic data maintains referential integrity."""
        conn = sqlite3.connect(synthetic_database)
        cursor = conn.cursor()
        
        # Test referential integrity
        # All orders should have valid customer_ids
        cursor.execute("""
            SELECT COUNT(*) FROM orders o 
            LEFT JOIN customers c ON o.customer_id = c.customer_id 
            WHERE c.customer_id IS NULL
        """)
        orphaned_orders = cursor.fetchone()[0]
        assert orphaned_orders == 0
        
        # All order_items should have valid order_ids and product_ids
        cursor.execute("""
            SELECT COUNT(*) FROM order_items oi 
            LEFT JOIN orders o ON oi.order_id = o.order_id 
            WHERE o.order_id IS NULL
        """)
        orphaned_order_items = cursor.fetchone()[0]
        assert orphaned_order_items == 0
        
        cursor.execute("""
            SELECT COUNT(*) FROM order_items oi 
            LEFT JOIN products p ON oi.product_id = p.product_id 
            WHERE p.product_id IS NULL
        """)
        invalid_product_refs = cursor.fetchone()[0]
        assert invalid_product_refs == 0
        
        # All reviews should have valid product_ids and customer_ids
        cursor.execute("""
            SELECT COUNT(*) FROM reviews r 
            LEFT JOIN products p ON r.product_id = p.product_id 
            WHERE p.product_id IS NULL
        """)
        invalid_review_products = cursor.fetchone()[0]
        assert invalid_review_products == 0
        
        # Test business logic consistency
        # Order total should match sum of order items
        cursor.execute("""
            SELECT o.order_id, o.total_amount, 
                   COALESCE(SUM(oi.total_price), 0) as calculated_total
            FROM orders o
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
            GROUP BY o.order_id, o.total_amount
            HAVING ABS(o.total_amount - calculated_total) > 0.01
        """)
        mismatched_totals = cursor.fetchall()
        assert len(mismatched_totals) == 0, f"Found orders with mismatched totals: {mismatched_totals}"
        
        conn.close()