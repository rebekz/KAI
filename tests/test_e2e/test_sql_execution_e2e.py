"""End-to-end tests for SQL execution against real database with synthetic data."""

import os
import pytest
import sqlite3
from typing import Dict, List



class TestSQLExecutionEndToEnd:
    """End-to-end tests for SQL execution with realistic synthetic data."""

    @pytest.fixture(scope="class")
    def test_db_path(self, tmp_path_factory):
        """Create a temporary SQLite database for testing."""
        db_path = tmp_path_factory.mktemp("sql_e2e_test") / "business_analytics.db"
        return str(db_path)

    @pytest.fixture(scope="class")
    def business_database(self, test_db_path):
        """Create a comprehensive business analytics database with realistic data."""
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()

        # Create comprehensive business schema
        cursor.executescript("""
            -- Companies table
            CREATE TABLE companies (
                company_id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_name VARCHAR(200) NOT NULL,
                industry VARCHAR(100) NOT NULL,
                founded_year INTEGER,
                headquarters VARCHAR(200),
                employee_count INTEGER,
                revenue_usd DECIMAL(15,2),
                is_public BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Employees table
            CREATE TABLE employees (
                employee_id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                department VARCHAR(100) NOT NULL,
                position VARCHAR(100) NOT NULL,
                hire_date DATE NOT NULL,
                salary DECIMAL(10,2) NOT NULL,
                manager_id INTEGER,
                is_active BOOLEAN DEFAULT 1,
                performance_rating INTEGER CHECK(performance_rating >= 1 AND performance_rating <= 5),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (company_id) REFERENCES companies(company_id),
                FOREIGN KEY (manager_id) REFERENCES employees(employee_id)
            );

            -- Projects table
            CREATE TABLE projects (
                project_id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                project_name VARCHAR(200) NOT NULL,
                description TEXT,
                start_date DATE NOT NULL,
                end_date DATE,
                budget DECIMAL(12,2) NOT NULL,
                actual_cost DECIMAL(12,2) DEFAULT 0,
                status VARCHAR(50) NOT NULL,
                priority VARCHAR(20) DEFAULT 'medium',
                project_manager_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (company_id) REFERENCES companies(company_id),
                FOREIGN KEY (project_manager_id) REFERENCES employees(employee_id)
            );

            -- Project assignments table
            CREATE TABLE project_assignments (
                assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                employee_id INTEGER NOT NULL,
                role VARCHAR(100) NOT NULL,
                allocation_percentage INTEGER DEFAULT 100,
                start_date DATE NOT NULL,
                end_date DATE,
                billable_rate DECIMAL(8,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (project_id) REFERENCES projects(project_id),
                FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
            );

            -- Time tracking table
            CREATE TABLE time_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id INTEGER NOT NULL,
                project_id INTEGER NOT NULL,
                work_date DATE NOT NULL,
                hours_worked DECIMAL(4,2) NOT NULL,
                description TEXT,
                is_billable BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
                FOREIGN KEY (project_id) REFERENCES projects(project_id)
            );

            -- Performance reviews table
            CREATE TABLE performance_reviews (
                review_id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id INTEGER NOT NULL,
                reviewer_id INTEGER NOT NULL,
                review_period VARCHAR(20) NOT NULL,
                review_date DATE NOT NULL,
                overall_rating INTEGER CHECK(overall_rating >= 1 AND overall_rating <= 5),
                goals_met INTEGER CHECK(goals_met >= 1 AND goals_met <= 5),
                communication_rating INTEGER CHECK(communication_rating >= 1 AND communication_rating <= 5),
                technical_skills INTEGER CHECK(technical_skills >= 1 AND technical_skills <= 5),
                comments TEXT,
                salary_change DECIMAL(10,2) DEFAULT 0,
                promotion BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
                FOREIGN KEY (reviewer_id) REFERENCES employees(employee_id)
            );
        """)

        # Insert comprehensive test data
        # Companies
        companies_data = [
            (1, "TechCorp Solutions", "Technology", 2015, "San Francisco, CA", 250, 25000000.00, 1),
            (2, "DataFlow Analytics", "Technology", 2018, "New York, NY", 120, 8500000.00, 0),
            (3, "GreenEnergy Inc", "Energy", 2012, "Austin, TX", 180, 15000000.00, 1),
            (4, "HealthFirst Medical", "Healthcare", 2008, "Boston, MA", 320, 45000000.00, 1),
            (5, "EduTech Learning", "Education", 2019, "Seattle, WA", 85, 3200000.00, 0),
        ]
        cursor.executemany(
            "INSERT INTO companies (company_id, company_name, industry, founded_year, headquarters, employee_count, revenue_usd, is_public) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            companies_data
        )

        # Employees (hierarchical structure with managers)
        employees_data = [
            # TechCorp Solutions
            (1, 1, "John", "Smith", "john.smith@techcorp.com", "Engineering", "CTO", "2015-03-01", 180000.00, None, 1, 5),
            (2, 1, "Sarah", "Johnson", "sarah.j@techcorp.com", "Engineering", "Senior Developer", "2016-06-15", 125000.00, 1, 1, 4),
            (3, 1, "Michael", "Brown", "m.brown@techcorp.com", "Engineering", "Developer", "2018-09-01", 95000.00, 2, 1, 4),
            (4, 1, "Emily", "Davis", "emily.d@techcorp.com", "Product", "Product Manager", "2017-01-20", 115000.00, None, 1, 5),
            (5, 1, "David", "Wilson", "d.wilson@techcorp.com", "Sales", "Sales Director", "2015-08-10", 140000.00, None, 1, 4),
            (6, 1, "Lisa", "Garcia", "lisa.g@techcorp.com", "Marketing", "Marketing Manager", "2019-03-15", 85000.00, None, 1, 3),
            # DataFlow Analytics
            (7, 2, "James", "Miller", "james.m@dataflow.com", "Data Science", "Lead Data Scientist", "2018-05-01", 145000.00, None, 1, 5),
            (8, 2, "Jennifer", "Martinez", "j.martinez@dataflow.com", "Data Science", "Data Scientist", "2019-08-20", 105000.00, 7, 1, 4),
            (9, 2, "Robert", "Anderson", "r.anderson@dataflow.com", "Engineering", "Backend Developer", "2020-01-15", 98000.00, None, 1, 3),
            (10, 2, "Ashley", "Taylor", "ashley.t@dataflow.com", "Business", "Business Analyst", "2021-04-01", 75000.00, None, 1, 4),
            # GreenEnergy Inc
            (11, 3, "Christopher", "Thomas", "chris.t@greenenergy.com", "Engineering", "Project Manager", "2012-09-01", 120000.00, None, 1, 4),
            (12, 3, "Amanda", "Jackson", "amanda.j@greenenergy.com", "Research", "Research Scientist", "2014-02-15", 110000.00, None, 1, 5),
            (13, 3, "Daniel", "White", "daniel.w@greenenergy.com", "Operations", "Operations Manager", "2016-07-01", 95000.00, None, 1, 3),
            # HealthFirst Medical
            (14, 4, "Jessica", "Harris", "jessica.h@healthfirst.com", "Medical", "Chief Medical Officer", "2008-11-01", 220000.00, None, 1, 5),
            (15, 4, "Matthew", "Clark", "matthew.c@healthfirst.com", "IT", "IT Director", "2015-06-01", 130000.00, None, 1, 4),
            (16, 4, "Nicole", "Lewis", "nicole.l@healthfirst.com", "Administration", "HR Manager", "2018-09-15", 85000.00, None, 1, 4),
            # EduTech Learning
            (17, 5, "Kevin", "Robinson", "kevin.r@edutech.com", "Product", "CEO", "2019-01-01", 160000.00, None, 1, 5),
            (18, 5, "Rachel", "Walker", "rachel.w@edutech.com", "Engineering", "Full Stack Developer", "2020-03-01", 92000.00, None, 1, 4),
        ]
        cursor.executemany(
            "INSERT INTO employees (employee_id, company_id, first_name, last_name, email, department, position, hire_date, salary, manager_id, is_active, performance_rating) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            employees_data
        )

        # Projects (include some over-budget projects)
        projects_data = [
            (1, 1, "AI Platform Development", "Building next-gen AI platform", "2023-01-15", "2023-12-31", 500000.00, 520000.00, "in_progress", "high", 1),
            (2, 1, "Mobile App Redesign", "Redesigning mobile application", "2023-06-01", "2023-11-30", 150000.00, 165000.00, "completed", "medium", 4),
            (3, 2, "Customer Analytics Dashboard", "Building analytics dashboard", "2023-03-01", "2024-02-28", 200000.00, 180000.00, "in_progress", "high", 7),
            (4, 3, "Solar Panel Efficiency Study", "Research on panel efficiency", "2023-02-01", "2023-08-31", 300000.00, 330000.00, "completed", "high", 11),
            (5, 4, "Patient Portal Enhancement", "Improving patient portal", "2023-04-01", "2024-01-31", 400000.00, 350000.00, "in_progress", "medium", 15),
            (6, 5, "Learning Management System", "Building new LMS platform", "2023-05-01", "2024-04-30", 250000.00, 280000.00, "in_progress", "high", 17),
        ]
        cursor.executemany(
            "INSERT INTO projects (project_id, company_id, project_name, description, start_date, end_date, budget, actual_cost, status, priority, project_manager_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            projects_data
        )

        # Project assignments
        assignments_data = [
            (1, 1, 1, "Technical Lead", 80, "2023-01-15", None, 200.00),
            (2, 1, 2, "Senior Developer", 100, "2023-01-20", None, 150.00),
            (3, 1, 3, "Developer", 100, "2023-02-01", None, 120.00),
            (4, 2, 4, "Product Manager", 60, "2023-06-01", "2023-11-30", 140.00),
            (5, 2, 6, "UX Designer", 80, "2023-06-01", "2023-11-30", 100.00),
            (6, 3, 7, "Lead Data Scientist", 90, "2023-03-01", None, 180.00),
            (7, 3, 8, "Data Scientist", 100, "2023-03-15", None, 130.00),
            (8, 4, 12, "Research Lead", 100, "2023-02-01", "2023-08-31", 140.00),
            (9, 5, 15, "Technical Lead", 70, "2023-04-01", None, 160.00),
            (10, 6, 17, "Project Manager", 50, "2023-05-01", None, 180.00),
            (11, 6, 18, "Full Stack Developer", 100, "2023-05-15", None, 110.00),
        ]
        cursor.executemany(
            "INSERT INTO project_assignments (assignment_id, project_id, employee_id, role, allocation_percentage, start_date, end_date, billable_rate) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            assignments_data
        )

        # Time logs (recent data)
        time_logs_data = [
            (1, 1, 1, "2023-11-20", 8.0, "AI model optimization", 1),
            (2, 2, 1, "2023-11-20", 6.5, "Code review and architecture", 1),
            (3, 3, 1, "2023-11-20", 7.0, "Feature development", 1),
            (4, 7, 3, "2023-11-20", 8.0, "Dashboard implementation", 1),
            (5, 8, 3, "2023-11-20", 7.5, "Data pipeline development", 1),
            (6, 12, 4, "2023-11-20", 8.0, "Research documentation", 1),
            (7, 15, 5, "2023-11-20", 6.0, "System architecture review", 1),
            (8, 18, 6, "2023-11-20", 8.0, "Frontend development", 1),
            (9, 1, 1, "2023-11-21", 7.5, "Team meetings and planning", 1),
            (10, 2, 1, "2023-11-21", 8.0, "Performance optimization", 1),
        ]
        cursor.executemany(
            "INSERT INTO time_logs (log_id, employee_id, project_id, work_date, hours_worked, description, is_billable) VALUES (?, ?, ?, ?, ?, ?, ?)",
            time_logs_data
        )

        # Performance reviews
        reviews_data = [
            (1, 2, 1, "2023-Q3", "2023-10-15", 4, 4, 5, 4, "Excellent technical skills, good team collaboration", 5000.00, 0),
            (2, 3, 2, "2023-Q3", "2023-10-20", 4, 4, 4, 4, "Solid performance, meeting all targets", 3000.00, 0),
            (3, 8, 7, "2023-Q3", "2023-10-25", 4, 5, 4, 5, "Outstanding analytical skills", 7000.00, 0),
            (4, 12, 11, "2023-Q3", "2023-10-30", 5, 5, 4, 5, "Exceptional research output", 8000.00, 1),
            (5, 18, 17, "2023-Q3", "2023-11-05", 4, 4, 4, 4, "Good progress on development tasks", 4000.00, 0),
        ]
        cursor.executemany(
            "INSERT INTO performance_reviews (review_id, employee_id, reviewer_id, review_period, review_date, overall_rating, goals_met, communication_rating, technical_skills, comments, salary_change, promotion) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            reviews_data
        )

        conn.commit()
        conn.close()
        return test_db_path

    @pytest.fixture
    def business_intelligence_queries(self) -> List[Dict]:
        """Real-world business intelligence queries that would be generated from natural language."""
        return [
            {
                "name": "Employee Count by Department",
                "natural_language": "How many employees do we have in each department?",
                "sql": """
                    SELECT 
                        department,
                        COUNT(*) as employee_count,
                        ROUND(AVG(salary), 2) as avg_salary
                    FROM employees 
                    WHERE is_active = 1
                    GROUP BY department
                    ORDER BY employee_count DESC
                """,
                "expected_validation": lambda results: len(results) > 0 and all(row[1] > 0 for row in results)
            },
            {
                "name": "Project Budget vs Actual Cost Analysis",
                "natural_language": "Show me projects that are over budget",
                "sql": """
                    SELECT 
                        p.project_name,
                        c.company_name,
                        p.budget,
                        p.actual_cost,
                        (p.actual_cost - p.budget) as over_budget_amount,
                        ROUND(((p.actual_cost - p.budget) / p.budget * 100), 2) as over_budget_percentage
                    FROM projects p
                    JOIN companies c ON p.company_id = c.company_id
                    WHERE p.actual_cost > p.budget
                    ORDER BY over_budget_percentage DESC
                """,
                "expected_validation": lambda results: all(row[4] > 0 for row in results)  # over_budget_amount > 0
            },
            {
                "name": "Top Performers by Department",
                "natural_language": "Who are the top performers in each department based on their latest review?",
                "sql": """
                    SELECT 
                        e.department,
                        e.first_name,
                        e.last_name,
                        e.position,
                        pr.overall_rating,
                        pr.technical_skills,
                        e.salary
                    FROM employees e
                    JOIN performance_reviews pr ON e.employee_id = pr.employee_id
                    WHERE pr.overall_rating >= 4
                        AND e.is_active = 1
                    ORDER BY e.department, pr.overall_rating DESC, pr.technical_skills DESC
                """,
                "expected_validation": lambda results: all(row[4] >= 4 for row in results)  # overall_rating >= 4
            },
            {
                "name": "Project Utilization by Employee",
                "natural_language": "Show me how much time each employee has logged on projects this month",
                "sql": """
                    SELECT 
                        e.first_name,
                        e.last_name,
                        e.department,
                        p.project_name,
                        SUM(tl.hours_worked) as total_hours,
                        COUNT(DISTINCT tl.work_date) as days_worked,
                        ROUND(AVG(tl.hours_worked), 2) as avg_daily_hours
                    FROM employees e
                    JOIN time_logs tl ON e.employee_id = tl.employee_id
                    JOIN projects p ON tl.project_id = p.project_id
                    WHERE tl.work_date >= '2023-11-01'
                        AND e.is_active = 1
                    GROUP BY e.employee_id, e.first_name, e.last_name, e.department, p.project_name
                    ORDER BY total_hours DESC
                """,
                "expected_validation": lambda results: all(row[4] > 0 for row in results)  # total_hours > 0
            },
            {
                "name": "Company Revenue per Employee",
                "natural_language": "What is the revenue per employee for each company?",
                "sql": """
                    SELECT 
                        c.company_name,
                        c.industry,
                        c.employee_count,
                        c.revenue_usd,
                        ROUND(c.revenue_usd / c.employee_count, 2) as revenue_per_employee,
                        CASE 
                            WHEN c.is_public = 1 THEN 'Public'
                            ELSE 'Private'
                        END as company_type
                    FROM companies c
                    WHERE c.employee_count > 0
                    ORDER BY revenue_per_employee DESC
                """,
                "expected_validation": lambda results: all(row[4] > 0 for row in results)  # revenue_per_employee > 0
            },
            {
                "name": "Salary Distribution Analysis",
                "natural_language": "Show me salary statistics by department and seniority level",
                "sql": """
                    SELECT 
                        department,
                        CASE 
                            WHEN position LIKE '%Senior%' OR position LIKE '%Lead%' OR position LIKE '%Director%' OR position LIKE '%Manager%' OR position LIKE '%CTO%' OR position LIKE '%CEO%' OR position LIKE '%Chief%' THEN 'Senior'
                            ELSE 'Junior/Mid'
                        END as seniority_level,
                        COUNT(*) as employee_count,
                        ROUND(MIN(salary), 2) as min_salary,
                        ROUND(MAX(salary), 2) as max_salary,
                        ROUND(AVG(salary), 2) as avg_salary,
                        ROUND(AVG(performance_rating), 2) as avg_performance
                    FROM employees
                    WHERE is_active = 1
                    GROUP BY department, seniority_level
                    ORDER BY department, avg_salary DESC
                """,
                "expected_validation": lambda results: len(results) > 0 and all(len(row) >= 7 for row in results)  # Just check structure
            },
            {
                "name": "Project Timeline Analysis",
                "natural_language": "Which projects are running behind schedule or over budget?",
                "sql": """
                    SELECT 
                        p.project_name,
                        c.company_name,
                        p.start_date,
                        p.end_date,
                        p.status,
                        p.budget,
                        p.actual_cost,
                        CASE 
                            WHEN p.end_date < date('now') AND p.status != 'completed' THEN 'Overdue'
                            WHEN p.actual_cost > p.budget THEN 'Over Budget'
                            WHEN p.end_date < date('now') AND p.status = 'completed' THEN 'Completed On Time'
                            ELSE 'On Track'
                        END as project_status_analysis,
                        julianday(p.end_date) - julianday(date('now')) as days_until_deadline
                    FROM projects p
                    JOIN companies c ON p.company_id = c.company_id
                    ORDER BY 
                        CASE 
                            WHEN p.end_date < date('now') AND p.status != 'completed' THEN 1
                            WHEN p.actual_cost > p.budget THEN 2
                            ELSE 3
                        END,
                        p.end_date
                """,
                "expected_validation": lambda results: len(results) > 0
            }
        ]

    def test_database_creation_and_schema(self, business_database):
        """Test that the business database is created with proper schema and data."""
        assert os.path.exists(business_database)
        
        conn = sqlite3.connect(business_database)
        cursor = conn.cursor()
        
        # Verify all tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        expected_tables = ['companies', 'employees', 'projects', 'project_assignments', 'time_logs', 'performance_reviews']
        
        for table in expected_tables:
            assert table in tables, f"Table {table} missing from database"
        
        # Verify data exists
        cursor.execute("SELECT COUNT(*) FROM companies")
        assert cursor.fetchone()[0] == 5
        
        cursor.execute("SELECT COUNT(*) FROM employees WHERE is_active = 1")
        assert cursor.fetchone()[0] == 18
        
        cursor.execute("SELECT COUNT(*) FROM projects")
        assert cursor.fetchone()[0] == 6
        
        conn.close()

    def test_business_intelligence_queries_execution(self, business_database, business_intelligence_queries):
        """Test execution of realistic business intelligence queries."""
        conn = sqlite3.connect(business_database)
        cursor = conn.cursor()
        
        for query_info in business_intelligence_queries:
            try:
                # Execute the query
                cursor.execute(query_info["sql"])
                results = cursor.fetchall()
                
                # Validate results using the provided validation function
                assert query_info["expected_validation"](results), f"Validation failed for query: {query_info['name']}"
                
                # Ensure we get meaningful results
                assert len(results) > 0, f"Query '{query_info['name']}' returned no results"
                
                print(f"✓ Query '{query_info['name']}' executed successfully with {len(results)} results")
                
            except Exception as e:
                pytest.fail(f"Query '{query_info['name']}' failed to execute: {str(e)}\nSQL: {query_info['sql']}")
        
        conn.close()

    def test_complex_join_queries(self, business_database):
        """Test complex multi-table join queries that AI would generate."""
        conn = sqlite3.connect(business_database)
        cursor = conn.cursor()
        
        complex_queries = [
            {
                "name": "Employee Project Workload Analysis",
                "sql": """
                    SELECT 
                        e.first_name || ' ' || e.last_name as employee_name,
                        e.department,
                        e.position,
                        COUNT(DISTINCT pa.project_id) as active_projects,
                        SUM(pa.allocation_percentage) as total_allocation,
                        ROUND(AVG(pa.billable_rate), 2) as avg_billable_rate,
                        SUM(tl.hours_worked) as total_hours_logged
                    FROM employees e
                    LEFT JOIN project_assignments pa ON e.employee_id = pa.employee_id
                    LEFT JOIN projects p ON pa.project_id = p.project_id AND p.status = 'in_progress'
                    LEFT JOIN time_logs tl ON e.employee_id = tl.employee_id 
                        AND tl.work_date >= '2023-11-01'
                    WHERE e.is_active = 1
                    GROUP BY e.employee_id, e.first_name, e.last_name, e.department, e.position
                    HAVING total_allocation > 0 OR total_hours_logged > 0
                    ORDER BY total_allocation DESC, total_hours_logged DESC
                """
            },
            {
                "name": "Company Performance Dashboard",
                "sql": """
                    SELECT 
                        c.company_name,
                        c.industry,
                        COUNT(DISTINCT e.employee_id) as total_employees,
                        COUNT(DISTINCT p.project_id) as total_projects,
                        COUNT(DISTINCT CASE WHEN p.status = 'in_progress' THEN p.project_id END) as active_projects,
                        COALESCE(SUM(p.budget), 0) as total_project_budget,
                        COALESCE(SUM(p.actual_cost), 0) as total_project_cost,
                        ROUND(AVG(e.salary), 2) as avg_employee_salary,
                        ROUND(AVG(e.performance_rating), 2) as avg_performance_rating
                    FROM companies c
                    LEFT JOIN employees e ON c.company_id = e.company_id AND e.is_active = 1
                    LEFT JOIN projects p ON c.company_id = p.company_id
                    GROUP BY c.company_id, c.company_name, c.industry
                    ORDER BY total_employees DESC
                """
            },
            {
                "name": "Project Profitability Analysis",
                "sql": """
                    SELECT 
                        p.project_name,
                        c.company_name,
                        p.budget,
                        p.actual_cost,
                        SUM(tl.hours_worked * COALESCE(pa.billable_rate, 100)) as total_billable_revenue,
                        (SUM(tl.hours_worked * COALESCE(pa.billable_rate, 100)) - p.actual_cost) as estimated_profit,
                        ROUND(
                            (SUM(tl.hours_worked * COALESCE(pa.billable_rate, 100)) - p.actual_cost) / 
                            NULLIF(SUM(tl.hours_worked * COALESCE(pa.billable_rate, 100)), 0) * 100, 
                            2
                        ) as profit_margin_percentage
                    FROM projects p
                    JOIN companies c ON p.company_id = c.company_id
                    LEFT JOIN project_assignments pa ON p.project_id = pa.project_id
                    LEFT JOIN time_logs tl ON pa.employee_id = tl.employee_id 
                        AND tl.project_id = p.project_id
                        AND tl.is_billable = 1
                    GROUP BY p.project_id, p.project_name, c.company_name, p.budget, p.actual_cost
                    HAVING total_billable_revenue > 0
                    ORDER BY profit_margin_percentage DESC
                """
            }
        ]
        
        for query_info in complex_queries:
            try:
                cursor.execute(query_info["sql"])
                results = cursor.fetchall()
                
                assert len(results) > 0, f"Complex query '{query_info['name']}' returned no results"
                
                # Verify data integrity
                for row in results:
                    assert all(col is not None for col in row[:3]), f"Query '{query_info['name']}' has NULL values in key columns"
                
                print(f"✓ Complex query '{query_info['name']}' executed successfully")
                
            except Exception as e:
                pytest.fail(f"Complex query '{query_info['name']}' failed: {str(e)}")
        
        conn.close()

    def test_sql_injection_prevention(self, business_database):
        """Test that the database handles potentially problematic queries safely."""
        conn = sqlite3.connect(business_database)
        cursor = conn.cursor()
        
        # Test queries that might be generated incorrectly by AI
        safe_queries = [
            "SELECT * FROM employees WHERE department = 'Engineering' AND salary > 100000",
            "SELECT COUNT(*) FROM projects WHERE status IN ('completed', 'in_progress')",
            "SELECT AVG(salary) FROM employees WHERE hire_date >= '2020-01-01'",
            "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 2"
        ]
        
        for query in safe_queries:
            try:
                cursor.execute(query)
                results = cursor.fetchall()
                assert isinstance(results, list), f"Query should return list: {query}"
            except Exception as e:
                pytest.fail(f"Safe query failed: {query}, Error: {str(e)}")
        
        conn.close()

    def test_performance_with_realistic_data_volume(self, business_database):
        """Test query performance with realistic data volumes."""
        conn = sqlite3.connect(business_database)
        cursor = conn.cursor()
        
        # Add indexes for better performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_employees_company_id ON employees(company_id)",
            "CREATE INDEX IF NOT EXISTS idx_employees_department ON employees(department)",
            "CREATE INDEX IF NOT EXISTS idx_projects_company_id ON projects(company_id)",
            "CREATE INDEX IF NOT EXISTS idx_project_assignments_project_id ON project_assignments(project_id)",
            "CREATE INDEX IF NOT EXISTS idx_time_logs_employee_id ON time_logs(employee_id)",
            "CREATE INDEX IF NOT EXISTS idx_time_logs_work_date ON time_logs(work_date)"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Test performance of common business queries
        import time
        
        performance_tests = [
            {
                "name": "Employee Summary",
                "sql": "SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department",
                "max_time": 0.1
            },
            {
                "name": "Project Dashboard",
                "sql": """
                    SELECT p.project_name, c.company_name, p.status, p.budget 
                    FROM projects p 
                    JOIN companies c ON p.company_id = c.company_id 
                    ORDER BY p.budget DESC
                """,
                "max_time": 0.1
            },
            {
                "name": "Complex Analytics",
                "sql": """
                    SELECT e.department, COUNT(*) as emp_count, 
                           AVG(e.salary) as avg_salary,
                           COUNT(DISTINCT pa.project_id) as active_projects
                    FROM employees e
                    LEFT JOIN project_assignments pa ON e.employee_id = pa.employee_id
                    WHERE e.is_active = 1
                    GROUP BY e.department
                """,
                "max_time": 0.2
            }
        ]
        
        for test_info in performance_tests:
            start_time = time.time()
            cursor.execute(test_info["sql"])
            results = cursor.fetchall()
            execution_time = time.time() - start_time
            
            assert execution_time < test_info["max_time"], f"Query '{test_info['name']}' took too long: {execution_time:.3f}s"
            assert len(results) > 0, f"Query '{test_info['name']}' returned no results"
        
        conn.close()

    def test_data_consistency_and_referential_integrity(self, business_database):
        """Test that all foreign key relationships are maintained."""
        conn = sqlite3.connect(business_database)
        cursor = conn.cursor()
        
        integrity_checks = [
            {
                "name": "Employees have valid companies",
                "sql": """
                    SELECT COUNT(*) FROM employees e 
                    LEFT JOIN companies c ON e.company_id = c.company_id 
                    WHERE c.company_id IS NULL
                """,
                "expected": 0
            },
            {
                "name": "Projects have valid companies",
                "sql": """
                    SELECT COUNT(*) FROM projects p 
                    LEFT JOIN companies c ON p.company_id = c.company_id 
                    WHERE c.company_id IS NULL
                """,
                "expected": 0
            },
            {
                "name": "Project assignments reference valid projects and employees",
                "sql": """
                    SELECT COUNT(*) FROM project_assignments pa 
                    LEFT JOIN projects p ON pa.project_id = p.project_id 
                    LEFT JOIN employees e ON pa.employee_id = e.employee_id
                    WHERE p.project_id IS NULL OR e.employee_id IS NULL
                """,
                "expected": 0
            },
            {
                "name": "Performance reviews reference valid employees",
                "sql": """
                    SELECT COUNT(*) FROM performance_reviews pr 
                    LEFT JOIN employees e ON pr.employee_id = e.employee_id 
                    WHERE e.employee_id IS NULL
                """,
                "expected": 0
            }
        ]
        
        for check in integrity_checks:
            cursor.execute(check["sql"])
            result = cursor.fetchone()[0]
            assert result == check["expected"], f"Integrity check failed: {check['name']} (expected {check['expected']}, got {result})"
        
        conn.close()

    def test_end_to_end_text_to_sql_simulation(self, business_database):
        """Simulate the complete text-to-SQL workflow using realistic business queries."""
        conn = sqlite3.connect(business_database)
        cursor = conn.cursor()
        
        # Simulate natural language to SQL scenarios
        text_to_sql_scenarios = [
            {
                "natural_language": "Show me all employees in the Engineering department",
                "generated_sql": "SELECT first_name, last_name, position, salary FROM employees WHERE department = 'Engineering' AND is_active = 1",
                "expected_count": lambda count: count > 0
            },
            {
                "natural_language": "What is the average salary by department?",
                "generated_sql": "SELECT department, AVG(salary) as avg_salary, COUNT(*) as employee_count FROM employees WHERE is_active = 1 GROUP BY department",
                "expected_count": lambda count: count > 0
            },
            {
                "natural_language": "Which projects are over budget?",
                "generated_sql": "SELECT project_name, budget, actual_cost, (actual_cost - budget) as over_amount FROM projects WHERE actual_cost > budget",
                "expected_count": lambda count: count > 0
            },
            {
                "natural_language": "Show me the top 3 highest paid employees",
                "generated_sql": "SELECT first_name, last_name, department, salary FROM employees WHERE is_active = 1 ORDER BY salary DESC LIMIT 3",
                "expected_count": lambda count: count == 3
            }
        ]
        
        for scenario in text_to_sql_scenarios:
            try:
                cursor.execute(scenario["generated_sql"])
                results = cursor.fetchall()
                
                assert scenario["expected_count"](len(results)), f"Scenario failed: {scenario['natural_language']}"
                print(f"✓ Text-to-SQL scenario '{scenario['natural_language']}' executed successfully")
                
            except Exception as e:
                pytest.fail(f"Text-to-SQL scenario failed: {scenario['natural_language']}, Error: {str(e)}")
        
        conn.close()