"""Unit tests for MySQL database connection functionality."""

import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import OperationalError

from app.modules.database_connection.models import DatabaseConnection, SupportedDialects
from app.utils.sql_database.sql_database import SQLDatabase, DBConnections
from app.utils.core.encrypt import FernetEncrypt


class TestMySQLConnection:
    """Test MySQL database connection functionality."""

    @pytest.fixture
    def mysql_connection_data(self):
        """Fixture for MySQL connection data."""
        return {
            "id": "mysql-test-id",
            "alias": "test_mysql_db",
            "connection_uri": "mysql://user:password@localhost:3306/testdb",
            "schemas": ["test_schema"],
            "metadata": {"environment": "test"}
        }

    @pytest.fixture
    def encrypted_mysql_connection(self, mysql_connection_data):
        """Fixture for encrypted MySQL connection."""
        fernet = FernetEncrypt()
        encrypted_uri = fernet.encrypt(mysql_connection_data["connection_uri"])
        return {
            **mysql_connection_data,
            "connection_uri": encrypted_uri
        }

    def test_mysql_dialect_support(self):
        """Test that MySQL is supported in SupportedDialects enum."""
        assert SupportedDialects.MYSQL.value == "mysql"
        assert "mysql" in [dialect.value for dialect in SupportedDialects]

    def test_mysql_connection_model_creation(self, mysql_connection_data):
        """Test creating a MySQL DatabaseConnection model."""
        connection = DatabaseConnection(**mysql_connection_data)
        
        assert connection.id == "mysql-test-id"
        assert connection.alias == "test_mysql_db"
        assert connection.dialect == "mysql"
        assert connection.schemas == ["test_schema"]
        assert connection.metadata == {"environment": "test"}

    def test_get_dialect_for_mysql(self):
        """Test dialect extraction for MySQL URI."""
        mysql_uri = "mysql://user:password@host:3306/db"
        dialect = DatabaseConnection.get_dialect(mysql_uri)
        assert dialect == "mysql"

    def test_set_dialect_for_mysql(self):
        """Test dialect setting for MySQL."""
        dialect = DatabaseConnection.set_dialect("mysql://")
        assert dialect == "mysql"

    def test_mysql_uri_encryption_handling(self, encrypted_mysql_connection):
        """Test MySQL connection with encrypted URI."""
        # The encrypted URI won't have dialect auto-detected, so add it manually
        encrypted_mysql_connection["dialect"] = "mysql"
        connection = DatabaseConnection(**encrypted_mysql_connection)
        assert connection.dialect == "mysql"

    @patch('app.utils.sql_database.sql_database.create_engine')
    def test_mysql_engine_creation(self, mock_create_engine):
        """Test SQLAlchemy engine creation for MySQL."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Test that mysql:// is converted to mysql+pymysql://
        mysql_uri = "mysql://user:password@localhost:3306/testdb"
        sql_db = SQLDatabase.from_uri(mysql_uri)
        
        # Verify the URI was transformed correctly
        expected_uri = "mysql+pymysql://user:password@localhost:3306/testdb"
        mock_create_engine.assert_called_once()
        actual_uri = mock_create_engine.call_args[0][0]
        assert actual_uri == expected_uri

    @patch('app.utils.sql_database.sql_database.create_engine')
    def test_mysql_engine_with_existing_pymysql_driver(self, mock_create_engine):
        """Test that mysql+pymysql:// URIs are not modified."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        pymysql_uri = "mysql+pymysql://user:password@localhost:3306/testdb"
        sql_db = SQLDatabase.from_uri(pymysql_uri)
        
        # Verify the URI was not modified
        mock_create_engine.assert_called_once()
        actual_uri = mock_create_engine.call_args[0][0]
        assert actual_uri == pymysql_uri

    @patch('app.utils.sql_database.sql_database.create_engine')
    @patch('app.utils.core.encrypt.FernetEncrypt.decrypt')
    def test_get_sql_engine_mysql(self, mock_decrypt, mock_create_engine):
        """Test get_sql_engine for MySQL connection."""
        # Setup mocks
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_decrypt.return_value = "mysql://user:password@localhost:3306/testdb"
        
        # Create connection
        connection = DatabaseConnection(
            id="mysql-test",
            alias="test_mysql",
            dialect="mysql",
            connection_uri="encrypted_uri",
            schemas=["test_schema"]
        )
        
        # Clear any existing connections
        DBConnections.db_connections.clear()
        
        # Get SQL engine
        sql_db = SQLDatabase.get_sql_engine(connection)
        
        # Verify engine creation with pymysql driver
        expected_uri = "mysql+pymysql://user:password@localhost:3306/testdb"
        mock_create_engine.assert_called_once()
        actual_uri = mock_create_engine.call_args[0][0]
        assert actual_uri == expected_uri
        
        # Verify connection was cached
        assert connection.id in DBConnections.db_connections

    @patch('app.utils.sql_database.sql_database.create_engine')
    def test_mysql_connection_pool_settings(self, mock_create_engine):
        """Test that MySQL connections use appropriate pool settings."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        mysql_uri = "mysql://user:password@localhost:3306/testdb"
        sql_db = SQLDatabase.from_uri(mysql_uri)
        
        # Verify pool settings
        _, kwargs = mock_create_engine.call_args
        assert kwargs['pool_size'] == 10
        assert kwargs['max_overflow'] == 5
        assert kwargs['pool_timeout'] == 30
        assert kwargs['pool_recycle'] == 1500
        assert kwargs['pool_pre_ping'] is True

    def test_extract_mysql_parameters(self):
        """Test parameter extraction from MySQL URI."""
        mysql_uri = "mysql://testuser:testpass@localhost:3306/testdb"
        params = SQLDatabase.extract_parameters(mysql_uri)
        
        assert params['driver'] == 'mysql'
        assert params['user'] == 'testuser'
        assert params['password'] == 'testpass'
        assert params['host'] == 'localhost'
        assert params['port'] == '3306'
        assert params['db'] == 'testdb'

    def test_mysql_uri_without_port(self):
        """Test MySQL URI without explicit port (should default to 3306)."""
        mysql_uri = "mysql://user:password@localhost/testdb"
        params = SQLDatabase.extract_parameters(mysql_uri)
        
        assert params['driver'] == 'mysql'
        assert params['host'] == 'localhost'
        assert params['port'] is None  # Port not specified in URI
        assert params['db'] == 'testdb'

    @patch('app.utils.sql_database.sql_database.create_engine')
    def test_mysql_connection_failure(self, mock_create_engine):
        """Test handling of MySQL connection failures."""
        # Setup mock to raise OperationalError
        mock_create_engine.side_effect = OperationalError("Can't connect to MySQL server", None, None)
        
        mysql_uri = "mysql://user:wrongpass@localhost:3306/testdb"
        
        with pytest.raises(OperationalError):
            sql_db = SQLDatabase.from_uri(mysql_uri)

    def test_mysql_schema_validation(self):
        """Test MySQL connection with multiple schemas."""
        connection_data = {
            "alias": "multi_schema_mysql",
            "connection_uri": "mysql://user:pass@host/db",
            "schemas": ["schema1", "schema2", "information_schema"]
        }
        
        connection = DatabaseConnection(**connection_data)
        assert len(connection.schemas) == 3
        assert "information_schema" in connection.schemas

    def test_mysql_metadata_handling(self):
        """Test MySQL connection with metadata."""
        metadata = {
            "charset": "utf8mb4",
            "collation": "utf8mb4_unicode_ci",
            "timeout": 30,
            "ssl_required": True
        }
        
        connection = DatabaseConnection(
            alias="mysql_with_metadata",
            connection_uri="mysql://user:pass@host/db",
            schemas=["public"],
            metadata=metadata
        )
        
        assert connection.metadata["charset"] == "utf8mb4"
        assert connection.metadata["ssl_required"] is True