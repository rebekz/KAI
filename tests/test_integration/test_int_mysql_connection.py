"""Integration tests for MySQL database connection."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

from app.main import app
from app.modules.database_connection.models import DatabaseConnection
from app.utils.core.encrypt import FernetEncrypt


@pytest.fixture
def client():
    """Create test client."""
    return TestClient(app)


@pytest.fixture
def mysql_connection_payload():
    """MySQL connection payload for API requests."""
    return {
        "alias": "test_mysql_integration",
        "connection_uri": "mysql://testuser:testpass@localhost:3306/testdb",
        "schemas": ["test_schema", "information_schema"]
    }


class TestMySQLConnectionIntegration:
    """Integration tests for MySQL database connections."""

    @patch('app.data.db.storage.Storage.insert')
    @patch('app.utils.sql_database.sql_database.create_engine')
    def test_create_mysql_connection_api(self, mock_create_engine, mock_insert, client, mysql_connection_payload):
        """Test creating MySQL connection via API."""
        # Setup mocks
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        # Mock the insert to return a connection with ID
        mock_insert.return_value = {
            "id": "mysql-conn-123",
            **mysql_connection_payload,
            "dialect": "mysql",
            "created_at": "2024-05-24T12:00:00"
        }
        
        # Make API request
        response = client.post("/api/database-connections", json=mysql_connection_payload)
        
        # Verify response
        assert response.status_code == 201
        data = response.json()
        assert data["alias"] == "test_mysql_integration"
        assert data["dialect"] == "mysql"
        assert "mysql-conn-123" in data["id"]

    @patch('app.data.db.storage.Storage.find_by_id')
    def test_get_mysql_connection_api(self, mock_find_by_id, client):
        """Test retrieving MySQL connection via API."""
        # Setup mock
        fernet = FernetEncrypt()
        encrypted_uri = fernet.encrypt("mysql://user:pass@host:3306/db")
        
        mock_find_by_id.return_value = {
            "id": "mysql-conn-456",
            "alias": "production_mysql",
            "dialect": "mysql",
            "connection_uri": encrypted_uri,
            "schemas": ["prod_schema"],
            "created_at": "2024-05-24T12:00:00"
        }
        
        # Make API request
        response = client.get("/api/database-connections/mysql-conn-456")
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["alias"] == "production_mysql"
        assert data["dialect"] == "mysql"

    @patch('app.data.db.storage.Storage.find_all')
    def test_list_connections_including_mysql(self, mock_find_all, client):
        """Test listing connections includes MySQL connections."""
        # Setup mock with mixed database types
        mock_find_all.return_value = [
            {
                "id": "pg-conn-1",
                "alias": "postgres_db",
                "dialect": "postgresql",
                "connection_uri": "encrypted_pg_uri",
                "schemas": ["public"]
            },
            {
                "id": "mysql-conn-1",
                "alias": "mysql_db",
                "dialect": "mysql",
                "connection_uri": "encrypted_mysql_uri",
                "schemas": ["mysql_schema"]
            },
            {
                "id": "csv-conn-1",
                "alias": "csv_data",
                "dialect": "csv",
                "connection_uri": "csv://data.csv",
                "schemas": []
            }
        ]
        
        # Make API request
        response = client.get("/api/database-connections")
        
        # Verify response
        assert response.status_code == 200
        connections = response.json()
        assert len(connections) == 3
        
        # Find MySQL connection
        mysql_conn = next(c for c in connections if c["dialect"] == "mysql")
        assert mysql_conn["alias"] == "mysql_db"
        assert mysql_conn["id"] == "mysql-conn-1"

    @patch('app.data.db.storage.Storage.find_by_id')
    @patch('app.data.db.storage.Storage.update')
    @patch('app.utils.sql_database.sql_database.create_engine')
    def test_update_mysql_connection_api(self, mock_create_engine, mock_update, mock_find_by_id, client):
        """Test updating MySQL connection via API."""
        # Setup mocks
        connection_id = "mysql-conn-789"
        original_uri = "mysql://olduser:oldpass@oldhost:3306/olddb"
        new_uri = "mysql://newuser:newpass@newhost:3306/newdb"
        
        fernet = FernetEncrypt()
        
        mock_find_by_id.return_value = {
            "id": connection_id,
            "alias": "mysql_to_update",
            "dialect": "mysql",
            "connection_uri": fernet.encrypt(original_uri),
            "schemas": ["old_schema"]
        }
        
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        update_payload = {
            "alias": "updated_mysql",
            "connection_uri": new_uri,
            "schemas": ["new_schema", "test_schema"]
        }
        
        mock_update.return_value = {
            "id": connection_id,
            "alias": "updated_mysql",
            "dialect": "mysql",
            "connection_uri": fernet.encrypt(new_uri),
            "schemas": ["new_schema", "test_schema"]
        }
        
        # Make API request
        response = client.put(f"/api/database-connections/{connection_id}", json=update_payload)
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["alias"] == "updated_mysql"
        assert data["schemas"] == ["new_schema", "test_schema"]

    @patch('app.data.db.storage.Storage.find_by_id')
    @patch('app.data.db.storage.Storage.delete')
    def test_delete_mysql_connection_api(self, mock_delete, mock_find_by_id, client):
        """Test deleting MySQL connection via API."""
        # Setup mocks
        connection_id = "mysql-conn-delete"
        
        mock_find_by_id.return_value = {
            "id": connection_id,
            "alias": "mysql_to_delete",
            "dialect": "mysql",
            "connection_uri": "encrypted_uri",
            "schemas": ["schema1"]
        }
        
        mock_delete.return_value = True
        
        # Make API request
        response = client.delete(f"/api/database-connections/{connection_id}")
        
        # Verify response
        assert response.status_code == 204
        mock_delete.assert_called_once_with("database_connections", connection_id)

    def test_mysql_connection_validation(self, client):
        """Test MySQL connection validation."""
        # Test with invalid URI format
        invalid_payload = {
            "alias": "invalid_mysql",
            "connection_uri": "not_a_valid_uri",
            "schemas": ["test"]
        }
        
        response = client.post("/api/database-connections", json=invalid_payload)
        assert response.status_code == 404  # Based on error handling in the code

    @patch('app.utils.sql_database.sql_database.create_engine')
    def test_mysql_connection_with_special_characters(self, mock_create_engine, client):
        """Test MySQL connection with special characters in password."""
        # Setup mock
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Connection with special characters
        special_char_payload = {
            "alias": "mysql_special_chars",
            "connection_uri": "mysql://user:p@ssw0rd!#$@host:3306/database",
            "schemas": ["default"]
        }
        
        with patch('app.data.db.storage.Storage.insert') as mock_insert:
            mock_insert.return_value = {
                "id": "mysql-special-123",
                **special_char_payload,
                "dialect": "mysql"
            }
            
            response = client.post("/api/database-connections", json=special_char_payload)
            
            # Should handle special characters properly
            assert response.status_code == 201