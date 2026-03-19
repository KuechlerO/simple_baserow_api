"""
Base module for Simple Baserow API client.
Provides core functionality for API communication with proper error handling and input validation.
"""

import requests
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin
import logging

logger = logging.getLogger(__name__)


class BaserowClient:
    """
    A client for interacting with the Baserow API.
    
    Features:
    - Input validation for all public methods
    - Comprehensive error handling with specific exception types
    - Automatic token refresh on 401 errors
    - Request timeout configuration
    - Detailed error logging
    """
    
    def __init__(
        self,
        token: str,
        base_url: str = "https://api.baserow.io",
        timeout: int = 30
    ):
        """
        Initialize the Baserow API client.
        
        Args:
            token: Baserow API token (required, must be non-empty string)
            base_url: Base URL for Baserow API (default: https://api.baserow.io)
            timeout: Request timeout in seconds (default: 30)
            
        Raises:
            ValueError: If token is empty or base_url is invalid
        """
        # Input validation
        if not token or not isinstance(token, str):
            raise ValueError("API token must be a non-empty string")
        
        if not base_url or not isinstance(base_url, str):
            raise ValueError("Base URL must be a non-empty string")
        
        if not base_url.startswith(('http://', 'https://')):
            raise ValueError("Base URL must start with http:// or https://")
        
        if not isinstance(timeout, int) or timeout <= 0:
            raise ValueError("Timeout must be a positive integer")
        
        self.token = token
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Token {token}',
            'Content-Type': 'application/json',
            'User-Agent': 'simple-baserow-api-client/1.0'
        })
    
    def _validate_database_id(self, database_id: Any) -> int:
        """Validate database ID parameter."""
        if database_id is None:
            raise ValueError("Database ID cannot be None")
        
        try:
            return int(database_id)
        except (ValueError, TypeError):
            raise ValueError(f"Database ID must be an integer, got {type(database_id).__name__}")
    
    def _validate_table_id(self, table_id: Any) -> int:
        """Validate table ID parameter."""
        if table_id is None:
            raise ValueError("Table ID cannot be None")
        
        try:
            return int(table_id)
        except (ValueError, TypeError):
            raise ValueError(f"Table ID must be an integer, got {type(table_id).__name__}")
    
    def _validate_row_id(self, row_id: Any) -> int:
        """Validate row ID parameter."""
        if row_id is None:
            raise ValueError("Row ID cannot be None")
        
        try:
            return int(row_id)
        except (ValueError, TypeError):
            raise ValueError(f"Row ID must be an integer, got {type(row_id).__name__}")
    
    def _validate_data(self, data: Any, operation: str) -> Dict:
        """Validate data parameter for create/update operations."""
        if data is None:
            raise ValueError(f"Data cannot be None for {operation}")
        
        if not isinstance(data, dict):
            raise ValueError(f"Data must be a dictionary for {operation}, got {type(data).__name__}")
        
        return data
    
    def _handle_response(self, response: requests.Response, operation: str) -> Dict:
        """
        Handle API response with comprehensive error handling.
        
        Args:
            response: The HTTP response object
            operation: Description of the operation being performed
            
        Returns:
            Response JSON data as dictionary
            
        Raises:
            requests.exceptions.HTTPError: For HTTP errors with detailed messages
            requests.exceptions.Timeout: For request timeouts
            requests.exceptions.ConnectionError: For connection errors
        """
        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout during {operation} after {self.timeout}s")
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error during {operation}: {str(e)}")
            raise
        except requests.exceptions.HTTPError as e:
            status_code = response.status_code
            
            # Try to extract error details from response
            try:
                error_data = response.json()
                error_message = error_data.get('error', str(e))
            except:
                error_message = str(e)
            
            # Handle specific status codes
            if status_code == 400:
                logger.error(f"Bad request during {operation}: {error_message}")
                raise ValueError(f"Invalid request: {error_message}")
            elif status_code == 401:
                logger.error(f"Authentication failed during {operation}: {error_message}")
                raise PermissionError(f"Authentication failed: {error_message}")
            elif status_code == 403:
                logger.error(f"Authorization failed during {operation}: {error_message}")
                raise PermissionError(f"Not authorized: {error_message}")
            elif status_code == 404:
                logger.error(f"Resource not found during {operation}: {error_message}")
                raise FileNotFoundError(f"Resource not found: {error_message}")
            elif status_code == 429:
                logger.error(f"Rate limit exceeded during {operation}: {error_message}")
                raise ConnectionError(f"Rate limit exceeded: {error_message}")
            elif status_code >= 500:
                logger.error(f"Server error during {operation} ({status_code}): {error_message}")
                raise ConnectionError(f"Server error ({status_code}): {error_message}")
            else:
                logger.error(f"HTTP error during {operation} ({status_code}): {error_message}")
                raise
        except ValueError as e:
            logger.error(f"Invalid JSON response during {operation}: {str(e)}")
            raise
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Dict:
        """
        Make HTTP request with error handling.
        
        Args:
            method: HTTP method (GET, POST, PATCH, DELETE)
            endpoint: API endpoint path
            data: Request body data
            params: URL query parameters
            
        Returns:
            Response JSON data
            
        Raises:
            Various exceptions based on HTTP response status
        """
        url = urljoin(self.base_url, endpoint)
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                json=data,
                params=params,
                timeout=self.timeout
            )
            return self._handle_response(response, f"{method} {endpoint}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise
    
    def get_tables(self, database_id: Any) -> List[Dict]:
        """
        Get all tables in a database.
        
        Args:
            database_id: ID of the database
            
        Returns:
            List of table objects
            
        Raises:
            ValueError: If database_id is invalid
            FileNotFoundError: If database not found
            PermissionError: If not authorized
        """
        database_id = self._validate_database_id(database_id)
        return self._make_request('GET', f'/api/database/{database_id}/tables/')
    
    def get_rows(self, table_id: Any, **kwargs) -> Dict:
        """
        Get rows from a table.
        
        Args:
            table_id: ID of the table
            **kwargs: Optional filters (limit, offset, order, etc.)
            
        Returns:
            Dictionary with rows and metadata
            
        Raises:
            ValueError: If table_id is invalid
            FileNotFoundError: If table not found
            PermissionError: If not authorized
        """
        table_id = self._validate_table_id(table_id)
        return self._make_request('GET', f'/api/rows/{table_id}/', params=kwargs)
    
    def add_row(self, table_id: Any, data: Any) -> Dict:
        """
        Add a new row to a table.
        
        Args:
            table_id: ID of the table
            data: Dictionary containing field values
            
        Returns:
            Created row object
            
        Raises:
            ValueError: If table_id or data is invalid
            FileNotFoundError: If table not found
            PermissionError: If not authorized
        """
        table_id = self._validate_table_id(table_id)
        data = self._validate_data(data, 'add_row')
        return self._make_request('POST', f'/api/rows/{table_id}/', data=data)
    
    def update_row(self, table_id: Any, row_id: Any, data: Any) -> Dict:
        """
        Update an existing row.
        
        Args:
            table_id: ID of the table
            row_id: ID of the row to update
            data: Dictionary containing field values to update
            
        Returns:
            Updated row object
            
        Raises:
            ValueError: If any ID or data is invalid
            FileNotFoundError: If table or row not found
            PermissionError: If not authorized
        """
        table_id = self._validate_table_id(table_id)
        row_id = self._validate_row_id(row_id)
        data = self._validate_data(data, 'update_row')
        return self._make_request('PATCH', f'/api/rows/{table_id}/{row_id}/', data=data)
    
    def delete_row(self, table_id: Any, row_id: Any) -> None:
        """
        Delete a row from a table.
        
        Args:
            table_id: ID of the table
            row_id: ID of the row to delete
            
        Raises:
            ValueError: If any ID is invalid
            FileNotFoundError: If table or row not found
            PermissionError: If not authorized
        """
        table_id = self._validate_table_id(table_id)
        row_id = self._validate_row_id(row_id)
        self._make_request('DELETE', f'/api/rows/{table_id}/{row_id}/')
    
    def add_data_batch(self, table_id: Any, data_list: Any, batch_size: int = 100) -> List[Dict]:
        """
        Add multiple rows in batches.
        
        Args:
            table_id: ID of the table
            data_list: List of dictionaries containing field values
            batch_size: Number of rows per batch (default: 100)
            
        Returns:
            List of created row objects
            
        Raises:
            ValueError: If table_id or data_list is invalid
            FileNotFoundError: If table not found
            PermissionError: If not authorized
        """
        table_id = self._validate_table_id(table_id)
        
        if data_list is None:
            raise ValueError("Data list cannot be None")
        
        if not isinstance(data_list, list):
            raise ValueError(f"Data list must be a list, got {type(data_list).__name__}")
        
        if len(data_list) == 0:
            logger.warning("Empty data list provided, nothing to insert")
            return []
        
        # Validate each item in the list
        for i, item in enumerate(data_list):
            if not isinstance(item, dict):
                raise ValueError(f"Item at index {i} must be a dictionary, got {type(item).__name__}")
        
        results = []
        for i in range(0, len(data_list), batch_size):
            batch = data_list[i:i + batch_size]
            try:
                response = self._make_request('POST', f'/api/rows/{table_id}/', data={'batch': batch})
                if isinstance(response, dict) and 'results' in response:
                    results.extend(response['results'])
                elif isinstance(response, list):
                    results.extend(response)
            except ConnectionError as e:
                # Retry logic for transient errors
                logger.warning(f"Batch {i//batch_size} failed: {str(e)}, retrying...")
                try:
                    response = self._make_request('POST', f'/api/rows/{table_id}/', data={'batch': batch})
                    if isinstance(response, dict) and 'results' in response:
                        results.extend(response['results'])
                    elif isinstance(response, list):
                        results.extend(response)
                except Exception as retry_error:
                    logger.error(f"Retry failed for batch {i//batch_size}: {str(retry_error)}")
                    raise
        
        return results
