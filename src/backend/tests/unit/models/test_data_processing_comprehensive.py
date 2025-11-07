"""
Comprehensive unit tests for DataProcessing model.

Tests SQLAlchemy model attributes, initialization, and methods.
"""
import pytest
from datetime import datetime
from unittest.mock import Mock, patch

from src.models.data_processing import DataProcessing
from src.db.base import Base


class TestDataProcessing:
    """Test DataProcessing model."""

    def test_data_processing_table_name(self):
        """Test DataProcessing table name."""
        assert DataProcessing.__tablename__ == "data_processing"

    def test_data_processing_inherits_base(self):
        """Test DataProcessing inherits from Base."""
        assert issubclass(DataProcessing, Base)

    def test_data_processing_columns(self):
        """Test DataProcessing has expected columns."""
        expected_columns = [
            'id', 'che_number', 'processed', 'company_name', 
            'created_at', 'updated_at'
        ]
        
        actual_columns = list(DataProcessing.__table__.columns.keys())
        
        for col in expected_columns:
            assert col in actual_columns

    def test_data_processing_primary_key(self):
        """Test DataProcessing primary key."""
        id_column = DataProcessing.__table__.columns['id']
        
        assert id_column.primary_key is True
        assert id_column.index is True

    def test_data_processing_che_number_constraints(self):
        """Test DataProcessing che_number column constraints."""
        che_number_column = DataProcessing.__table__.columns['che_number']
        
        assert che_number_column.unique is True
        assert che_number_column.index is True
        assert che_number_column.nullable is False

    def test_data_processing_nullable_constraints(self):
        """Test DataProcessing nullable constraints."""
        columns = DataProcessing.__table__.columns
        
        # Required fields
        assert columns['che_number'].nullable is False
        assert columns['processed'].nullable is False
        
        # Optional fields
        assert columns['company_name'].nullable is True

    def test_data_processing_default_values(self):
        """Test DataProcessing column default values."""
        columns = DataProcessing.__table__.columns
        
        # Boolean default
        assert columns['processed'].default.arg is False

    def test_data_processing_datetime_defaults(self):
        """Test DataProcessing datetime columns have defaults."""
        columns = DataProcessing.__table__.columns
        
        created_at = columns['created_at']
        updated_at = columns['updated_at']
        
        assert created_at.default is not None
        assert updated_at.default is not None
        assert updated_at.onupdate is not None

    def test_data_processing_init_minimal(self):
        """Test DataProcessing initialization with minimal data."""
        data_processing = DataProcessing(che_number="CHE123456789")
        
        assert data_processing.che_number == "CHE123456789"
        assert data_processing.processed is False
        assert data_processing.company_name is None

    def test_data_processing_init_full(self):
        """Test DataProcessing initialization with full data."""
        data_processing = DataProcessing(
            che_number="CHE123456789",
            processed=True,
            company_name="Test Company"
        )
        
        assert data_processing.che_number == "CHE123456789"
        assert data_processing.processed is True
        assert data_processing.company_name == "Test Company"

    def test_data_processing_init_processed_none(self):
        """Test DataProcessing initialization sets processed to False when None."""
        data_processing = DataProcessing(
            che_number="CHE123456789",
            processed=None
        )
        
        assert data_processing.processed is False

    def test_data_processing_init_processed_explicit_false(self):
        """Test DataProcessing initialization with explicit False processed."""
        data_processing = DataProcessing(
            che_number="CHE123456789",
            processed=False
        )
        
        assert data_processing.processed is False

    def test_data_processing_init_processed_explicit_true(self):
        """Test DataProcessing initialization with explicit True processed."""
        data_processing = DataProcessing(
            che_number="CHE123456789",
            processed=True
        )
        
        assert data_processing.processed is True

    def test_data_processing_init_calls_super(self):
        """Test DataProcessing __init__ calls super().__init__."""
        with patch.object(Base, '__init__') as mock_super_init:
            data_processing = DataProcessing(che_number="CHE123456789")
            
            mock_super_init.assert_called_once_with(che_number="CHE123456789")

    def test_data_processing_repr_minimal(self):
        """Test DataProcessing __repr__ with minimal data."""
        data_processing = DataProcessing()
        data_processing.id = 1
        data_processing.che_number = "CHE123456789"
        data_processing.processed = False
        data_processing.company_name = None
        
        result = repr(data_processing)
        
        expected = "<DataProcessing(id=1, che_number=CHE123456789, processed=False, company_name=None)>"
        assert result == expected

    def test_data_processing_repr_full(self):
        """Test DataProcessing __repr__ with full data."""
        data_processing = DataProcessing()
        data_processing.id = 2
        data_processing.che_number = "CHE987654321"
        data_processing.processed = True
        data_processing.company_name = "Test Company Ltd"
        
        result = repr(data_processing)
        
        expected = "<DataProcessing(id=2, che_number=CHE987654321, processed=True, company_name=Test Company Ltd)>"
        assert result == expected

    def test_data_processing_repr_none_values(self):
        """Test DataProcessing __repr__ with None values."""
        data_processing = DataProcessing()
        data_processing.id = None
        data_processing.che_number = None
        data_processing.processed = None
        data_processing.company_name = None
        
        result = repr(data_processing)
        
        expected = "<DataProcessing(id=None, che_number=None, processed=None, company_name=None)>"
        assert result == expected

    def test_data_processing_column_types(self):
        """Test DataProcessing column types."""
        columns = DataProcessing.__table__.columns
        
        # Check column types
        assert 'INTEGER' in str(columns['id'].type).upper()
        assert 'VARCHAR' in str(columns['che_number'].type).upper() or 'STRING' in str(columns['che_number'].type).upper()
        assert 'BOOLEAN' in str(columns['processed'].type).upper()
        assert 'VARCHAR' in str(columns['company_name'].type).upper() or 'STRING' in str(columns['company_name'].type).upper()
        assert 'DATETIME' in str(columns['created_at'].type).upper()
        assert 'DATETIME' in str(columns['updated_at'].type).upper()

    def test_data_processing_init_with_datetime_fields(self):
        """Test DataProcessing initialization with datetime fields."""
        now = datetime.utcnow()
        data_processing = DataProcessing(
            che_number="CHE123456789",
            created_at=now,
            updated_at=now
        )
        
        assert data_processing.che_number == "CHE123456789"
        assert data_processing.created_at == now
        assert data_processing.updated_at == now

    def test_data_processing_init_empty_kwargs(self):
        """Test DataProcessing initialization with empty kwargs."""
        data_processing = DataProcessing()
        
        # Should not raise an error
        assert data_processing.processed is False

    def test_data_processing_init_rejects_extra_kwargs(self):
        """Test DataProcessing initialization rejects extra kwargs."""
        # SQLAlchemy should reject unknown kwargs
        with pytest.raises(TypeError, match="invalid keyword argument"):
            DataProcessing(
                che_number="CHE123456789",
                extra_field="ignored"
            )

    def test_data_processing_processed_boolean_values(self):
        """Test DataProcessing processed field with various boolean values."""
        # Test with True
        data_processing_true = DataProcessing(che_number="CHE1", processed=True)
        assert data_processing_true.processed is True
        
        # Test with False
        data_processing_false = DataProcessing(che_number="CHE2", processed=False)
        assert data_processing_false.processed is False
        
        # Test with None (should become False)
        data_processing_none = DataProcessing(che_number="CHE3", processed=None)
        assert data_processing_none.processed is False

    def test_data_processing_che_number_string_values(self):
        """Test DataProcessing che_number field with various string values."""
        test_cases = [
            "CHE123456789",
            "CHE-123-456-789",
            "che123456789",
            "123456789",
            "ABC123XYZ"
        ]
        
        for che_number in test_cases:
            data_processing = DataProcessing(che_number=che_number)
            assert data_processing.che_number == che_number

    def test_data_processing_company_name_string_values(self):
        """Test DataProcessing company_name field with various string values."""
        test_cases = [
            "Test Company",
            "Test Company Ltd",
            "Company with Special Characters !@#$%",
            "Very Long Company Name That Exceeds Normal Length",
            "",  # Empty string
            None  # None value
        ]
        
        for company_name in test_cases:
            data_processing = DataProcessing(
                che_number=f"CHE{hash(str(company_name))}",
                company_name=company_name
            )
            assert data_processing.company_name == company_name
