"""
Comprehensive unit tests for cron utilities.

Tests ensure_utc, calculate_next_run, and calculate_next_run_from_last functions.
"""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, Mock
import croniter

from src.utils.cron_utils import (
    ensure_utc,
    calculate_next_run,
    calculate_next_run_from_last
)


class TestEnsureUtc:
    """Test ensure_utc function."""

    def test_ensure_utc_none_input(self):
        """Test ensure_utc with None input."""
        result = ensure_utc(None)
        
        assert result is None

    def test_ensure_utc_naive_datetime(self):
        """Test ensure_utc with naive datetime."""
        naive_dt = datetime(2023, 1, 1, 12, 0, 0)
        
        result = ensure_utc(naive_dt)
        
        assert result.tzinfo == timezone.utc
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 1
        assert result.hour == 12
        assert result.minute == 0
        assert result.second == 0

    def test_ensure_utc_utc_datetime(self):
        """Test ensure_utc with UTC datetime."""
        utc_dt = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        result = ensure_utc(utc_dt)
        
        assert result.tzinfo == timezone.utc
        assert result == utc_dt

    def test_ensure_utc_timezone_aware_datetime(self):
        """Test ensure_utc with timezone-aware datetime."""
        # Create a timezone offset of +5 hours
        tz_offset = timezone(timedelta(hours=5))
        tz_dt = datetime(2023, 1, 1, 17, 0, 0, tzinfo=tz_offset)
        
        result = ensure_utc(tz_dt)
        
        assert result.tzinfo == timezone.utc
        # 17:00 +5 should become 12:00 UTC
        assert result.hour == 12

    def test_ensure_utc_preserves_date_components(self):
        """Test ensure_utc preserves date components correctly."""
        test_cases = [
            datetime(2023, 12, 31, 23, 59, 59),
            datetime(2024, 2, 29, 0, 0, 0),  # Leap year
            datetime(2023, 6, 15, 12, 30, 45),
        ]
        
        for dt in test_cases:
            result = ensure_utc(dt)
            
            assert result.tzinfo == timezone.utc
            assert result.year == dt.year
            assert result.month == dt.month
            assert result.day == dt.day
            assert result.hour == dt.hour
            assert result.minute == dt.minute
            assert result.second == dt.second


class TestCalculateNextRun:
    """Test calculate_next_run function."""

    @patch('src.utils.cron_utils.datetime')
    def test_calculate_next_run_no_base_time(self, mock_datetime):
        """Test calculate_next_run with no base time (uses now)."""
        mock_now = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        
        # Mock croniter
        with patch('src.utils.cron_utils.croniter.croniter') as mock_croniter:
            mock_cron = Mock()
            mock_cron.get_next.return_value = datetime(2023, 1, 1, 13, 0, 0)
            mock_croniter.return_value = mock_cron
            
            result = calculate_next_run("0 13 * * *")
            
            assert isinstance(result, datetime)
            assert result.tzinfo is None  # Should be timezone-naive
            mock_croniter.assert_called_once_with("0 13 * * *", mock_now)

    def test_calculate_next_run_with_naive_base_time(self):
        """Test calculate_next_run with naive base time."""
        base_time = datetime(2023, 1, 1, 10, 0, 0)
        
        with patch('src.utils.cron_utils.croniter.croniter') as mock_croniter:
            mock_cron = Mock()
            mock_cron.get_next.return_value = datetime(2023, 1, 1, 11, 0, 0)
            mock_croniter.return_value = mock_cron
            
            result = calculate_next_run("0 11 * * *", base_time)
            
            assert isinstance(result, datetime)
            assert result.tzinfo is None
            mock_croniter.assert_called_once_with("0 11 * * *", base_time)

    def test_calculate_next_run_with_timezone_aware_base_time(self):
        """Test calculate_next_run with timezone-aware base time."""
        tz_offset = timezone(timedelta(hours=5))
        base_time = datetime(2023, 1, 1, 15, 0, 0, tzinfo=tz_offset)
        
        with patch('src.utils.cron_utils.croniter.croniter') as mock_croniter:
            mock_cron = Mock()
            mock_cron.get_next.return_value = datetime(2023, 1, 1, 11, 0, 0)
            mock_croniter.return_value = mock_cron
            
            result = calculate_next_run("0 11 * * *", base_time)
            
            assert isinstance(result, datetime)
            assert result.tzinfo is None
            # Should convert timezone-aware to naive
            expected_naive = base_time.astimezone().replace(tzinfo=None)
            mock_croniter.assert_called_once_with("0 11 * * *", expected_naive)

    def test_calculate_next_run_invalid_cron_expression(self):
        """Test calculate_next_run with invalid cron expression."""
        base_time = datetime(2023, 1, 1, 10, 0, 0)
        
        with patch('src.utils.cron_utils.croniter.croniter') as mock_croniter:
            mock_croniter.side_effect = Exception("Invalid cron expression")
            
            with pytest.raises(ValueError, match="Invalid cron expression"):
                calculate_next_run("invalid cron", base_time)

    def test_calculate_next_run_common_cron_expressions(self):
        """Test calculate_next_run with common cron expressions."""
        base_time = datetime(2023, 1, 1, 10, 0, 0)
        
        test_cases = [
            ("0 12 * * *", "Daily at noon"),
            ("0 0 * * 0", "Weekly on Sunday"),
            ("0 0 1 * *", "Monthly on 1st"),
            ("*/15 * * * *", "Every 15 minutes"),
        ]
        
        for cron_expr, description in test_cases:
            with patch('src.utils.cron_utils.croniter.croniter') as mock_croniter:
                mock_cron = Mock()
                mock_cron.get_next.return_value = datetime(2023, 1, 1, 12, 0, 0)
                mock_croniter.return_value = mock_cron
                
                result = calculate_next_run(cron_expr, base_time)
                
                assert isinstance(result, datetime)
                assert result.tzinfo is None
                mock_croniter.assert_called_once_with(cron_expr, base_time)


class TestCalculateNextRunFromLast:
    """Test calculate_next_run_from_last function."""

    @patch('src.utils.cron_utils.datetime')
    def test_calculate_next_run_from_last_no_last_run(self, mock_datetime):
        """Test calculate_next_run_from_last with no last run."""
        mock_now = datetime(2023, 1, 1, 10, 0, 0)
        mock_now_utc = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.side_effect = [mock_now, mock_now_utc]
        
        with patch('src.utils.cron_utils.croniter.croniter') as mock_croniter:
            mock_cron = Mock()
            mock_cron.get_next.return_value = datetime(2023, 1, 1, 12, 0, 0)
            mock_croniter.return_value = mock_cron
            
            result = calculate_next_run_from_last("0 12 * * *", None)
            
            assert isinstance(result, datetime)
            assert result.tzinfo is None

    @patch('src.utils.cron_utils.datetime')
    def test_calculate_next_run_from_last_with_past_last_run(self, mock_datetime):
        """Test calculate_next_run_from_last with past last run."""
        mock_now = datetime(2023, 1, 1, 10, 0, 0)
        mock_now_utc = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.side_effect = [mock_now, mock_now_utc]
        
        last_run = datetime(2023, 1, 1, 8, 0, 0)  # Past time
        
        with patch('src.utils.cron_utils.croniter.croniter') as mock_croniter:
            mock_cron = Mock()
            mock_cron.get_next.return_value = datetime(2023, 1, 1, 12, 0, 0)
            mock_croniter.return_value = mock_cron
            
            result = calculate_next_run_from_last("0 12 * * *", last_run)
            
            assert isinstance(result, datetime)
            assert result.tzinfo is None

    @patch('src.utils.cron_utils.datetime')
    def test_calculate_next_run_from_last_with_future_last_run(self, mock_datetime):
        """Test calculate_next_run_from_last with future last run."""
        mock_now = datetime(2023, 1, 1, 10, 0, 0)
        mock_now_utc = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.side_effect = [mock_now, mock_now_utc]
        
        last_run = datetime(2023, 1, 1, 12, 0, 0)  # Future time
        
        with patch('src.utils.cron_utils.calculate_next_run') as mock_calc:
            mock_calc.return_value = datetime(2023, 1, 1, 13, 0, 0)
            
            result = calculate_next_run_from_last("0 13 * * *", last_run)
            
            assert isinstance(result, datetime)
            assert result.tzinfo is None
            mock_calc.assert_called_once_with("0 13 * * *", last_run)

    @patch('src.utils.cron_utils.datetime')
    def test_calculate_next_run_from_last_timezone_aware_last_run(self, mock_datetime):
        """Test calculate_next_run_from_last with timezone-aware last run."""
        mock_now = datetime(2023, 1, 1, 10, 0, 0)
        mock_now_utc = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.side_effect = [mock_now, mock_now_utc]
        
        tz_offset = timezone(timedelta(hours=5))
        last_run = datetime(2023, 1, 1, 13, 0, 0, tzinfo=tz_offset)  # 8:00 local time
        
        with patch('src.utils.cron_utils.croniter.croniter') as mock_croniter:
            mock_cron = Mock()
            mock_cron.get_next.return_value = datetime(2023, 1, 1, 12, 0, 0)
            mock_croniter.return_value = mock_cron
            
            result = calculate_next_run_from_last("0 12 * * *", last_run)
            
            assert isinstance(result, datetime)
            assert result.tzinfo is None

    @patch('src.utils.cron_utils.datetime')
    def test_calculate_next_run_from_last_today_schedule_available(self, mock_datetime):
        """Test calculate_next_run_from_last finds today's schedule."""
        mock_now = datetime(2023, 1, 1, 10, 0, 0)
        mock_now_utc = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.side_effect = [mock_now, mock_now_utc]
        
        with patch('src.utils.cron_utils.croniter.croniter') as mock_croniter:
            mock_cron = Mock()
            # Return a time later today
            next_run = datetime(2023, 1, 1, 14, 0, 0)
            mock_cron.get_next.return_value = next_run
            mock_croniter.return_value = mock_cron
            
            result = calculate_next_run_from_last("0 14 * * *", None)
            
            assert isinstance(result, datetime)
            assert result.tzinfo is None

    @patch('src.utils.cron_utils.datetime')
    def test_calculate_next_run_from_last_exception_handling(self, mock_datetime):
        """Test calculate_next_run_from_last handles exceptions."""
        mock_now = datetime(2023, 1, 1, 10, 0, 0)
        mock_now_utc = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.side_effect = [mock_now, mock_now_utc]
        
        with patch('src.utils.cron_utils.croniter.croniter') as mock_croniter:
            mock_croniter.side_effect = Exception("Croniter error")
            
            with patch('src.utils.cron_utils.calculate_next_run') as mock_calc:
                mock_calc.return_value = datetime(2023, 1, 1, 12, 0, 0)
                
                result = calculate_next_run_from_last("0 12 * * *", None)
                
                assert isinstance(result, datetime)
                assert result.tzinfo is None
                mock_calc.assert_called_once_with("0 12 * * *", mock_now)

    def test_calculate_next_run_from_last_function_exists(self):
        """Test calculate_next_run_from_last function exists and is callable."""
        assert callable(calculate_next_run_from_last)
        
        # Test function signature
        import inspect
        sig = inspect.signature(calculate_next_run_from_last)
        params = list(sig.parameters.keys())
        
        assert 'cron_expression' in params
        assert 'last_run' in params


class TestCronUtilsIntegration:
    """Integration tests for cron utilities."""

    def test_all_functions_exist(self):
        """Test all expected functions exist."""
        assert callable(ensure_utc)
        assert callable(calculate_next_run)
        assert callable(calculate_next_run_from_last)

    def test_functions_return_correct_types(self):
        """Test functions return expected types."""
        # Test ensure_utc
        result = ensure_utc(datetime(2023, 1, 1))
        assert isinstance(result, datetime)
        
        # Test calculate_next_run with mocking
        with patch('src.utils.cron_utils.croniter.croniter') as mock_croniter:
            mock_cron = Mock()
            mock_cron.get_next.return_value = datetime(2023, 1, 1, 12, 0, 0)
            mock_croniter.return_value = mock_cron
            
            result = calculate_next_run("0 12 * * *")
            assert isinstance(result, datetime)

    def test_timezone_handling_consistency(self):
        """Test timezone handling is consistent across functions."""
        # All functions should return timezone-naive datetimes for database storage
        naive_dt = datetime(2023, 1, 1, 12, 0, 0)
        utc_dt = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        # ensure_utc should return timezone-aware
        result1 = ensure_utc(naive_dt)
        assert result1.tzinfo is not None
        
        # calculate_next_run should return timezone-naive
        with patch('src.utils.cron_utils.croniter.croniter') as mock_croniter:
            mock_cron = Mock()
            mock_cron.get_next.return_value = datetime(2023, 1, 1, 13, 0, 0)
            mock_croniter.return_value = mock_cron
            
            result2 = calculate_next_run("0 13 * * *", naive_dt)
            assert result2.tzinfo is None
