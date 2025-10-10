"""
Comprehensive unit tests for rate limiter utilities.

Tests TokenBucket, TokenBucketManager classes and utility functions.
"""
import pytest
import time
import threading
from unittest.mock import patch, Mock

from src.utils.rate_limiter import (
    TokenBucket,
    TokenBucketManager,
    token_bucket_manager,
    DEFAULT_ANTHROPIC_INPUT_TPM,
    DEFAULT_ANTHROPIC_OUTPUT_TPM,
    DEFAULT_GOOGLE_INPUT_TPM,
    DEFAULT_GOOGLE_OUTPUT_TPM,
    consume_anthropic_input_tokens,
    consume_anthropic_output_tokens,
    consume_google_input_tokens,
    consume_google_output_tokens
)


class TestTokenBucket:
    """Test TokenBucket class."""

    def test_token_bucket_init_minimal(self):
        """Test TokenBucket initialization with minimal parameters."""
        bucket = TokenBucket(tokens_per_minute=60)

        assert bucket.tokens_per_minute == 60
        assert bucket.max_capacity == 60  # Default to tokens_per_minute
        assert bucket.tokens == 60  # Default to max_capacity
        assert bucket.refill_rate == 1.0  # 60/60 = 1 token per second
        assert hasattr(bucket, 'lock')
        assert bucket.lock is not None

    def test_token_bucket_init_full(self):
        """Test TokenBucket initialization with all parameters."""
        bucket = TokenBucket(
            tokens_per_minute=120,
            max_capacity=200,
            initial_tokens=50
        )
        
        assert bucket.tokens_per_minute == 120
        assert bucket.max_capacity == 200
        assert bucket.tokens == 50
        assert bucket.refill_rate == 2.0  # 120/60 = 2 tokens per second

    def test_token_bucket_init_none_values(self):
        """Test TokenBucket initialization with None values."""
        bucket = TokenBucket(
            tokens_per_minute=60,
            max_capacity=None,
            initial_tokens=None
        )
        
        assert bucket.max_capacity == 60
        assert bucket.tokens == 60

    def test_token_bucket_refill_rate_calculation(self):
        """Test TokenBucket refill rate calculation."""
        test_cases = [
            (60, 1.0),    # 60 tokens/min = 1 token/sec
            (120, 2.0),   # 120 tokens/min = 2 tokens/sec
            (30, 0.5),    # 30 tokens/min = 0.5 tokens/sec
            (3600, 60.0), # 3600 tokens/min = 60 tokens/sec
        ]
        
        for tokens_per_minute, expected_rate in test_cases:
            bucket = TokenBucket(tokens_per_minute)
            assert bucket.refill_rate == expected_rate

    @patch('time.time')
    def test_token_bucket_refill_no_time_elapsed(self, mock_time):
        """Test TokenBucket _refill with no time elapsed."""
        mock_time.return_value = 100.0
        
        bucket = TokenBucket(tokens_per_minute=60, initial_tokens=30)
        initial_tokens = bucket.tokens
        
        bucket._refill()
        
        # No time elapsed, tokens should remain the same
        assert bucket.tokens == initial_tokens

    @patch('time.time')
    def test_token_bucket_refill_with_time_elapsed(self, mock_time):
        """Test TokenBucket _refill with time elapsed."""
        # Start at time 100
        mock_time.return_value = 100.0
        bucket = TokenBucket(tokens_per_minute=60, initial_tokens=30)
        
        # Advance time by 10 seconds
        mock_time.return_value = 110.0
        bucket._refill()
        
        # Should add 10 tokens (1 token/sec * 10 sec)
        assert bucket.tokens == 40.0

    @patch('time.time')
    def test_token_bucket_refill_max_capacity_limit(self, mock_time):
        """Test TokenBucket _refill respects max capacity."""
        # Start at time 100
        mock_time.return_value = 100.0
        bucket = TokenBucket(tokens_per_minute=60, max_capacity=50, initial_tokens=45)
        
        # Advance time by 10 seconds (would add 10 tokens)
        mock_time.return_value = 110.0
        bucket._refill()
        
        # Should be capped at max_capacity
        assert bucket.tokens == 50.0

    @patch('time.time')
    def test_token_bucket_consume_sufficient_tokens(self, mock_time):
        """Test TokenBucket consume with sufficient tokens."""
        mock_time.return_value = 100.0  # Fixed time to avoid refill

        bucket = TokenBucket(tokens_per_minute=60, initial_tokens=50)

        result = bucket.consume(10)

        assert result is True
        assert bucket.tokens == 40.0

    @patch('time.time')
    def test_token_bucket_consume_insufficient_tokens_no_wait(self, mock_time):
        """Test TokenBucket consume with insufficient tokens and no wait."""
        mock_time.return_value = 100.0  # Fixed time to avoid refill

        bucket = TokenBucket(tokens_per_minute=60, initial_tokens=5)

        result = bucket.consume(10, wait=False)

        assert result is False
        assert bucket.tokens == 5.0  # Tokens unchanged

    @patch('src.utils.rate_limiter.time.sleep')
    @patch('src.utils.rate_limiter.time.time')
    def test_token_bucket_consume_insufficient_tokens_with_wait(self, mock_time, mock_sleep):
        """Test TokenBucket consume with insufficient tokens and wait."""
        # Setup time progression: init, _refill, consume check, after sleep, final _refill, final check
        mock_time.side_effect = [100.0, 100.0, 100.0, 105.0, 105.0, 105.0]

        bucket = TokenBucket(tokens_per_minute=60, initial_tokens=5)

        result = bucket.consume(10, wait=True)

        assert result is True
        # Should have waited for 5 seconds (deficit of 5 tokens / 1 token per second)
        mock_sleep.assert_called_once_with(5.0)

    @patch('time.time')
    def test_token_bucket_consume_zero_tokens(self, mock_time):
        """Test TokenBucket consume with zero tokens."""
        mock_time.return_value = 100.0  # Fixed time to avoid refill

        bucket = TokenBucket(tokens_per_minute=60, initial_tokens=10)

        result = bucket.consume(0)

        assert result is True
        assert bucket.tokens == 10.0  # No change

    @patch('time.time')
    def test_token_bucket_consume_exact_tokens(self, mock_time):
        """Test TokenBucket consume with exact token amount."""
        mock_time.return_value = 100.0  # Fixed time to avoid refill

        bucket = TokenBucket(tokens_per_minute=60, initial_tokens=10)

        result = bucket.consume(10)

        assert result is True
        assert bucket.tokens == 0.0


class TestTokenBucketManager:
    """Test TokenBucketManager class."""

    def test_token_bucket_manager_init(self):
        """Test TokenBucketManager initialization."""
        manager = TokenBucketManager()

        assert isinstance(manager.buckets, dict)
        assert len(manager.buckets) == 0
        assert hasattr(manager, 'lock')
        assert manager.lock is not None

    def test_token_bucket_manager_get_bucket_new(self):
        """Test TokenBucketManager get_bucket creates new bucket."""
        manager = TokenBucketManager()
        
        bucket = manager.get_bucket("test-key", 120)
        
        assert isinstance(bucket, TokenBucket)
        assert bucket.tokens_per_minute == 120
        assert "test-key" in manager.buckets
        assert manager.buckets["test-key"] is bucket

    def test_token_bucket_manager_get_bucket_existing(self):
        """Test TokenBucketManager get_bucket returns existing bucket."""
        manager = TokenBucketManager()
        
        # Create first bucket
        bucket1 = manager.get_bucket("test-key", 120)
        
        # Get same bucket again
        bucket2 = manager.get_bucket("test-key", 240)  # Different rate, should be ignored
        
        assert bucket1 is bucket2
        assert bucket1.tokens_per_minute == 120  # Original rate preserved

    def test_token_bucket_manager_consume_tokens_new_bucket(self):
        """Test TokenBucketManager consume_tokens with new bucket."""
        manager = TokenBucketManager()
        
        result = manager.consume_tokens("new-key", 10, 60)
        
        assert result is True
        assert "new-key" in manager.buckets

    @patch('time.time')
    def test_token_bucket_manager_consume_tokens_existing_bucket(self, mock_time):
        """Test TokenBucketManager consume_tokens with existing bucket."""
        mock_time.return_value = 100.0  # Fixed time to avoid refill

        manager = TokenBucketManager()

        # First consumption creates bucket
        result1 = manager.consume_tokens("test-key", 10, 60)

        # Second consumption uses existing bucket
        result2 = manager.consume_tokens("test-key", 20, 120)  # Different rate ignored

        assert result1 is True
        assert result2 is True
        assert manager.buckets["test-key"].tokens == 30.0  # 60 - 10 - 20


class TestGlobalTokenBucketManager:
    """Test global token_bucket_manager instance."""

    def test_global_token_bucket_manager_exists(self):
        """Test global token_bucket_manager instance exists."""
        assert token_bucket_manager is not None
        assert isinstance(token_bucket_manager, TokenBucketManager)


class TestConstants:
    """Test rate limiter constants."""

    def test_default_constants_exist(self):
        """Test default rate limit constants exist."""
        assert isinstance(DEFAULT_ANTHROPIC_INPUT_TPM, int)
        assert isinstance(DEFAULT_ANTHROPIC_OUTPUT_TPM, int)
        assert isinstance(DEFAULT_GOOGLE_INPUT_TPM, int)
        assert isinstance(DEFAULT_GOOGLE_OUTPUT_TPM, int)

    def test_default_constants_values(self):
        """Test default rate limit constant values."""
        assert DEFAULT_ANTHROPIC_INPUT_TPM == 40000
        assert DEFAULT_ANTHROPIC_OUTPUT_TPM == 8000
        assert DEFAULT_GOOGLE_INPUT_TPM == 60000
        assert DEFAULT_GOOGLE_OUTPUT_TPM == 12000


class TestAnthropicTokenFunctions:
    """Test Anthropic token consumption functions."""

    @patch('src.utils.rate_limiter.token_bucket_manager')
    def test_consume_anthropic_input_tokens_default(self, mock_manager):
        """Test consume_anthropic_input_tokens with default settings."""
        mock_manager.consume_tokens.return_value = True
        
        result = consume_anthropic_input_tokens(1000)
        
        assert result is True
        mock_manager.consume_tokens.assert_called_once_with(
            'anthropic-input', 1000, DEFAULT_ANTHROPIC_INPUT_TPM, True
        )

    @patch('src.utils.rate_limiter.token_bucket_manager')
    def test_consume_anthropic_input_tokens_no_wait(self, mock_manager):
        """Test consume_anthropic_input_tokens with wait=False."""
        mock_manager.consume_tokens.return_value = False
        
        result = consume_anthropic_input_tokens(1000, wait=False)
        
        assert result is False
        mock_manager.consume_tokens.assert_called_once_with(
            'anthropic-input', 1000, DEFAULT_ANTHROPIC_INPUT_TPM, False
        )

    @patch('src.utils.rate_limiter.token_bucket_manager')
    def test_consume_anthropic_input_tokens_with_rpm(self, mock_manager):
        """Test consume_anthropic_input_tokens with RPM setting."""
        mock_manager.consume_tokens.return_value = True
        
        result = consume_anthropic_input_tokens(1000, rpm=10)
        
        # RPM=10 * 10000 tokens/request = 100000 TPM, but capped at DEFAULT_ANTHROPIC_INPUT_TPM
        expected_tpm = min(10 * 10000, DEFAULT_ANTHROPIC_INPUT_TPM)
        
        assert result is True
        mock_manager.consume_tokens.assert_called_once_with(
            'anthropic-input', 1000, expected_tpm, True
        )

    @patch('src.utils.rate_limiter.token_bucket_manager')
    def test_consume_anthropic_output_tokens_default(self, mock_manager):
        """Test consume_anthropic_output_tokens with default settings."""
        mock_manager.consume_tokens.return_value = True
        
        result = consume_anthropic_output_tokens(500)
        
        assert result is True
        mock_manager.consume_tokens.assert_called_once_with(
            'anthropic-output', 500, DEFAULT_ANTHROPIC_OUTPUT_TPM, True
        )

    @patch('src.utils.rate_limiter.token_bucket_manager')
    def test_consume_anthropic_output_tokens_with_rpm(self, mock_manager):
        """Test consume_anthropic_output_tokens with RPM setting."""
        mock_manager.consume_tokens.return_value = True
        
        result = consume_anthropic_output_tokens(500, rpm=20)
        
        # RPM=20 * 2000 tokens/request = 40000 TPM, but capped at DEFAULT_ANTHROPIC_OUTPUT_TPM
        expected_tpm = min(20 * 2000, DEFAULT_ANTHROPIC_OUTPUT_TPM)
        
        assert result is True
        mock_manager.consume_tokens.assert_called_once_with(
            'anthropic-output', 500, expected_tpm, True
        )


class TestGoogleTokenFunctions:
    """Test Google token consumption functions."""

    @patch('src.utils.rate_limiter.token_bucket_manager')
    def test_consume_google_input_tokens_default(self, mock_manager):
        """Test consume_google_input_tokens with default settings."""
        mock_manager.consume_tokens.return_value = True
        
        result = consume_google_input_tokens(2000)
        
        assert result is True
        mock_manager.consume_tokens.assert_called_once_with(
            'google-input', 2000, DEFAULT_GOOGLE_INPUT_TPM, True
        )

    @patch('src.utils.rate_limiter.token_bucket_manager')
    def test_consume_google_input_tokens_with_rpm(self, mock_manager):
        """Test consume_google_input_tokens with RPM setting."""
        mock_manager.consume_tokens.return_value = True
        
        result = consume_google_input_tokens(2000, rpm=15)
        
        # RPM=15 * 6000 tokens/request = 90000 TPM, but capped at DEFAULT_GOOGLE_INPUT_TPM
        expected_tpm = min(15 * 6000, DEFAULT_GOOGLE_INPUT_TPM)
        
        assert result is True
        mock_manager.consume_tokens.assert_called_once_with(
            'google-input', 2000, expected_tpm, True
        )

    @patch('src.utils.rate_limiter.token_bucket_manager')
    def test_consume_google_output_tokens_default(self, mock_manager):
        """Test consume_google_output_tokens with default settings."""
        mock_manager.consume_tokens.return_value = True
        
        result = consume_google_output_tokens(800)
        
        assert result is True
        mock_manager.consume_tokens.assert_called_once_with(
            'google-output', 800, DEFAULT_GOOGLE_OUTPUT_TPM, True
        )

    @patch('src.utils.rate_limiter.token_bucket_manager')
    def test_consume_google_output_tokens_with_rpm(self, mock_manager):
        """Test consume_google_output_tokens with RPM setting."""
        mock_manager.consume_tokens.return_value = True
        
        result = consume_google_output_tokens(800, rpm=25)
        
        # RPM=25 * 2000 tokens/request = 50000 TPM, but capped at DEFAULT_GOOGLE_OUTPUT_TPM
        expected_tpm = min(25 * 2000, DEFAULT_GOOGLE_OUTPUT_TPM)
        
        assert result is True
        mock_manager.consume_tokens.assert_called_once_with(
            'google-output', 800, expected_tpm, True
        )


class TestRpmCalculations:
    """Test RPM to TPM calculations."""

    @patch('src.utils.rate_limiter.token_bucket_manager')
    def test_anthropic_rpm_zero_uses_default(self, mock_manager):
        """Test that RPM=0 uses default TPM."""
        mock_manager.consume_tokens.return_value = True
        
        consume_anthropic_input_tokens(1000, rpm=0)
        
        mock_manager.consume_tokens.assert_called_once_with(
            'anthropic-input', 1000, DEFAULT_ANTHROPIC_INPUT_TPM, True
        )

    @patch('src.utils.rate_limiter.token_bucket_manager')
    def test_google_rpm_none_uses_default(self, mock_manager):
        """Test that RPM=None uses default TPM."""
        mock_manager.consume_tokens.return_value = True
        
        consume_google_input_tokens(1000, rpm=None)
        
        mock_manager.consume_tokens.assert_called_once_with(
            'google-input', 1000, DEFAULT_GOOGLE_INPUT_TPM, True
        )

    @patch('src.utils.rate_limiter.token_bucket_manager')
    def test_rpm_calculation_uses_minimum(self, mock_manager):
        """Test that RPM calculation uses minimum of calculated and default."""
        mock_manager.consume_tokens.return_value = True
        
        # Use a very high RPM that would exceed default
        consume_anthropic_input_tokens(1000, rpm=1000)
        
        # Should use default TPM since calculated would be higher
        mock_manager.consume_tokens.assert_called_once_with(
            'anthropic-input', 1000, DEFAULT_ANTHROPIC_INPUT_TPM, True
        )
