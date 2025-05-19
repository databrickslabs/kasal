"""
Rate limiting utilities for managing API request rates.
"""
import threading
import time
from typing import Dict, Optional
import logging

from src.core.logger import LoggerManager

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().crew

class TokenBucket:
    """
    A token bucket rate limiter for managing API rate limits.
    This is designed to handle token-based rate limits for LLM APIs like Anthropic.
    
    The implementation uses a token bucket algorithm where:
    - A bucket has a maximum capacity of tokens
    - Tokens are added to the bucket at a constant rate
    - When a request needs tokens, it takes them from the bucket if available
    - If insufficient tokens, the request waits until enough tokens are available
    """
    
    def __init__(
        self, 
        tokens_per_minute: int,
        max_capacity: Optional[int] = None,
        initial_tokens: Optional[int] = None
    ):
        """
        Initialize the token bucket rate limiter.
        
        Args:
            tokens_per_minute: The rate at which tokens refill (tokens per minute)
            max_capacity: The maximum number of tokens the bucket can hold
            initial_tokens: The initial number of tokens in the bucket
        """
        self.tokens_per_minute = tokens_per_minute
        self.max_capacity = max_capacity if max_capacity is not None else tokens_per_minute
        self.tokens = initial_tokens if initial_tokens is not None else self.max_capacity
        self.last_refill_time = time.time()
        self.lock = threading.RLock()
        
        # Token replenishment rate in tokens per second
        self.refill_rate = tokens_per_minute / 60.0
        
        logger.info(f"TokenBucket initialized with {tokens_per_minute} tokens per minute " 
                   f"({self.refill_rate:.2f} tokens/second), max capacity: {self.max_capacity}")
    
    def _refill(self):
        """Refill the token bucket based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill_time
        
        # Calculate tokens to add based on elapsed time
        tokens_to_add = elapsed * self.refill_rate
        
        if tokens_to_add > 0:
            self.tokens = min(self.tokens + tokens_to_add, self.max_capacity)
            self.last_refill_time = now
            logger.debug(f"Added {tokens_to_add:.2f} tokens, current tokens: {self.tokens:.2f}")
    
    def consume(self, tokens: int, wait: bool = True) -> bool:
        """
        Consume tokens from the bucket.
        
        Args:
            tokens: Number of tokens to consume
            wait: Whether to wait for tokens to be available
            
        Returns:
            True if tokens were consumed, False if not and wait=False
        """
        with self.lock:
            self._refill()
            
            if self.tokens >= tokens:
                # If we have enough tokens, consume them immediately
                self.tokens -= tokens
                logger.debug(f"Consumed {tokens} tokens, remaining: {self.tokens:.2f}")
                return True
            
            if not wait:
                # If we don't have enough tokens and we're not waiting, return False
                logger.debug(f"Not enough tokens ({self.tokens:.2f}) for request ({tokens}), not waiting")
                return False
            
            # Calculate time needed to accumulate required tokens
            deficit = tokens - self.tokens
            wait_time = deficit / self.refill_rate
            
            logger.info(f"Rate limit: Waiting {wait_time:.2f}s for {deficit:.2f} more tokens")
            
            # Release lock while waiting
            self.lock.release()
            try:
                time.sleep(wait_time)
            finally:
                # Reacquire lock
                self.lock.acquire()
            
            # Recalculate after waiting
            self._refill()
            
            # We should have enough tokens now, but double-check
            if self.tokens >= tokens:
                self.tokens -= tokens
                logger.debug(f"After waiting, consumed {tokens} tokens, remaining: {self.tokens:.2f}")
                return True
            else:
                # This shouldn't happen in normal operation
                logger.warning(f"Still not enough tokens after waiting: {self.tokens:.2f}, needed: {tokens}")
                # Try to consume what we can
                self.tokens = 0
                return True  # Return true since we waited as requested

class TokenBucketManager:
    """
    Manages multiple token buckets for different models/providers.
    This allows for different rate limits for different APIs.
    """
    
    def __init__(self):
        self.buckets: Dict[str, TokenBucket] = {}
        self.lock = threading.Lock()
    
    def get_bucket(self, key: str, tokens_per_minute: int) -> TokenBucket:
        """
        Get or create a token bucket for the specified key.
        
        Args:
            key: The identifier for this bucket (e.g., 'anthropic-input', 'anthropic-output')
            tokens_per_minute: Rate limit in tokens per minute
            
        Returns:
            TokenBucket instance
        """
        with self.lock:
            if key not in self.buckets:
                self.buckets[key] = TokenBucket(tokens_per_minute)
            return self.buckets[key]
    
    def consume_tokens(self, key: str, tokens: int, tokens_per_minute: int, wait: bool = True) -> bool:
        """
        Consume tokens from the specified bucket.
        
        Args:
            key: The bucket identifier
            tokens: Number of tokens to consume
            tokens_per_minute: Rate limit for this bucket (used if bucket doesn't exist)
            wait: Whether to wait for tokens to become available
            
        Returns:
            True if tokens were consumed, False otherwise
        """
        bucket = self.get_bucket(key, tokens_per_minute)
        return bucket.consume(tokens, wait)

# Global token bucket manager
token_bucket_manager = TokenBucketManager()

# Default token rate limits based on Anthropic's tier 1 limits
DEFAULT_ANTHROPIC_INPUT_TPM = 40000   # Input tokens per minute
DEFAULT_ANTHROPIC_OUTPUT_TPM = 8000   # Output tokens per minute

# Default token rate limits for Google Gemini
DEFAULT_GOOGLE_INPUT_TPM = 60000    # Input tokens per minute (estimate)
DEFAULT_GOOGLE_OUTPUT_TPM = 12000   # Output tokens per minute (estimate)

def consume_anthropic_input_tokens(tokens: int, wait: bool = True, rpm: Optional[int] = None) -> bool:
    """
    Consume input tokens for Anthropic API requests.
    
    Args:
        tokens: Number of input tokens to consume
        wait: Whether to wait for tokens to become available
        rpm: Optional RPM setting that will be used to calculate TPM
        
    Returns:
        True if tokens were consumed, False otherwise
    """
    # If an RPM setting is provided, use it to calculate TPM
    # Assuming average input tokens per request: ~10,000 tokens per request for Anthropic
    tpm = DEFAULT_ANTHROPIC_INPUT_TPM
    if rpm is not None and rpm > 0:
        # Conservative estimate: 10,000 input tokens per request
        estimated_tpm = rpm * 10000
        # Use the lower of the two values to be safe
        tpm = min(estimated_tpm, DEFAULT_ANTHROPIC_INPUT_TPM)
        logger.info(f"Using max_rpm={rpm} to calculate input TPM={tpm}")
    
    return token_bucket_manager.consume_tokens('anthropic-input', tokens, tpm, wait)

def consume_anthropic_output_tokens(tokens: int, wait: bool = True, rpm: Optional[int] = None) -> bool:
    """
    Consume output tokens for Anthropic API requests.
    
    Args:
        tokens: Number of output tokens to consume
        wait: Whether to wait for tokens to become available
        rpm: Optional RPM setting that will be used to calculate TPM
        
    Returns:
        True if tokens were consumed, False otherwise
    """
    # If an RPM setting is provided, use it to calculate TPM
    # Assuming average output tokens per request: ~2,000 tokens per request for Anthropic
    tpm = DEFAULT_ANTHROPIC_OUTPUT_TPM
    if rpm is not None and rpm > 0:
        # Conservative estimate: 2,000 output tokens per request
        estimated_tpm = rpm * 2000
        # Use the lower of the two values to be safe
        tpm = min(estimated_tpm, DEFAULT_ANTHROPIC_OUTPUT_TPM)
        logger.info(f"Using max_rpm={rpm} to calculate output TPM={tpm}")
    
    return token_bucket_manager.consume_tokens('anthropic-output', tokens, tpm, wait)

def consume_google_input_tokens(tokens: int, wait: bool = True, rpm: Optional[int] = None) -> bool:
    """
    Consume input tokens for Google Gemini API requests.
    
    Args:
        tokens: Number of input tokens to consume
        wait: Whether to wait for tokens to become available
        rpm: Optional RPM setting that will be used to calculate TPM
        
    Returns:
        True if tokens were consumed, False otherwise
    """
    # If an RPM setting is provided, use it to calculate TPM
    # Assuming average input tokens per request: ~6,000 tokens per request for Gemini
    tpm = DEFAULT_GOOGLE_INPUT_TPM
    if rpm is not None and rpm > 0:
        # Conservative estimate: 6,000 input tokens per request
        estimated_tpm = rpm * 6000
        # Use the lower of the two values to be safe
        tpm = min(estimated_tpm, DEFAULT_GOOGLE_INPUT_TPM)
        logger.info(f"Using max_rpm={rpm} to calculate Google input TPM={tpm}")
    
    return token_bucket_manager.consume_tokens('google-input', tokens, tpm, wait)

def consume_google_output_tokens(tokens: int, wait: bool = True, rpm: Optional[int] = None) -> bool:
    """
    Consume output tokens for Google Gemini API requests.
    
    Args:
        tokens: Number of output tokens to consume
        wait: Whether to wait for tokens to become available
        rpm: Optional RPM setting that will be used to calculate TPM
        
    Returns:
        True if tokens were consumed, False otherwise
    """
    # If an RPM setting is provided, use it to calculate TPM
    # Assuming average output tokens per request: ~2,000 tokens per request for Gemini
    tpm = DEFAULT_GOOGLE_OUTPUT_TPM
    if rpm is not None and rpm > 0:
        # Conservative estimate: 2,000 output tokens per request
        estimated_tpm = rpm * 2000
        # Use the lower of the two values to be safe
        tpm = min(estimated_tpm, DEFAULT_GOOGLE_OUTPUT_TPM)
        logger.info(f"Using max_rpm={rpm} to calculate Google output TPM={tpm}")
    
    return token_bucket_manager.consume_tokens('google-output', tokens, tpm, wait) 