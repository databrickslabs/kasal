"""
Unit tests for encryption_utils module.
"""

import os
import pytest
import tempfile
import base64
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
from cryptography.fernet import Fernet

from src.utils.encryption_utils import EncryptionUtils


class TestEncryptionUtils:
    """Test EncryptionUtils class."""
    
    def test_get_key_directory(self):
        """Test get_key_directory creates and returns correct path."""
        with patch('pathlib.Path.home') as mock_home:
            mock_home.return_value = Path("/mock/home")
            
            with patch('pathlib.Path.mkdir') as mock_mkdir:
                result = EncryptionUtils.get_key_directory()
                
                expected_path = Path("/mock/home") / ".backendcrew" / "keys"
                assert result == expected_path
                mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
    
    def test_generate_ssh_key_pair(self):
        """Test SSH key pair generation."""
        private_key, public_key = EncryptionUtils.generate_ssh_key_pair()
        
        assert isinstance(private_key, bytes)
        assert isinstance(public_key, bytes)
        assert b"BEGIN PRIVATE KEY" in private_key
        assert b"BEGIN PUBLIC KEY" in public_key
    
    def test_get_or_create_ssh_keys_existing_keys(self):
        """Test get_or_create_ssh_keys with existing keys."""
        mock_private_key = b"mock_private_key"
        mock_public_key = b"mock_public_key"
        
        with patch.object(EncryptionUtils, 'get_key_directory') as mock_get_dir:
            # Mock directory
            mock_dir = Mock()
            mock_get_dir.return_value = mock_dir
            
            # Mock path objects
            mock_private_path = Mock()
            mock_public_path = Mock()
            
            # Mock the division operator to return our mocked paths
            mock_dir.__truediv__ = Mock(side_effect=lambda x: {
                "private_key.pem": mock_private_path,
                "public_key.pem": mock_public_path
            }[x])
            
            # Configure mocked paths
            mock_private_path.exists.return_value = True
            mock_public_path.exists.return_value = True
            mock_private_path.read_bytes.return_value = mock_private_key
            mock_public_path.read_bytes.return_value = mock_public_key
            
            private_key, public_key = EncryptionUtils.get_or_create_ssh_keys()
            
            assert private_key == mock_private_key
            assert public_key == mock_public_key
    
    def test_get_or_create_ssh_keys_new_keys(self):
        """Test get_or_create_ssh_keys with new key generation."""
        mock_private_key = b"new_private_key"
        mock_public_key = b"new_public_key"
        
        with patch.object(EncryptionUtils, 'get_key_directory') as mock_get_dir, \
             patch.object(EncryptionUtils, 'generate_ssh_key_pair') as mock_generate:
            
            # Mock directory
            mock_dir = Mock()
            mock_get_dir.return_value = mock_dir
            mock_generate.return_value = (mock_private_key, mock_public_key)
            
            # Mock path objects
            mock_private_path = Mock()
            mock_public_path = Mock()
            
            # Mock the division operator to return our mocked paths
            mock_dir.__truediv__ = Mock(side_effect=lambda x: {
                "private_key.pem": mock_private_path,
                "public_key.pem": mock_public_path
            }[x])
            
            # Configure mocked paths - keys don't exist initially
            mock_private_path.exists.return_value = False
            mock_public_path.exists.return_value = False
            
            private_key, public_key = EncryptionUtils.get_or_create_ssh_keys()
            
            assert private_key == mock_private_key
            assert public_key == mock_public_key
            mock_private_path.write_bytes.assert_called_once_with(mock_private_key)
            mock_public_path.write_bytes.assert_called_once_with(mock_public_key)
    
    def test_get_encryption_key_with_env_var(self):
        """Test get_encryption_key with environment variable set."""
        test_key = "test_encryption_key"
        
        with patch.dict(os.environ, {'ENCRYPTION_KEY': test_key}):
            result = EncryptionUtils.get_encryption_key()
            
            assert result == test_key.encode()
    
    def test_get_encryption_key_without_env_var(self):
        """Test get_encryption_key without environment variable."""
        with patch.dict(os.environ, {}, clear=True), \
             patch('src.utils.encryption_utils.Fernet.generate_key') as mock_generate:
            
            mock_key = b"generated_key"
            mock_generate.return_value = mock_key
            
            result = EncryptionUtils.get_encryption_key()
            
            assert result == mock_key
    
    def test_encrypt_with_ssh(self):
        """Test encrypt_with_ssh method."""
        test_value = "test_secret_value"
        mock_private_key = b"mock_private_key"
        mock_public_key = b"mock_public_key"
        
        with patch('src.utils.encryption_utils.EncryptionUtils.get_or_create_ssh_keys') as mock_get_keys:
            mock_get_keys.return_value = (mock_private_key, mock_public_key)
            
            # Mock the cryptography operations
            with patch('src.utils.encryption_utils.serialization.load_pem_public_key') as mock_load_public, \
                 patch('src.utils.encryption_utils.Fernet') as mock_fernet_class:
                
                # Mock public key
                mock_public_key_obj = Mock()
                mock_load_public.return_value = mock_public_key_obj
                
                # Mock symmetric encryption
                mock_symmetric_key = b"symmetric_key"
                mock_encrypted_value = b"encrypted_value"
                mock_fernet = Mock()
                mock_fernet.encrypt.return_value = mock_encrypted_value
                mock_fernet_class.return_value = mock_fernet
                
                # Mock RSA encryption
                mock_encrypted_key = b"encrypted_symmetric_key"
                mock_public_key_obj.encrypt.return_value = mock_encrypted_key
                
                with patch('src.utils.encryption_utils.Fernet.generate_key', return_value=mock_symmetric_key):
                    result = EncryptionUtils.encrypt_with_ssh(test_value)
                    
                    assert isinstance(result, str)
                    # Should be base64 encoded
                    decoded = base64.b64decode(result.encode())
                    assert b":" in decoded
    
    def test_decrypt_with_ssh(self):
        """Test decrypt_with_ssh method."""
        test_value = "test_secret_value"
        
        # Create a mock encrypted value (base64 encoded combination)
        mock_encrypted_key = base64.b64encode(b"encrypted_key")
        mock_encrypted_data = b"encrypted_data"
        mock_combined = mock_encrypted_key + b":" + mock_encrypted_data
        mock_encrypted_value = base64.b64encode(mock_combined).decode()
        
        mock_private_key = b"mock_private_key"
        mock_public_key = b"mock_public_key"
        
        with patch('src.utils.encryption_utils.EncryptionUtils.get_or_create_ssh_keys') as mock_get_keys:
            mock_get_keys.return_value = (mock_private_key, mock_public_key)
            
            with patch('src.utils.encryption_utils.serialization.load_pem_private_key') as mock_load_private, \
                 patch('src.utils.encryption_utils.Fernet') as mock_fernet_class:
                
                # Mock private key
                mock_private_key_obj = Mock()
                mock_load_private.return_value = mock_private_key_obj
                
                # Mock RSA decryption
                # Use a valid 32-byte Fernet key
                mock_symmetric_key = Fernet.generate_key()  # This generates a valid key
                mock_private_key_obj.decrypt.return_value = mock_symmetric_key
                
                # Mock symmetric decryption
                mock_fernet = Mock()
                mock_fernet.decrypt.return_value = test_value.encode()
                mock_fernet_class.return_value = mock_fernet
                
                result = EncryptionUtils.decrypt_with_ssh(mock_encrypted_value)
                
                assert result == test_value
    
    def test_decrypt_with_ssh_exception(self):
        """Test decrypt_with_ssh with exception handling."""
        invalid_encrypted_value = "invalid_encrypted_value"
        
        with patch('src.utils.encryption_utils.EncryptionUtils.get_or_create_ssh_keys') as mock_get_keys:
            mock_get_keys.side_effect = Exception("Key error")
            
            result = EncryptionUtils.decrypt_with_ssh(invalid_encrypted_value)
            
            assert result == ""
    
    def test_is_ssh_encrypted_valid(self):
        """Test is_ssh_encrypted with valid SSH encrypted value."""
        # Create a mock encrypted value format
        mock_encrypted_key = base64.b64encode(b"encrypted_key")
        mock_encrypted_data = b"encrypted_data"
        mock_combined = mock_encrypted_key + b":" + mock_encrypted_data
        mock_encrypted_value = base64.b64encode(mock_combined).decode()
        
        result = EncryptionUtils.is_ssh_encrypted(mock_encrypted_value)
        
        assert result is True
    
    def test_is_ssh_encrypted_invalid(self):
        """Test is_ssh_encrypted with invalid value."""
        invalid_value = "not_ssh_encrypted"
        
        result = EncryptionUtils.is_ssh_encrypted(invalid_value)
        
        assert result is False
    
    def test_encrypt_value_ssh_success(self):
        """Test encrypt_value using SSH encryption successfully."""
        test_value = "test_value"
        expected_result = "ssh_encrypted_result"
        
        # We need to patch at the module level, not the class level
        with patch.object(EncryptionUtils, 'encrypt_with_ssh', return_value=expected_result) as mock_ssh_encrypt:
            result = EncryptionUtils.encrypt_value(test_value)
            
            assert result == expected_result
            mock_ssh_encrypt.assert_called_once_with(test_value)
    
    def test_encrypt_value_ssh_fallback_to_fernet(self):
        """Test encrypt_value falling back to Fernet when SSH fails."""
        test_value = "test_value"
        expected_result = b"fernet_encrypted_result"
        
        with patch.object(EncryptionUtils, 'encrypt_with_ssh', side_effect=Exception("SSH error")) as mock_ssh_encrypt, \
             patch.object(EncryptionUtils, 'get_encryption_key') as mock_get_key, \
             patch('src.utils.encryption_utils.Fernet') as mock_fernet_class:
            
            # Fernet encryption succeeds
            mock_key = Fernet.generate_key()  # Use a valid key
            mock_get_key.return_value = mock_key
            mock_fernet = Mock()
            mock_fernet.encrypt.return_value = expected_result
            mock_fernet_class.return_value = mock_fernet
            
            result = EncryptionUtils.encrypt_value(test_value)
            
            assert result == expected_result.decode()
            mock_fernet.encrypt.assert_called_once_with(test_value.encode())
    
    def test_decrypt_value_ssh_encrypted(self):
        """Test decrypt_value with SSH encrypted value."""
        test_encrypted_value = "ssh_encrypted_value"
        expected_result = "decrypted_value"
        
        with patch.object(EncryptionUtils, 'is_ssh_encrypted', return_value=True) as mock_is_ssh, \
             patch.object(EncryptionUtils, 'decrypt_with_ssh', return_value=expected_result) as mock_ssh_decrypt:
            
            result = EncryptionUtils.decrypt_value(test_encrypted_value)
            
            assert result == expected_result
            mock_ssh_decrypt.assert_called_once_with(test_encrypted_value)
    
    def test_decrypt_value_fernet_encrypted(self):
        """Test decrypt_value with Fernet encrypted value."""
        test_encrypted_value = "fernet_encrypted_value"
        expected_result = "decrypted_value"
        
        with patch.object(EncryptionUtils, 'is_ssh_encrypted', return_value=False) as mock_is_ssh, \
             patch.object(EncryptionUtils, 'get_encryption_key') as mock_get_key, \
             patch('src.utils.encryption_utils.Fernet') as mock_fernet_class:
            
            # Fernet decryption
            mock_key = Fernet.generate_key()  # Use a valid key
            mock_get_key.return_value = mock_key
            mock_fernet = Mock()
            mock_fernet.decrypt.return_value = expected_result.encode()
            mock_fernet_class.return_value = mock_fernet
            
            result = EncryptionUtils.decrypt_value(test_encrypted_value)
            
            assert result == expected_result
            mock_fernet.decrypt.assert_called_once_with(test_encrypted_value.encode())
    
    def test_decrypt_value_exception(self):
        """Test decrypt_value with exception handling."""
        test_encrypted_value = "invalid_encrypted_value"
        
        with patch('src.utils.encryption_utils.EncryptionUtils.is_ssh_encrypted') as mock_is_ssh:
            mock_is_ssh.side_effect = Exception("Decryption error")
            
            result = EncryptionUtils.decrypt_value(test_encrypted_value)
            
            assert result == ""
    
    def test_encrypt_with_ssh_exception(self):
        """Test encrypt_with_ssh when an exception occurs."""
        test_value = "test_value"
        
        with patch.object(EncryptionUtils, 'get_or_create_ssh_keys', side_effect=Exception("Key generation error")):
            with pytest.raises(Exception, match="Key generation error"):
                EncryptionUtils.encrypt_with_ssh(test_value)


class TestEncryptionIntegration:
    """Test integration scenarios for encryption utilities."""
    
    def test_encrypt_decrypt_roundtrip_ssh(self):
        """Test encrypt/decrypt roundtrip using SSH encryption."""
        test_value = "test_secret_data"
        
        with patch('src.utils.encryption_utils.EncryptionUtils.get_or_create_ssh_keys') as mock_get_keys:
            # Generate actual keys for this test
            private_key, public_key = EncryptionUtils.generate_ssh_key_pair()
            mock_get_keys.return_value = (private_key, public_key)
            
            # Encrypt the value
            encrypted = EncryptionUtils.encrypt_with_ssh(test_value)
            assert encrypted != test_value
            assert isinstance(encrypted, str)
            
            # Decrypt the value
            decrypted = EncryptionUtils.decrypt_with_ssh(encrypted)
            assert decrypted == test_value
    
    def test_encrypt_decrypt_roundtrip_through_main_methods(self):
        """Test encrypt/decrypt roundtrip through main encrypt_value/decrypt_value methods."""
        test_value = "test_secret_data"
        
        with patch('src.utils.encryption_utils.EncryptionUtils.get_or_create_ssh_keys') as mock_get_keys:
            # Generate actual keys for this test
            private_key, public_key = EncryptionUtils.generate_ssh_key_pair()
            mock_get_keys.return_value = (private_key, public_key)
            
            # Encrypt using main method
            encrypted = EncryptionUtils.encrypt_value(test_value)
            assert encrypted != test_value
            
            # Decrypt using main method
            decrypted = EncryptionUtils.decrypt_value(encrypted)
            assert decrypted == test_value
    
    def test_fernet_encryption_fallback_roundtrip(self):
        """Test Fernet encryption fallback roundtrip."""
        test_value = "test_secret_data"
        test_key = Fernet.generate_key()
        
        with patch('src.utils.encryption_utils.EncryptionUtils.encrypt_with_ssh') as mock_ssh_encrypt, \
             patch('src.utils.encryption_utils.EncryptionUtils.get_encryption_key') as mock_get_key:
            
            # Force SSH encryption to fail
            mock_ssh_encrypt.side_effect = Exception("SSH failed")
            mock_get_key.return_value = test_key
            
            # Encrypt using main method (should fall back to Fernet)
            encrypted = EncryptionUtils.encrypt_value(test_value)
            assert encrypted != test_value
            
            # Mock the SSH detection to return False for Fernet encrypted data
            with patch('src.utils.encryption_utils.EncryptionUtils.is_ssh_encrypted', return_value=False):
                # Decrypt using main method
                decrypted = EncryptionUtils.decrypt_value(encrypted)
                assert decrypted == test_value