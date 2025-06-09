"""
Encryption utilities module.

This module provides utilities for encrypting and decrypting sensitive data.
"""

import os
import logging
import base64
from typing import Tuple
from pathlib import Path
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend

# Initialize logger
logger = logging.getLogger(__name__)


class EncryptionUtils:
    """Utility class for encryption and decryption operations."""
    
    @staticmethod
    def get_key_directory() -> Path:
        """Get the directory where SSH keys are stored"""
        # Use a directory in the user's home directory
        home_dir = Path.home()
        key_dir = home_dir / ".backendcrew" / "keys"
        key_dir.mkdir(parents=True, exist_ok=True)
        return key_dir

    @staticmethod
    def generate_ssh_key_pair() -> Tuple[bytes, bytes]:
        """Generate a new RSA key pair for encryption"""
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        
        # Serialize private key
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        # Serialize public key
        public_key_bytes = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        
        return private_key_bytes, public_key_bytes

    @staticmethod
    def get_or_create_ssh_keys() -> Tuple[bytes, bytes]:
        """Get existing SSH keys or create new ones if they don't exist"""
        key_dir = EncryptionUtils.get_key_directory()
        private_key_path = key_dir / "private_key.pem"
        public_key_path = key_dir / "public_key.pem"
        
        # Check if keys already exist
        if private_key_path.exists() and public_key_path.exists():
            private_key = private_key_path.read_bytes()
            public_key = public_key_path.read_bytes()
        else:
            # Generate new keys
            private_key, public_key = EncryptionUtils.generate_ssh_key_pair()
            # Save keys to files
            private_key_path.write_bytes(private_key)
            public_key_path.write_bytes(public_key)
            logger.info("Generated new SSH key pair for encryption")
        
        return private_key, public_key

    @staticmethod
    def get_encryption_key() -> bytes:
        """Get or generate a Fernet encryption key (for backward compatibility)"""
        key = os.getenv("ENCRYPTION_KEY")
        if not key:
            # Generate a key and warn that it's not persisted
            key = Fernet.generate_key().decode()
            logger.warning(
                "ENCRYPTION_KEY environment variable not set. "
                "Generated a temporary key. Keys will not persist across restarts."
            )
        return key.encode() if isinstance(key, str) else key

    @staticmethod
    def encrypt_with_ssh(value: str) -> str:
        """Encrypt a value using RSA public key encryption"""
        try:
            _, public_key_bytes = EncryptionUtils.get_or_create_ssh_keys()
            public_key = serialization.load_pem_public_key(
                public_key_bytes,
                backend=default_backend()
            )
            
            # RSA can only encrypt limited data size, so we'll use a hybrid approach
            # Generate a symmetric key
            symmetric_key = Fernet.generate_key()
            f = Fernet(symmetric_key)
            
            # Encrypt the value with the symmetric key
            encrypted_value = f.encrypt(value.encode())
            
            # Encrypt the symmetric key with the public key
            encrypted_key = public_key.encrypt(
                symmetric_key,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            
            # Combine the encrypted key and value, with a separator
            combined = base64.b64encode(encrypted_key) + b":" + encrypted_value
            return base64.b64encode(combined).decode()
        except Exception as e:
            logger.error(f"Error encrypting value with SSH key: {str(e)}")
            raise

    @staticmethod
    def decrypt_with_ssh(encrypted_value: str) -> str:
        """Decrypt a value using RSA private key encryption"""
        try:
            private_key_bytes, _ = EncryptionUtils.get_or_create_ssh_keys()
            private_key = serialization.load_pem_private_key(
                private_key_bytes,
                password=None,
                backend=default_backend()
            )
            
            # Decode the combined value
            combined = base64.b64decode(encrypted_value.encode())
            encrypted_key, encrypted_data = combined.split(b":", 1)
            encrypted_key = base64.b64decode(encrypted_key)
            
            # Decrypt the symmetric key
            symmetric_key = private_key.decrypt(
                encrypted_key,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            
            # Use the symmetric key to decrypt the value
            f = Fernet(symmetric_key)
            decrypted_value = f.decrypt(encrypted_data).decode()
            return decrypted_value
        except Exception as e:
            logger.error(f"Error decrypting value with SSH key: {str(e)}")
            return ""

    @staticmethod
    def is_ssh_encrypted(value: str) -> bool:
        """Check if a value is encrypted with SSH keys"""
        try:
            # Try to decode as base64 and split
            combined = base64.b64decode(value.encode())
            parts = combined.split(b":", 1)
            return len(parts) == 2
        except:
            return False

    @staticmethod
    def encrypt_value(value: str) -> str:
        """Encrypt a value using SSH key encryption or Fernet for backward compatibility"""
        try:
            # Use SSH encryption
            return EncryptionUtils.encrypt_with_ssh(value)
        except Exception as e:
            logger.error(f"Error with SSH encryption, falling back to Fernet: {str(e)}")
            # Fall back to Fernet encryption
            f = Fernet(EncryptionUtils.get_encryption_key())
            return f.encrypt(value.encode()).decode()

    @staticmethod
    def decrypt_value(encrypted_value: str) -> str:
        """Decrypt a value using the appropriate method"""
        try:
            # Check if the value is encrypted with SSH keys
            if EncryptionUtils.is_ssh_encrypted(encrypted_value):
                return EncryptionUtils.decrypt_with_ssh(encrypted_value)
            else:
                # Fall back to Fernet decryption
                f = Fernet(EncryptionUtils.get_encryption_key())
                return f.decrypt(encrypted_value.encode()).decode()
        except Exception as e:
            logger.error(f"Error decrypting value: {str(e)}")
            return "" 