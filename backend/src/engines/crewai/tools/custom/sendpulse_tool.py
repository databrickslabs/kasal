from typing import Optional, Dict, Any
import os
import tempfile
import hashlib
import logging
import json
import requests
import httpx
from pysendpulse.pysendpulse import PySendPulse
from langchain.tools import BaseTool
from ..schemas import SendPulseEmailOutput, EmailContent, EmailSender, EmailRecipient
from pydantic import ConfigDict, PrivateAttr
import backoff

logger = logging.getLogger(__name__)

class SendPulseEmailTool(BaseTool):
    """Tool for sending emails using SendPulse API"""
    name: str = "SendPulseEmailTool"
    description: str = "A tool for sending emails using the SendPulse email service"
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    # Use PrivateAttr for non-Pydantic fields
    _api_client: PySendPulse = PrivateAttr()
    _default_from: EmailSender = PrivateAttr()
    _default_to: EmailRecipient = PrivateAttr()
    _token_dir: str = PrivateAttr()
    
    def __init__(self, api_id: str, api_secret: str, default_from_name: str, default_from_email: str, 
                 default_to_name: str, default_to_email: str):
        """Initialize SendPulse email tool with API credentials and default email settings"""
        super().__init__()
        
        logger.info(f"Initializing SendPulseEmailTool with sender: {default_from_name} <{default_from_email}> and recipient: {default_to_name} <{default_to_email}>")
        
        # Create a temporary directory for token storage
        self._token_dir = tempfile.mkdtemp(prefix='sendpulse_')
        
        # Generate token filename - PySendPulse will append the hash to this base name
        token_filename = "token"  # Base name without extension
        token_path = os.path.join(self._token_dir, token_filename)
        
        logger.info(f"Using token storage directory: {self._token_dir}")
        logger.info(f"Token base path: {token_path}")
        
        # Calculate the actual filename that PySendPulse will use
        m = hashlib.md5()
        m.update(f"{api_id}::{api_secret}".encode('utf-8'))
        actual_token_path = f"{token_path}{m.hexdigest()}"
        logger.info(f"Expected actual token path: {actual_token_path}")
        
        # Create token file with proper permissions
        try:
            with open(actual_token_path, 'w') as f:
                f.write('')  # Create empty file
            # Set proper permissions (read/write for owner only)
            os.chmod(actual_token_path, 0o600)
            logger.info("Created token file with proper permissions")
        except Exception as e:
            logger.error(f"Failed to create token file: {str(e)}")
            raise
        
        try:
            logger.info(f"Initializing SendPulse client with API ID: {api_id[:4]}...")
            # Initialize with file storage using base token path (PySendPulse will append hash)
            self._api_client = PySendPulse(api_id, api_secret, "FILE", token_file_path=token_path)
            
            # Test authentication by getting token
            logger.info("Attempting to get authentication token...")
            token = self._api_client._PySendPulse__get_token()
            if not token:
                raise Exception("Failed to get authentication token")
            logger.info("Successfully obtained authentication token")
            
            # Verify token was written to file
            if not os.path.exists(actual_token_path) or os.path.getsize(actual_token_path) == 0:
                raise Exception("Token file was not created or is empty")
            logger.info("Token file exists and contains data")
                
            logger.info("Successfully authenticated with SendPulse API")
            
        except Exception as e:
            logger.error(f"Failed to initialize SendPulse client: {str(e)}")
            if hasattr(e, 'response'):
                logger.error(f"API Response: {e.response.text if hasattr(e.response, 'text') else e.response}")
            raise
            
        self._default_from = EmailSender(name=default_from_name, email=default_from_email)
        self._default_to = EmailRecipient(name=default_to_name, email=default_to_email)
    
    def __del__(self):
        """Cleanup temporary directory when the tool is destroyed"""
        try:
            if hasattr(self, '_token_dir') and os.path.exists(self._token_dir):
                for f in os.listdir(self._token_dir):
                    os.remove(os.path.join(self._token_dir, f))
                os.rmdir(self._token_dir)
        except:
            pass
    
    def _run(self, subject: str, html: str, text: Optional[str] = None) -> str:
        """Execute the tool's main functionality"""
        try:
            # If input is a JSON string, parse it
            if isinstance(subject, str) and subject.startswith('{'):
                try:
                    data = json.loads(subject)
                    subject = data.get('subject', '')
                    html = data.get('html', '')
                    text = data.get('text')
                    logger.info(f"Parsed JSON input - Subject: {subject}")
                except json.JSONDecodeError as e:
                    error_msg = f"Invalid JSON input: {str(e)}"
                    logger.error(error_msg)
                    return str(SendPulseEmailOutput(
                        success=False,
                        message_id=None,
                        error=error_msg
                    ).model_dump())

            # Create email content in SendPulse format
            email_dict = {
                'subject': subject,
                'html': html,
                'text': text if text else '',
                'from': {
                    'name': self._default_from.name,
                    'email': self._default_from.email
                },
                'to': [
                    {
                        'name': self._default_to.name,
                        'email': self._default_to.email
                    }
                ]
            }
            
            logger.info(f"Preparing email with content: {email_dict}")
            
            # Ensure we have a valid token before sending
            if not self._api_client._PySendPulse__token:
                logger.info("No token found, getting new token...")
                token = self._api_client._PySendPulse__get_token()
                if not token:
                    raise Exception("Failed to get authentication token")
                logger.info("Successfully obtained new token")
            
            # Send email
            try:
                # Log request details
                logger.info("Making API request to SendPulse...")
                
                # Call the API with the email object
                logger.info("Attempting to send email...")
                logger.info(f"Using email data: {email_dict}")
                
                # Try sending with just the required fields
                simplified_email = {
                    'html': email_dict['html'],
                    'text': email_dict['text'],
                    'subject': email_dict['subject'],
                    'from': {'email': email_dict['from']['email']},
                    'to': [{'email': email_dict['to'][0]['email']}]
                }
                logger.info(f"Sending simplified email: {simplified_email}")
                response = self._api_client.smtp_send_mail(simplified_email)
                logger.info(f"Raw SendPulse API response: {response}")
                
                # Check if we got a data wrapper in response
                if isinstance(response, dict):
                    if 'data' in response:
                        response = response['data']
                    
                    # Log detailed error information
                    if response.get('is_error', False):
                        error_code = response.get('http_code')
                        error_msg = response.get('message', 'Unknown error')
                        logger.error(f"SendPulse API error {error_code}: {error_msg}")
                        logger.error(f"Full error response: {response}")
                        
                        if error_code == 403:
                            logger.error(f"Permission denied for domain {sender_domain}. This could be due to:")
                            logger.error("1. Domain not verified in SendPulse")
                            logger.error("2. Account not activated")
                            logger.error("3. Insufficient API permissions")
                            logger.error("4. Account restrictions")
                            logger.error("Please verify the domain in SendPulse dashboard and ensure account is active")
                
            except Exception as e:
                error_msg = f"SendPulse API call failed: {str(e)}"
                logger.error(error_msg)
                if hasattr(e, 'response'):
                    logger.error(f"API Response: {e.response.text if hasattr(e.response, 'text') else e.response}")
                return str(SendPulseEmailOutput(
                    success=False,
                    message_id=None,
                    error=error_msg
                ).model_dump())
            
            if isinstance(response, dict):
                # Check for error in data wrapper
                if response.get('is_error', False):
                    error_msg = f"API Error {response.get('http_code')}: {response.get('message', 'Unknown error')}"
                    logger.error(f"Email sending failed with response: {response}")
                    output = SendPulseEmailOutput(
                        success=False,
                        message_id=None,
                        error=error_msg
                    )
                else:
                    output = SendPulseEmailOutput(
                        success=True,
                        message_id=str(response.get('id')),
                        error=None
                    )
                    logger.info(f"Email sent successfully with ID: {response.get('id')}")
            else:
                error_msg = f"Unexpected API response format: {response}"
                logger.error(error_msg)
                output = SendPulseEmailOutput(
                    success=False,
                    message_id=None,
                    error=error_msg
                )
                
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(error_msg)
            output = SendPulseEmailOutput(
                success=False,
                message_id=None,
                error=error_msg
            )
        
        return str(output.model_dump()) 