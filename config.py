# config.py - Enhanced Configuration with Multi-Moderator Support
import os
from dotenv import load_dotenv
from cryptography.fernet import Fernet
from typing import List, Union
import logging

load_dotenv()

class Config:
    """Enhanced configuration class with multi-moderator support"""
    
    # Basic configuration
    TOKEN = os.getenv('DISCORD_TOKEN')
    CREDENTIALS_PATH = os.getenv('GOOGLE_SHEETS_CREDENTIALS_PATH', './credentials.json')
    SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
    GUILD_ID = int(os.getenv('GUILD_ID', 0)) if os.getenv('GUILD_ID') else None
    
    # Multi-Moderator Role Configuration
    # Single moderator role (backwards compatibility)
    MODERATOR_ROLE_ID = int(os.getenv('MODERATOR_ROLE_ID', 0)) if os.getenv('MODERATOR_ROLE_ID') else None
    
    # Multiple moderator roles (new feature)
    # Can be set as comma-separated values in .env: MODERATOR_ROLE_IDS=123456789,987654321,555666777
    _moderator_roles_env = os.getenv('MODERATOR_ROLE_IDS', '')
    if _moderator_roles_env:
        try:
            MODERATOR_ROLE_IDS = [int(role_id.strip()) for role_id in _moderator_roles_env.split(',') if role_id.strip()]
        except ValueError:
            MODERATOR_ROLE_IDS = [MODERATOR_ROLE_ID] if MODERATOR_ROLE_ID else []
    else:
        MODERATOR_ROLE_IDS = [MODERATOR_ROLE_ID] if MODERATOR_ROLE_ID else []
    
    # Hierarchical permissions - if True, any role higher than the moderator role(s) will also have moderator permissions
    HIERARCHICAL_PERMISSIONS = os.getenv('HIERARCHICAL_PERMISSIONS', 'false').lower() == 'true'
    
    # Status channel for persistent messages
    STATUS_CHANNEL_ID = int(os.getenv('STATUS_CHANNEL_ID', 0)) if os.getenv('STATUS_CHANNEL_ID') else None
    
    # Security settings
    RATE_LIMIT_PER_USER = int(os.getenv('RATE_LIMIT_PER_USER', 5))
    RATE_LIMIT_WINDOW = int(os.getenv('RATE_LIMIT_WINDOW', 60))
    
    # Monitoring settings
    HEALTH_CHECK_PORT = int(os.getenv('HEALTH_CHECK_PORT', 8080))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def validate(cls):
        """Validate required configuration with enhanced moderator role checking"""
        required_vars = {
            'TOKEN': cls.TOKEN,
            'CREDENTIALS_PATH': cls.CREDENTIALS_PATH,
            'SPREADSHEET_ID': cls.SPREADSHEET_ID,
        }
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Validate moderator role configuration
        if not cls.MODERATOR_ROLE_IDS:
            logging.warning("No moderator roles configured. Moderator commands will not work.")
        else:
            logging.info(f"Moderator roles configured: {cls.MODERATOR_ROLE_IDS}")
            logging.info(f"Hierarchical permissions: {cls.HIERARCHICAL_PERMISSIONS}")
        
        # Warn about optional STATUS_CHANNEL_ID
        if not cls.STATUS_CHANNEL_ID:
            logging.warning("STATUS_CHANNEL_ID not set - persistent status messages will be disabled")
        
        # Validate file paths
        if not os.path.exists(cls.CREDENTIALS_PATH):
            raise FileNotFoundError(f"Google credentials file not found: {cls.CREDENTIALS_PATH}")
        
        logging.info("Configuration validated successfully")
        
        return True

class SecureConfig:
    """Enhanced security configuration"""
    
    @staticmethod
    def encrypt_token(token: str) -> bytes:
        key = Fernet.generate_key()
        cipher = Fernet(key)
        return cipher.encrypt(token.encode())
    
    @staticmethod
    def decrypt_token(encrypted_token: bytes, key: bytes) -> str:
        cipher = Fernet(key)
        return cipher.decrypt(encrypted_token).decode()
