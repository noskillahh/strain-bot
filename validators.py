# validators.py - Input validation functions
import re
from typing import Optional

class InputValidator:
    """Input validation for user inputs"""
    
    @staticmethod
    def validate_strain_name(name: str) -> Optional[str]:
        """Validate and clean strain name input"""
        if not name or not isinstance(name, str):
            return None
        
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', name.strip())
        
        # Check length (2-50 characters)
        if not 2 <= len(cleaned) <= 50:
            return None
        
        # Allow letters, numbers, spaces, hyphens, and # symbol
        if not re.match(r'^[a-zA-Z0-9\s\-#\'\"\.]+$', cleaned):
            return None
        
        return cleaned.title()
    
    @staticmethod
    def validate_rating(rating: int) -> bool:
        """Validate rating is within acceptable range"""
        return isinstance(rating, int) and 1 <= rating <= 10
    
    @staticmethod
    def sanitize_user_input(text: str, max_length: int = 100) -> str:
        """General input sanitization"""
        if not text or not isinstance(text, str):
            return ""
        
        # Remove potential harmful characters
        sanitized = re.sub(r'[<>\"\'&]', '', text)
        
        # Limit length
        return sanitized[:max_length].strip()
