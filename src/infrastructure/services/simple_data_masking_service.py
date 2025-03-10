# src/infrastructure/services/simple_data_masking_service.py
import logging
import hashlib
import re
from typing import Dict, Any, List, Union

from src.domain.interfaces.repositories import DataMaskingService


class SimpleDataMaskingService(DataMaskingService):
    """Simple implementation of DataMaskingService for masking sensitive data."""
    
    def __init__(self, salt: str = None):
        self.logger = logging.getLogger(__name__)
        # Salt for hashing. If not provided, a static salt is used.
        # In production, this should be a secure secret.
        self.salt = salt or "financial-market-data-masking-salt"
        
        # Default masking rules
        self.default_masking_rules = {
            "email": self._mask_email,
            "phone": self._mask_phone,
            "credit_card": self._mask_credit_card,
            "ssn": self._mask_ssn,
            "personal_id": self._mask_personal_id,
            "name": self._mask_name,
            "hash": self._hash_value
        }
    
    def mask_sensitive_data(self, data: Dict[str, Any], fields_to_mask: List[str]) -> Dict[str, Any]:
        """Mask sensitive data fields."""
        if not data or not fields_to_mask:
            return data
        
        try:
            result = data.copy()
            
            for field_spec in fields_to_mask:
                # Parse field specification (can be field_name or field_name:rule)
                if ":" in field_spec:
                    field_name, rule = field_spec.split(":", 1)
                else:
                    field_name, rule = field_spec, "default"
                
                # Apply masking to the field
                self._apply_masking(result, field_name, rule)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error masking sensitive data: {str(e)}")
            # Return original data if masking fails to avoid data loss
            return data
    
    def _apply_masking(self, data: Dict[str, Any], field_path: str, rule: str) -> None:
        """Apply masking to a specific field in the data dict."""
        # Support for nested fields (using dot notation)
        parts = field_path.split(".")
        
        # Navigate to the parent object
        current = data
        for i in range(len(parts) - 1):
            part = parts[i]
            
            # Handle list index notation (field[0])
            match = re.match(r"(.*)\[(\d+)\]", part)
            if match:
                field_name, index = match.groups()
                index = int(index)
                
                if field_name not in current:
                    return  # Field doesn't exist
                
                if not isinstance(current[field_name], list) or index >= len(current[field_name]):
                    return  # Index out of range
                
                current = current[field_name][index]
            else:
                if part not in current:
                    return  # Field doesn't exist
                
                current = current[part]
        
        # Get the actual field name (last part)
        field = parts[-1]
        
        # Handle list index notation in the final field
        match = re.match(r"(.*)\[(\d+)\]", field)
        if match:
            field_name, index = match.groups()
            index = int(index)
            
            if field_name not in current:
                return  # Field doesn't exist
            
            if not isinstance(current[field_name], list) or index >= len(current[field_name]):
                return  # Index out of range
            
            # Apply masking rule
            current[field_name][index] = self._apply_masking_rule(current[field_name][index], rule)
        else:
            if field not in current:
                return  # Field doesn't exist
            
            # Apply masking rule
            current[field] = self._apply_masking_rule(current[field], rule)
    
    def _apply_masking_rule(self, value: Any, rule: str) -> Any:
        """Apply a specific masking rule to a value."""
        # Skip None values
        if value is None:
            return None
        
        # Handle different data types
        if isinstance(value, (list, tuple)):
            return [self._apply_masking_rule(item, rule) for item in value]
        
        if isinstance(value, dict):
            return {k: self._apply_masking_rule(v, rule) for k, v in value.items()}
        
        # Convert to string for masking
        value_str = str(value)
        
        # Apply rule
        if rule == "default":
            # Choose rule based on field content
            if self._looks_like_email(value_str):
                return self._mask_email(value_str)
            elif self._looks_like_phone(value_str):
                return self._mask_phone(value_str)
            elif self._looks_like_credit_card(value_str):
                return self._mask_credit_card(value_str)
            elif self._looks_like_ssn(value_str):
                return self._mask_ssn(value_str)
            else:
                # Default to partial masking for unknown types
                return self._mask_partial(value_str)
        elif rule in self.default_masking_rules:
            return self.default_masking_rules[rule](value_str)
        elif rule == "remove":
            return "[REMOVED]"
        else:
            # Default to partial masking for unknown rules
            return self._mask_partial(value_str)
    
    def _looks_like_email(self, value: str) -> bool:
        """Check if a value looks like an email."""
        return re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", value) is not None
    
    def _looks_like_phone(self, value: str) -> bool:
        """Check if a value looks like a phone number."""
        # Remove common phone number separators
        clean_value = re.sub(r"[\s\-\(\)\.]", "", value)
        return clean_value.isdigit() and 7 <= len(clean_value) <= 15
    
    def _looks_like_credit_card(self, value: str) -> bool:
        """Check if a value looks like a credit card number."""
        # Remove spaces and dashes
        clean_value = re.sub(r"[\s\-]", "", value)
        return clean_value.isdigit() and 13 <= len(clean_value) <= 19
    
    def _looks_like_ssn(self, value: str) -> bool:
        """Check if a value looks like a US Social Security Number."""
        # Remove dashes
        clean_value = re.sub(r"[\s\-]", "", value)
        return clean_value.isdigit() and len(clean_value) == 9
    
    def _mask_email(self, value: str) -> str:
        """Mask an email address."""
        if not value or "@" not in value:
            return value
        
        username, domain = value.split("@", 1)
        
        if len(username) <= 2:
            masked_username = "*" * len(username)
        else:
            masked_username = username[0] + "*" * (len(username) - 2) + username[-1]
        
        return f"{masked_username}@{domain}"
    
    def _mask_phone(self, value: str) -> str:
        """Mask a phone number."""
        # Remove common phone number separators
        clean_value = re.sub(r"[\s\-\(\)\.]", "", value)
        
        if len(clean_value) <= 4:
            return "*" * len(value)
        
        # Keep the last 4 digits, mask the rest
        masked = "*" * (len(clean_value) - 4) + clean_value[-4:]
        
        # Try to preserve original format
        if len(masked) == len(clean_value):
            result = ""
            clean_index = 0
            
            for char in value:
                if char in " -().":
                    result += char
                else:
                    result += masked[clean_index]
                    clean_index += 1
            
            return result
        
        return masked
    
    def _mask_credit_card(self, value: str) -> str:
        """Mask a credit card number."""
        # Remove spaces and dashes
        clean_value = re.sub(r"[\s\-]", "", value)
        
        if len(clean_value) <= 4:
            return "*" * len(value)
        
        # Keep the last 4 digits, mask the rest
        masked = "*" * (len(clean_value) - 4) + clean_value[-4:]
        
        # Try to preserve original format
        if len(masked) == len(clean_value):
            result = ""
            clean_index = 0
            
            for char in value:
                if char in " -":
                    result += char
                else:
                    result += masked[clean_index]
                    clean_index += 1
            
            return result
        
        return masked
    
    def _mask_ssn(self, value: str) -> str:
        """Mask a Social Security Number."""
        # Remove dashes
        clean_value = re.sub(r"[\s\-]", "", value)
        
        if len(clean_value) <= 4:
            return "*" * len(value)
        
        # Keep the last 4 digits, mask the rest
        masked = "*" * (len(clean_value) - 4) + clean_value[-4:]
        
        # Try to preserve original format
        if len(masked) == len(clean_value):
            result = ""
            clean_index = 0
            
            for char in value:
                if char in " -":
                    result += char
                else:
                    result += masked[clean_index]
                    clean_index += 1
            
            return result
        
        return masked
    
    def _mask_personal_id(self, value: str) -> str:
        """Mask a personal ID (passport, driver's license, etc.)."""
        if len(value) <= 4:
            return "*" * len(value)
        
        # Keep first and last characters, mask the middle
        return value[0] + "*" * (len(value) - 2) + value[-1]
    
    def _mask_name(self, value: str) -> str:
        """Mask a name."""
        parts = value.split()
        masked_parts = []
        
        for part in parts:
            if len(part) <= 1:
                masked_parts.append(part)
            else:
                masked_parts.append(part[0] + "*" * (len(part) - 1))
        
        return " ".join(masked_parts)
    
    def _mask_partial(self, value: str) -> str:
        """General partial masking for unknown types."""
        if len(value) <= 4:
            return "*" * len(value)
        
        # Mask the middle third
        third = len(value) // 3
        return value[:third] + "*" * third + value[2*third:]
    
    def _hash_value(self, value: str) -> str:
        """Hash a value."""
        # Add salt and hash
        salted = f"{value}{self.salt}"
        return hashlib.sha256(salted.encode()).hexdigest()