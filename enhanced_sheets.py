# enhanced_sheets.py - Enhanced Google Sheets manager v5 with Producer Support
import asyncio
import time
import secrets
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Optional, Tuple, List
import gspread
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class RateLimiter:
    """Rate limiter for API calls"""
    
    def __init__(self, max_requests: int, time_window: int):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
    
    def wait_if_needed(self):
        now = time.time()
        self.requests = [req_time for req_time in self.requests if now - req_time < self.time_window]
        
        if len(self.requests) >= self.max_requests:
            sleep_time = self.time_window - (now - self.requests[0]) + 0.1
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        self.requests.append(now)

class OptimizedSheetsManager:
    """Enhanced Google Sheets manager with categories, producer support and proper username handling"""
    
    def __init__(self, credentials_path: str, spreadsheet_id: str):
        self.credentials_path = credentials_path
        self.spreadsheet_id = spreadsheet_id
        self.gc = None
        self.spreadsheet = None
        self._lock = asyncio.Lock()
        self._rate_limiter = RateLimiter(90, 60)  # 90 requests per minute
        self.executor = ThreadPoolExecutor(max_workers=3)
        
        # Categories
        self.valid_categories = ['flower', 'hash', 'rosin']
        
        # Caching
        self.cache: Dict[str, Tuple[Any, float]] = {}
        self.cache_ttl = 300  # 5 minutes
        
        self._initialize_sheets()
    
    def _initialize_sheets(self):
        """Initialize Google Sheets connection and ensure proper schema"""
        try:
            self.gc = gspread.service_account(filename=self.credentials_path)
            self.spreadsheet = self.gc.open_by_key(self.spreadsheet_id)
            logger.info("Google Sheets connection initialized successfully")
            
            # Ensure sheets have proper headers
            self._ensure_sheet_headers()
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets: {e}", exc_info=True)
            raise
    
    def _ensure_sheet_headers(self):
        """Ensure all sheets have the correct headers with username, producer columns, and producers sheet"""
        try:
            # Ensure Strains sheet has correct headers including Producer
            strains_sheet = self.spreadsheet.worksheet("Strains")
            headers = strains_sheet.row_values(1)
            expected_strains_headers = [
                "Unique_ID", "Strain_Name", "Status", "Average_Rating", "Total_Ratings",
                "Date_Added", "Harvest_Date", "Package_Date", "Category", "Producer"
            ]
            if not headers or len(headers) < len(expected_strains_headers):
                strains_sheet.clear()
                strains_sheet.append_row(expected_strains_headers)
                logger.info("Updated Strains sheet headers with Producer column")
            
            # Ensure Ratings sheet has correct headers including Username
            try:
                ratings_sheet = self.spreadsheet.worksheet("Ratings")
            except gspread.exceptions.WorksheetNotFound:
                ratings_sheet = self.spreadsheet.add_worksheet(title="Ratings", rows="10000", cols="6")
            
            ratings_headers = ratings_sheet.row_values(1)
            expected_ratings_headers = [
                "Rating_ID", "Unique_ID", "User_ID", "Rating", "Date_Rated", "Username"
            ]
            if not ratings_headers or len(ratings_headers) < len(expected_ratings_headers):
                ratings_sheet.clear()
                ratings_sheet.append_row(expected_ratings_headers)
                logger.info("Updated Ratings sheet headers")
            
            # Ensure Submissions sheet has correct headers including Username and Producer
            try:
                submissions_sheet = self.spreadsheet.worksheet("Submissions")
            except gspread.exceptions.WorksheetNotFound:
                submissions_sheet = self.spreadsheet.add_worksheet(title="Submissions", rows="1000", cols="10")
            
            submissions_headers = submissions_sheet.row_values(1)
            expected_submissions_headers = [
                "Submission_ID", "Unique_ID", "Strain_Name", "User_ID", 
                "Harvest_Date", "Package_Date", "Date_Added", "Category", "Producer", "Username"
            ]
            if not submissions_headers or len(submissions_headers) < len(expected_submissions_headers):
                submissions_sheet.clear()
                submissions_sheet.append_row(expected_submissions_headers)
                logger.info("Updated Submissions sheet headers with Producer and Username columns")
            
            # NEW: Ensure Producers sheet exists for persistent producer storage
            try:
                producers_sheet = self.spreadsheet.worksheet("Producers")
            except gspread.exceptions.WorksheetNotFound:
                producers_sheet = self.spreadsheet.add_worksheet(title="Producers", rows="100", cols="2")
                # Add headers
                producers_sheet.append_row(["Producer_Name", "Date_Added"])
                # Add default producers
                default_producers = [
                    "Hollandse Hoogtes",
                    "Q-Farms", 
                    "Fyta",
                    "Aardachtig",
                    "Canadelaar",
                    "Holigram"
                ]
                for producer in default_producers:
                    producers_sheet.append_row([producer, datetime.now().strftime("%Y-%m-%d")])
                logger.info("Created Producers sheet with default producers")
            
            # Ensure Producers sheet has correct headers
            producers_headers = producers_sheet.row_values(1)
            if not producers_headers or len(producers_headers) < 2:
                # If headers are missing, add them but preserve existing data
                existing_data = producers_sheet.get_all_values()
                producers_sheet.clear()
                producers_sheet.append_row(["Producer_Name", "Date_Added"])
                # Re-add existing data if any (skip if first row was headers)
                for row in existing_data:
                    if row and len(row) > 0 and row[0] and row[0] != "Producer_Name":
                        # Add date if missing
                        if len(row) < 2:
                            row.append(datetime.now().strftime("%Y-%m-%d"))
                        producers_sheet.append_row(row)
                logger.info("Updated Producers sheet headers")
                
        except Exception as e:
            logger.error(f"Error ensuring sheet headers: {e}")
    
    async def get_all_producers(self) -> List[str]:
        """Get all valid producers from the Producers sheet"""
        def operation():
            try:
                producers_sheet = self.spreadsheet.worksheet("Producers")
                records = producers_sheet.get_all_records()
                
                # Extract just the producer names, filter out empty ones
                producers = [
                    str(record.get('Producer_Name', '')).strip() 
                    for record in records 
                    if record.get('Producer_Name', '').strip()
                ]
                
                # Remove duplicates and sort
                producers = sorted(list(set(producers)))
                
                # If no producers found, return defaults
                if not producers:
                    default_producers = [
                        "Hollandse Hoogtes", "Q-Farms", "Fyta", 
                        "Aardachtig", "Canadelaar", "Holigram"
                    ]
                    logger.warning("No producers found in sheet, returning defaults")
                    return default_producers
                
                return producers
                
            except gspread.exceptions.WorksheetNotFound:
                # Sheet doesn't exist, return defaults
                logger.warning("Producers sheet not found, returning defaults")
                return ["Hollandse Hoogtes", "Q-Farms", "Fyta", "Aardachtig", "Canadelaar", "Holigram"]
            except Exception as e:
                logger.error(f"Error getting producers: {e}")
                return ["Hollandse Hoogtes", "Q-Farms", "Fyta", "Aardachtig", "Canadelaar", "Holigram"]
        
        return await self.safe_operation(operation)
    
    async def add_producer(self, producer_name: str) -> bool:
        """Add a new producer to the Producers sheet"""
        def operation():
            try:
                producers_sheet = self.spreadsheet.worksheet("Producers")
                
                # Check if producer already exists
                records = producers_sheet.get_all_records()
                existing_producers = [
                    str(record.get('Producer_Name', '')).strip().lower() 
                    for record in records
                ]
                
                if producer_name.strip().lower() in existing_producers:
                    return False  # Already exists
                
                # Add the new producer
                producers_sheet.append_row([
                    producer_name.strip(),
                    datetime.now().strftime("%Y-%m-%d")
                ])
                
                logger.info(f"Added producer: {producer_name}")
                return True
                
            except Exception as e:
                logger.error(f"Error adding producer {producer_name}: {e}")
                return False
        
        return await self.safe_operation(operation)
    
    async def remove_producer(self, producer_name: str) -> bool:
        """Remove a producer from the Producers sheet"""
        def operation():
            try:
                producers_sheet = self.spreadsheet.worksheet("Producers")
                records = producers_sheet.get_all_values()
                
                # Find the producer to remove (case insensitive)
                producer_lower = producer_name.strip().lower()
                row_to_delete = None
                
                for i, row in enumerate(records):
                    if len(row) > 0 and str(row[0]).strip().lower() == producer_lower:
                        row_to_delete = i + 1  # Sheet rows are 1-indexed
                        break
                
                if row_to_delete and row_to_delete > 1:  # Don't delete header row
                    producers_sheet.delete_rows(row_to_delete)
                    logger.info(f"Removed producer: {producer_name}")
                    return True
                
                return False  # Producer not found
                
            except Exception as e:
                logger.error(f"Error removing producer {producer_name}: {e}")
                return False
        
        return await self.safe_operation(operation)
    
    def _generate_unique_id(self) -> str:
        """Generate 8-character unique hex identifier"""
        return secrets.token_hex(4).upper()
    
    def _validate_date_dd_mm_yyyy(self, date_str: str) -> bool:
        """Validate date format (DD-MM-YYYY)"""
        try:
            datetime.strptime(date_str, '%d-%m-%Y')
            return True
        except ValueError:
            return False
    
    def _convert_date_to_storage_format(self, date_str: str) -> str:
        """Convert DD-MM-YYYY to YYYY-MM-DD for internal storage consistency"""
        try:
            date_obj = datetime.strptime(date_str, '%d-%m-%Y')
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            return date_str  # Return as-is if conversion fails
    
    def _convert_date_to_display_format(self, date_str: str) -> str:
        """Convert YYYY-MM-DD to DD-MM-YYYY for display"""
        try:
            # Try YYYY-MM-DD format first
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            return date_obj.strftime('%d-%m-%Y')
        except ValueError:
            try:
                # Try DD-MM-YYYY format (already in display format)
                datetime.strptime(date_str, '%d-%m-%Y')
                return date_str
            except ValueError:
                return date_str  # Return as-is if both fail
    
    def _format_user_id_for_sheets(self, user_id: int) -> str:
        """Format user ID as string to prevent scientific notation in sheets"""
        return f"'{user_id}"  # Prefix with apostrophe to force text format
    
    def _extract_user_id_from_sheets(self, user_id_str: str) -> int:
        """Extract user ID from sheets format (remove apostrophe if present)"""
        try:
            # Remove apostrophe if present
            clean_id = str(user_id_str).lstrip("'")
            return int(clean_id)
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse user ID: {user_id_str}")
            return 0
    
    def _sanitize_username(self, username: str) -> str:
        """Sanitize username for storage (remove problematic characters)"""
        if not username:
            return ""
        # Remove any characters that might cause issues in sheets
        sanitized = str(username).strip()
        # Replace any single quotes that might interfere with our user ID formatting
        sanitized = sanitized.replace("'", "'")  # Replace straight quote with curly quote
        return sanitized[:50]  # Limit length to prevent sheet issues
    
    def _sanitize_producer(self, producer: str) -> str:
        """Sanitize producer name for storage"""
        if not producer:
            return "Unknown"
        return str(producer).strip()[:50]  # Limit length
    
    async def safe_operation(self, operation):
        """Execute sheet operation with enhanced error handling"""
        async with self._lock:
            try:
                self._rate_limiter.wait_if_needed()
                result = await asyncio.get_event_loop().run_in_executor(
                    self.executor, operation
                )
                return result
            except Exception as e:
                logger.error(f"Sheet operation failed: {e}", exc_info=True)
                return None
    
    async def cached_operation(self, cache_key: str, operation, cache_duration: int = None):
        """Execute operation with caching"""
        cache_duration = cache_duration or self.cache_ttl
        
        # Check cache first
        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if time.time() - timestamp < cache_duration:
                logger.debug(f"Cache hit for key: {cache_key}")
                return data
        
        # Execute operation
        result = await self.safe_operation(operation)
        
        # Cache result if successful
        if result is not None:
            self.cache[cache_key] = (result, time.time())
            logger.debug(f"Cached result for key: {cache_key}")
        
        return result
    
    def clear_cache(self, prefix: str = None):
        """Clear cache entries"""
        if prefix:
            keys_to_remove = [key for key in self.cache.keys() if key.startswith(prefix)]
            for key in keys_to_remove:
                del self.cache[key]
        else:
            self.cache.clear()
        logger.info(f"Cache cleared {'with prefix: ' + prefix if prefix else 'completely'}")
    
    def _normalize_strain_name(self, name: str) -> str:
        """Normalize strain name for duplicate checking - case insensitive, remove special chars"""
        normalized = re.sub(r'[^a-zA-Z0-9]', '', name.lower())
        return normalized
    
    async def check_strain_duplicate(self, strain_name: str, harvest_date: str, package_date: str, category: str, producer: str = None) -> Optional[str]:
        """Check for duplicate strain with same normalized name, dates, category, and producer. Returns unique_id if duplicate found."""
        def operation():
            try:
                strains_sheet = self.spreadsheet.worksheet("Strains")
                records = strains_sheet.get_all_records()
                
                normalized_input = self._normalize_strain_name(strain_name)
                
                for record in records:
                    # Handle cases where Category/Producer columns might not exist yet in existing data
                    record_category = str(record.get('Category', 'flower')).lower()
                    record_producer = str(record.get('Producer', 'Unknown'))
                    
                    record_name_normalized = self._normalize_strain_name(str(record.get('Strain_Name', '')))
                    record_harvest = str(record.get('Harvest_Date', ''))
                    record_package = str(record.get('Package_Date', ''))
                    
                    # Check for duplicate including producer if provided
                    if (record_name_normalized == normalized_input and 
                        record_harvest == harvest_date and 
                        record_package == package_date and
                        record_category == category.lower() and
                        (not producer or record_producer == producer)):
                        return str(record.get('Unique_ID', ''))
                
                return None
            except Exception as e:
                logger.error(f"Error checking strain duplicate: {e}")
                return None
        
        return await self.safe_operation(operation)
    
    async def get_strain_by_identifier(self, identifier: str, category: str = None) -> Optional[Dict]:
        """Get strain by unique ID or name (with wildcard support), optionally filtered by category"""
        cache_key = f"strain_search_{identifier.lower()}_{category or 'all'}"
        
        def operation():
            strains_sheet = self.spreadsheet.worksheet("Strains")
            records = strains_sheet.get_all_records()
            
            # Handle cases where Category/Producer columns might not exist yet in existing data
            for record in records:
                if 'Category' not in record:
                    record['Category'] = 'flower'  # Default to flower for existing records
                if 'Producer' not in record:
                    record['Producer'] = 'Unknown'  # Default producer for existing records
            
            # Filter by category if specified
            if category:
                records = [r for r in records if str(r.get('Category', 'flower')).lower() == category.lower()]
            
            # First try exact unique ID match
            for record in records:
                unique_id_str = str(record.get('Unique_ID', '')).upper()
                if unique_id_str == identifier.upper():
                    return record
            
            # Then try exact name match
            for record in records:
                strain_name_str = str(record['Strain_Name']).lower()
                if strain_name_str == identifier.lower():
                    return record
            
            # Finally try wildcard matching on name
            if '*' in identifier or '?' in identifier:
                pattern = identifier.replace('*', '.*').replace('?', '.')
                regex = re.compile(pattern, re.IGNORECASE)
                for record in records:
                    if regex.search(str(record['Strain_Name'])):
                        return record
            else:
                # Partial matching if no wildcards
                for record in records:
                    if identifier.lower() in str(record['Strain_Name']).lower():
                        return record
            
            return None
        
        return await self.cached_operation(cache_key, operation, cache_duration=60)
    
    async def search_strains(self, query: str, category: str = None) -> List[Dict]:
        """Search for multiple strains matching query, optionally filtered by category"""
        def operation():
            strains_sheet = self.spreadsheet.worksheet("Strains")
            records = strains_sheet.get_all_records()
            matches = []
            
            # Handle cases where Category/Producer columns might not exist yet in existing data
            for record in records:
                if 'Category' not in record:
                    record['Category'] = 'flower'  # Default to flower for existing records
                if 'Producer' not in record:
                    record['Producer'] = 'Unknown'  # Default producer for existing records
            
            # Filter by category if specified
            if category:
                records = [r for r in records if str(r.get('Category', 'flower')).lower() == category.lower()]
            
            # Handle wildcard search
            if '*' in query or '?' in query:
                pattern = query.replace('*', '.*').replace('?', '.')
                regex = re.compile(pattern, re.IGNORECASE)
                for record in records:
                    strain_name = str(record['Strain_Name'])
                    unique_id = str(record.get('Unique_ID', ''))
                    if regex.search(strain_name) or regex.search(unique_id):
                        matches.append(record)
            else:
                # Partial matching
                query_lower = query.lower()
                for record in records:
                    strain_name = str(record['Strain_Name']).lower()
                    unique_id = str(record.get('Unique_ID', '')).lower()
                    if (query_lower in strain_name or query_lower in unique_id):
                        matches.append(record)
            
            return matches[:10]  # Limit to 10 results
        
        return await self.safe_operation(operation) or []
    
    async def add_strain_submission(self, strain_name: str, harvest_date: str, package_date: str, category: str, producer: str, user_id: int, username: str = "") -> Optional[str]:
        """Add new strain submission with category, producer support and username - returns unique_id if successful"""
        def operation():
            try:
                # Validate dates in DD-MM-YYYY format
                if not self._validate_date_dd_mm_yyyy(harvest_date) or not self._validate_date_dd_mm_yyyy(package_date):
                    return None
                
                # Validate category
                if category.lower() not in self.valid_categories:
                    return None
                
                # Generate unique ID
                unique_id = self._generate_unique_id()
                
                # Format user ID and sanitize username and producer
                formatted_user_id = self._format_user_id_for_sheets(user_id)
                sanitized_username = self._sanitize_username(username)
                sanitized_producer = self._sanitize_producer(producer)
                
                # Add to Strains sheet (using display format for user visibility)
                strains_sheet = self.spreadsheet.worksheet("Strains")
                
                strains_sheet.append_row([
                    unique_id,
                    strain_name,
                    "Pending",
                    0,
                    0,
                    datetime.now().strftime("%Y-%m-%d"),
                    harvest_date,  # Keep in DD-MM-YYYY format for display
                    package_date,  # Keep in DD-MM-YYYY format for display
                    category.lower(),  # Add category column
                    sanitized_producer  # Add producer column
                ])
                
                # Add to Submissions sheet for tracking
                submissions_sheet = self.spreadsheet.worksheet("Submissions")
                next_submission_id = len(submissions_sheet.get_all_values())
                submissions_sheet.append_row([
                    next_submission_id,
                    unique_id,
                    strain_name,
                    formatted_user_id,  # Use formatted user ID
                    harvest_date,  # Keep in DD-MM-YYYY format
                    package_date,  # Keep in DD-MM-YYYY format
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    category.lower(),
                    sanitized_producer,  # Store producer for tracking
                    sanitized_username  # Store username for tracking
                ])
                
                return unique_id
            except Exception as e:
                logger.error(f"Error in add_strain_submission: {e}")
                return None
        
        return await self.safe_operation(operation)
    
    async def add_rating(self, identifier: str, user_id: int, rating: int, username: str, category: str = None) -> bool:
        """Add user rating for a strain (by unique ID or name), optionally filtered by category"""
        def operation():
            return self._add_rating_operation(identifier, user_id, rating, username, category)
        
        result = await self.safe_operation(operation)
        if result:
            # Clear relevant cache
            self.clear_cache(f"strain_")
            self.clear_cache("all_approved_strains")
            self.clear_cache("top_strains")
        return result
    
    def _add_rating_operation(self, identifier: str, user_id: int, rating: int, username: str, category: str = None) -> bool:
        """Internal rating operation with identifier, username, and category support"""
        try:
            ratings_sheet = self.spreadsheet.worksheet("Ratings")
            strains_sheet = self.spreadsheet.worksheet("Strains")
            
            ratings = ratings_sheet.get_all_records()
            strains = strains_sheet.get_all_records()
            
            # Handle cases where Category/Producer columns might not exist yet in existing data
            for strain in strains:
                if 'Category' not in strain:
                    strain['Category'] = 'flower'  # Default to flower for existing records
                if 'Producer' not in strain:
                    strain['Producer'] = 'Unknown'  # Default producer for existing records
            
            # Filter strains by category if specified
            if category:
                strains = [s for s in strains if str(s.get('Category', 'flower')).lower() == category.lower()]
            
            # Find strain by identifier
            strain_data = None
            for strain in strains:
                unique_id_str = str(strain.get('Unique_ID', '')).upper()
                strain_name_str = str(strain.get('Strain_Name', '')).lower()
                
                if ((unique_id_str == identifier.upper()) or
                    (strain_name_str == identifier.lower())):
                    strain_data = strain
                    break
            
            if not strain_data or strain_data['Status'] != 'Approved':
                return False
            
            # Check for duplicate rating using Unique_ID
            formatted_user_id = self._format_user_id_for_sheets(user_id)
            for rating_record in ratings:
                rating_unique_id = str(rating_record.get('Unique_ID', ''))
                strain_unique_id = str(strain_data.get('Unique_ID', ''))
                record_user_id = str(rating_record.get('User_ID', ''))
                
                # Handle both formatted and unformatted user IDs
                if (rating_unique_id == strain_unique_id and 
                    (record_user_id == str(user_id) or record_user_id == formatted_user_id)):
                    return False  # User already rated this strain
            
            # Sanitize username
            sanitized_username = self._sanitize_username(username)
            
            # Add new rating with username (always include username column)
            next_rating_id = len(ratings) + 1
            ratings_sheet.append_row([
                next_rating_id,
                str(strain_data['Unique_ID']),  # Ensure it's a string
                formatted_user_id,  # Use formatted user ID
                rating,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                sanitized_username  # Always add username as 6th column
            ])
            
            # Update strain average
            strain_unique_id = str(strain_data['Unique_ID'])
            strain_ratings = [r for r in ratings if str(r.get('Unique_ID', '')) == strain_unique_id]
            strain_ratings.append({'Rating': rating})  # Include new rating
            
            total_ratings = len(strain_ratings)
            avg_rating = sum(r['Rating'] for r in strain_ratings) / total_ratings
            
            # Update strain record (find by Unique_ID)
            for i, strain in enumerate(strains_sheet.get_all_records(), start=2):
                if str(strain.get('Unique_ID', '')) == strain_unique_id:
                    strains_sheet.update_cell(i, 4, round(avg_rating, 2))  # Average_Rating (column D)
                    strains_sheet.update_cell(i, 5, total_ratings)  # Total_Ratings (column E)
                    break
            
            return True
            
        except Exception as e:
            logger.error(f"Error in _add_rating_operation: {e}", exc_info=True)
            return False
    
    async def get_top_strains_for_status(self, category: str, limit: int = 10) -> List[Dict]:
        """Get top rated strains for status display with category filter"""
        cache_key = f"top_strains_{category}_{limit}"
        
        def operation():
            strains_sheet = self.spreadsheet.worksheet("Strains")
            records = strains_sheet.get_all_records()
            
            # Handle cases where Category/Producer columns might not exist yet in existing data
            for record in records:
                if 'Category' not in record:
                    record['Category'] = 'flower'  # Default to flower for existing records
                if 'Producer' not in record:
                    record['Producer'] = 'Unknown'  # Default producer for existing records
            
            # Filter for approved strains with ratings in the specified category
            approved_with_ratings = [
                r for r in records 
                if (r['Status'] == 'Approved' and 
                    int(r.get('Total_Ratings', 0)) > 0 and
                    str(r.get('Category', 'flower')).lower() == category.lower())
            ]
            
            # Sort by average rating (desc)
            sorted_strains = sorted(
                approved_with_ratings, 
                key=lambda x: float(x.get('Average_Rating', 0)), 
                reverse=True
            )
            
            return sorted_strains[:limit]
        
        return await self.cached_operation(cache_key, operation, cache_duration=120) or []
    
    async def get_recent_ratings_for_status(self, limit: int = 10) -> List[Dict]:
        """Get recent ratings for status display with user info, proper user ID handling, and producer info"""
        def operation():
            try:
                ratings_sheet = self.spreadsheet.worksheet("Ratings")
                strains_sheet = self.spreadsheet.worksheet("Strains")
                
                ratings = ratings_sheet.get_all_records()
                strains = strains_sheet.get_all_records()
                
                # Handle cases where Producer column might not exist yet
                for strain in strains:
                    if 'Producer' not in strain:
                        strain['Producer'] = 'Unknown'
                
                # Create strain lookup using Unique_ID (convert to string for safety)
                strain_lookup = {str(s.get('Unique_ID', '')): s for s in strains}
                
                # Sort ratings by date (newest first)
                sorted_ratings = sorted(ratings, 
                                      key=lambda x: x.get('Date_Rated', ''), 
                                      reverse=True)
                
                # Combine rating data with strain info
                enriched_ratings = []
                for rating in sorted_ratings[:limit]:
                    rating_unique_id = str(rating.get('Unique_ID', ''))
                    strain_info = strain_lookup.get(rating_unique_id, {})
                    
                    # Only include ratings for approved strains
                    if strain_info.get('Status') == 'Approved':
                        # Extract user ID properly
                        user_id = self._extract_user_id_from_sheets(rating.get('User_ID', ''))
                        
                        # Use stored username if available, otherwise fall back to User-ID format
                        stored_username = rating.get('Username', '').strip()
                        username = stored_username if stored_username else f"User-{user_id}"
                        
                        enriched_rating = {
                            **rating,
                            'User_ID_Clean': user_id,  # Add clean user ID
                            'Username_Display': username,  # Add display username
                            'Strain_Name': strain_info.get('Strain_Name', 'Unknown'),
                            'Harvest_Date': strain_info.get('Harvest_Date', 'N/A'),
                            'Package_Date': strain_info.get('Package_Date', 'N/A'),
                            'Category': strain_info.get('Category', 'flower'),
                            'Producer': strain_info.get('Producer', 'Unknown')  # Add producer info
                        }
                        enriched_ratings.append(enriched_rating)
                
                return enriched_ratings[:limit]  # Limit after filtering
            except Exception as e:
                logger.error(f"Error getting recent ratings for status: {e}")
                return []
        
        return await self.safe_operation(operation) or []
    
    async def get_all_approved_strains(self, category: str = None) -> List[Dict]:
        """Get all approved strains with their ratings, optionally filtered by category"""
        cache_key = f"all_approved_strains_{category or 'all'}"
        
        def operation():
            strains_sheet = self.spreadsheet.worksheet("Strains")
            records = strains_sheet.get_all_records()
            
            # Handle cases where Category/Producer columns might not exist yet in existing data
            for record in records:
                if 'Category' not in record:
                    record['Category'] = 'flower'  # Default to flower for existing records
                if 'Producer' not in record:
                    record['Producer'] = 'Unknown'  # Default producer for existing records
            
            # Filter for approved strains
            approved = [r for r in records if r['Status'] == 'Approved']
            
            # Filter by category if specified
            if category:
                approved = [r for r in approved if str(r.get('Category', 'flower')).lower() == category.lower()]
            
            # Sort by average rating (desc), but put unrated strains at the end
            def sort_key(strain):
                rating = float(strain.get('Average_Rating', 0))
                total_ratings = int(strain.get('Total_Ratings', 0))
                # If no ratings, use -1 to put at end, otherwise use actual rating
                return rating if total_ratings > 0 else -1
            
            sorted_strains = sorted(approved, key=sort_key, reverse=True)
            
            return sorted_strains
        
        return await self.cached_operation(cache_key, operation, cache_duration=120) or []
    
    # Keep existing methods but update for category and producer support where needed
    async def get_pending_strains(self) -> List[Dict]:
        """Get all pending strain submissions"""
        def operation():
            strains_sheet = self.spreadsheet.worksheet("Strains")
            records = strains_sheet.get_all_records()
            
            # Handle cases where Category/Producer columns might not exist yet in existing data
            for record in records:
                if 'Category' not in record:
                    record['Category'] = 'flower'  # Default to flower for existing records
                if 'Producer' not in record:
                    record['Producer'] = 'Unknown'  # Default producer for existing records
            
            return [record for record in records if record['Status'] == 'Pending']
        
        result = await self.safe_operation(operation)
        return result or []
    
    async def approve_strain(self, identifier: str) -> bool:
        """Approve a pending strain submission by unique ID or name"""
        def operation():
            strains_sheet = self.spreadsheet.worksheet("Strains")
            records = strains_sheet.get_all_records()
            
            for i, record in enumerate(records, start=2):  # Start at row 2 (skip header)
                # Convert Unique_ID to string to handle cases where it's read as int
                unique_id_str = str(record.get('Unique_ID', '')).upper()
                strain_name_str = str(record.get('Strain_Name', '')).lower()
                identifier_upper = identifier.upper()
                identifier_lower = identifier.lower()
                
                # Check by unique ID or name
                if ((unique_id_str == identifier_upper) or
                    (strain_name_str == identifier_lower)) and \
                   record['Status'] == 'Pending':
                    strains_sheet.update_cell(i, 3, "Approved")  # Column C = Status
                    return True
            return False
        
        result = await self.safe_operation(operation)
        if result:
            # Clear cache since data changed
            self.clear_cache(f"strain_")
            self.clear_cache("top_strains")
        return result
    
    async def rename_strain(self, unique_id: str, new_name: str) -> bool:
        """Rename a strain by unique ID"""
        def operation():
            try:
                strains_sheet = self.spreadsheet.worksheet("Strains")
                records = strains_sheet.get_all_records()
                
                for i, record in enumerate(records, start=2):  # Start at row 2 (skip header)
                    if str(record.get('Unique_ID', '')).upper() == unique_id.upper():
                        strains_sheet.update_cell(i, 2, new_name)  # Column B = Strain_Name
                        return True
                return False
            except Exception as e:
                logger.error(f"Error renaming strain: {e}")
                return False
        
        result = await self.safe_operation(operation)
        if result:
            # Clear cache since data changed
            self.clear_cache(f"strain_")
            self.clear_cache("top_strains")
        return result
    
    async def get_strain_ratings_with_users(self, unique_id: str, limit: int = 5) -> List[Dict]:
        """Get recent ratings for a specific strain with user info"""
        def operation():
            try:
                ratings_sheet = self.spreadsheet.worksheet("Ratings")
                ratings = ratings_sheet.get_all_records()
                
                # Filter ratings for this strain and sort by date (newest first)
                strain_ratings = [r for r in ratings if str(r.get('Unique_ID', '')) == str(unique_id)]
                sorted_ratings = sorted(strain_ratings, 
                                      key=lambda x: x.get('Date_Rated', ''), 
                                      reverse=True)
                
                # Add clean user IDs and display usernames
                for rating in sorted_ratings:
                    rating['User_ID_Clean'] = self._extract_user_id_from_sheets(rating.get('User_ID', ''))
                    # Use stored username if available, otherwise fall back to User-ID format
                    stored_username = rating.get('Username', '').strip()
                    rating['Username_Display'] = stored_username if stored_username else f"User-{rating['User_ID_Clean']}"
                
                return sorted_ratings[:limit]
            except Exception as e:
                logger.error(f"Error getting strain ratings: {e}")
                return []
        
        return await self.safe_operation(operation) or []
    
    async def get_pending_strains_count(self) -> int:
        """Get count of pending strains for notifications"""
        def operation():
            strains_sheet = self.spreadsheet.worksheet("Strains")
            records = strains_sheet.get_all_records()
            return len([record for record in records if record['Status'] == 'Pending'])
        
        result = await self.safe_operation(operation)
        return result or 0
    
    # Legacy methods for backwards compatibility
    async def get_strain_by_name(self, strain_name: str) -> Optional[Dict]:
        """Legacy method - now uses get_strain_by_identifier"""
        return await self.get_strain_by_identifier(strain_name)
    
    async def get_last_submissions(self, limit: int = 10) -> List[Dict]:
        """Get last N strain submissions with proper user ID handling, usernames, and producer info"""
        def operation():
            try:
                submissions_sheet = self.spreadsheet.worksheet("Submissions")
                records = submissions_sheet.get_all_records()
                
                # Add clean user IDs to records and handle missing Category/Producer/Username columns
                for record in records:
                    record['User_ID_Clean'] = self._extract_user_id_from_sheets(record.get('User_ID', ''))
                    # Handle cases where Category column might not exist yet
                    if 'Category' not in record:
                        record['Category'] = 'flower'  # Default to flower for existing records
                    # Handle producer field for newer records
                    if 'Producer' not in record:
                        record['Producer'] = 'Unknown'  # Default for older records
                    # Handle username field for newer records
                    if 'Username' not in record:
                        record['Username'] = ''  # Empty for older records
                
                # Sort by date (newest first) and return last N
                sorted_records = sorted(records, 
                                      key=lambda x: x.get('Date_Added', ''), 
                                      reverse=True)
                return sorted_records[:limit]
            except gspread.exceptions.WorksheetNotFound:
                logger.warning("Submissions sheet not found")
                return []
        
        return await self.safe_operation(operation) or []
    
    async def get_last_ratings(self, limit: int = 10) -> List[Dict]:
        """Get last N ratings with strain information, proper user ID handling, and producer info"""
        def operation():
            try:
                ratings_sheet = self.spreadsheet.worksheet("Ratings")
                strains_sheet = self.spreadsheet.worksheet("Strains")
                
                ratings = ratings_sheet.get_all_records()
                strains = strains_sheet.get_all_records()
                
                # Handle cases where Producer column might not exist yet
                for strain in strains:
                    if 'Producer' not in strain:
                        strain['Producer'] = 'Unknown'
                
                # Create strain lookup using Unique_ID (convert to string for safety)
                strain_lookup = {str(s.get('Unique_ID', '')): s for s in strains}
                
                # Sort ratings by date (newest first)
                sorted_ratings = sorted(ratings, 
                                      key=lambda x: x.get('Date_Rated', ''), 
                                      reverse=True)
                
                # Combine rating data with strain info
                enriched_ratings = []
                for rating in sorted_ratings[:limit]:
                    rating_unique_id = str(rating.get('Unique_ID', ''))
                    strain_info = strain_lookup.get(rating_unique_id, {})
                    
                    # Extract clean user ID
                    user_id_clean = self._extract_user_id_from_sheets(rating.get('User_ID', ''))
                    
                    # Use stored username if available, otherwise fall back to User-ID format
                    stored_username = rating.get('Username', '').strip()
                    username_display = stored_username if stored_username else f"User-{user_id_clean}"
                    
                    enriched_rating = {
                        **rating,
                        'User_ID_Clean': user_id_clean,
                        'Username_Display': username_display,
                        'Strain_Name': strain_info.get('Strain_Name', 'Unknown'),
                        'Harvest_Date': strain_info.get('Harvest_Date', 'Unknown'),
                        'Package_Date': strain_info.get('Package_Date', 'Unknown'),
                        'Category': strain_info.get('Category', 'flower'),
                        'Producer': strain_info.get('Producer', 'Unknown')  # Add producer info
                    }
                    enriched_ratings.append(enriched_rating)
                
                return enriched_ratings
            except Exception as e:
                logger.error(f"Error getting last ratings: {e}")
                return []
        
        return await self.safe_operation(operation) or []
