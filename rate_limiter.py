# rate_limiter.py - Advanced rate limiting system
import time
from collections import defaultdict
from typing import Dict, List
import asyncio

class AdvancedRateLimiter:
    """Advanced rate limiting with per-user and per-guild limits"""
    
    def __init__(self):
        self.user_requests: Dict[int, List[float]] = defaultdict(list)
        self.guild_requests: Dict[int, List[float]] = defaultdict(list)
        self._lock = asyncio.Lock()
    
    async def check_user_limit(self, user_id: int, limit: int = 5, window: int = 60) -> bool:
        """Check if user is within rate limit"""
        async with self._lock:
            now = time.time()
            user_calls = self.user_requests[user_id]
            
            # Clean old requests
            user_calls[:] = [call for call in user_calls if now - call < window]
            
            if len(user_calls) >= limit:
                return False
            
            user_calls.append(now)
            return True
    
    async def check_guild_limit(self, guild_id: int, limit: int = 50, window: int = 60) -> bool:
        """Check if guild is within rate limit"""
        async with self._lock:
            now = time.time()
            guild_calls = self.guild_requests[guild_id]
            
            # Clean old requests
            guild_calls[:] = [call for call in guild_calls if now - call < window]
            
            if len(guild_calls) >= limit:
                return False
            
            guild_calls.append(now)
            return True
    
    def get_user_remaining_calls(self, user_id: int, limit: int = 5, window: int = 60) -> int:
        """Get remaining calls for user"""
        now = time.time()
        user_calls = self.user_requests[user_id]
        user_calls[:] = [call for call in user_calls if now - call < window]
        return max(0, limit - len(user_calls))
