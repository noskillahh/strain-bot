# monitoring.py - Complete health monitoring and logging system
import logging
import json
from datetime import datetime
from aiohttp import web
import asyncio
from typing import Optional

class StructuredFormatter(logging.Formatter):
    """JSON structured logging formatter"""
    
    def format(self, record):
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add custom fields if present
        if hasattr(record, 'user_id'):
            log_entry['user_id'] = record.user_id
        if hasattr(record, 'guild_id'):
            log_entry['guild_id'] = record.guild_id
        if hasattr(record, 'command_name'):
            log_entry['command_name'] = record.command_name
        
        return json.dumps(log_entry)

def setup_logging(level: str = 'INFO'):
    """Configure structured logging"""
    # Create formatter
    formatter = StructuredFormatter()
    
    # File handler
    file_handler = logging.FileHandler('strain_bot.log')
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(getattr(logging, level.upper()))
    
    return logger

class HealthMonitor:
    """Health monitoring for the bot"""
    
    def __init__(self, bot):
        self.bot = bot
        self.app: Optional[web.Application] = None
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
    
    async def health_check(self, request):
        """Health check endpoint"""
        try:
            # Test Google Sheets connection
            sheets_healthy = await self._test_sheets_connection()
            
            # Test Discord connection
            discord_healthy = self.bot.is_ready()
            
            status = {
                'status': 'healthy' if sheets_healthy and discord_healthy else 'unhealthy',
                'timestamp': datetime.utcnow().isoformat(),
                'services': {
                    'discord': {
                        'status': 'healthy' if discord_healthy else 'unhealthy',
                        'guilds': len(self.bot.guilds) if discord_healthy else 0,
                        'latency_ms': round(self.bot.latency * 1000, 2) if discord_healthy else None
                    },
                    'google_sheets': {
                        'status': 'healthy' if sheets_healthy else 'unhealthy'
                    }
                }
            }
            
            status_code = 200 if status['status'] == 'healthy' else 503
            return web.json_response(status, status=status_code)
        
        except Exception as e:
            return web.json_response({
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }, status=503)
    
    async def _test_sheets_connection(self) -> bool:
        """Test Google Sheets connection"""
        try:
            # Simple test operation
            if hasattr(self.bot, 'sheets_manager') and hasattr(self.bot.sheets_manager, 'safe_operation'):
                result = await self.bot.sheets_manager.safe_operation(lambda: True)
                return result is not None
            return False
        except:
            return False
    
    async def start_health_server(self, port: int = 8080):
        """Start health check server"""
        try:
            self.app = web.Application()
            self.app.router.add_get('/health', self.health_check)
            self.app.router.add_get('/metrics', self.metrics_endpoint)
            
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            
            self.site = web.TCPSite(self.runner, '0.0.0.0', port)
            await self.site.start()
            
            logging.info(f"Health check server started on port {port}")
        except Exception as e:
            logging.error(f"Failed to start health server: {e}")
    
    async def metrics_endpoint(self, request):
        """Prometheus-style metrics endpoint"""
        try:
            metrics = [
                f'discord_bot_guilds {len(self.bot.guilds)}',
                f'discord_bot_latency_seconds {self.bot.latency}',
                f'discord_bot_ready {1 if self.bot.is_ready() else 0}',
            ]
            
            return web.Response(text='\n'.join(metrics), content_type='text/plain')
        except Exception as e:
            return web.Response(text=f'# Error generating metrics: {e}', status=500)
    
    async def stop_health_server(self):
        """Stop health check server"""
        try:
            if self.site:
                await self.site.stop()
            if self.runner:
                await self.runner.cleanup()
        except Exception as e:
            logging.error(f"Error stopping health server: {e}")
