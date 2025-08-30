# main.py - Enhanced Discord Bot v7 with Fixed Approval Flow and New Features
import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

# Import our custom modules
from config import Config
from validators import InputValidator
from rate_limiter import AdvancedRateLimiter
from monitoring import setup_logging, HealthMonitor
from enhanced_sheets import OptimizedSheetsManager

# Initialize logging first
logger = setup_logging(Config.LOG_LEVEL)

class ProducerSelect(discord.ui.Select):
    """Producer selection dropdown for strain submission"""
    def __init__(self, valid_producers: List[str]):
        options = [
            discord.SelectOption(label=producer, value=producer)
            for producer in valid_producers
        ]
        super().__init__(placeholder="Select a producer...", options=options, min_values=1, max_values=1)
    
    async def callback(self, interaction: discord.Interaction):
        # Just acknowledge the selection - the modal will handle the rest
        await interaction.response.defer()

class CategorySelect(discord.ui.Select):
    """Category selection dropdown for strain submission"""
    def __init__(self):
        options = [
            discord.SelectOption(label="🌿 Flower", description="Cannabis flower/bud", value="flower"),
            discord.SelectOption(label="🍯 Hash", description="Hash products", value="hash"),
            discord.SelectOption(label="🧈 Rosin", description="Rosin products", value="rosin")
        ]
        super().__init__(placeholder="Select a category...", options=options, min_values=1, max_values=1)
    
    async def callback(self, interaction: discord.Interaction):
        # Just acknowledge the selection - the modal will handle the rest
        await interaction.response.defer()

class StrainSubmissionModal(discord.ui.Modal, title='Submit New Product'):
    """Modal for strain submission with category and producer support"""
    
    def __init__(self, category: str, producer: str):
        super().__init__()
        self.category = category
        self.producer = producer
        
        # Update title based on category
        category_names = {"flower": "Flower", "hash": "Hash", "rosin": "Rosin"}
        self.title = f'Submit New {category_names.get(category, "Product")}'
    
    strain_name = discord.ui.TextInput(
        label='Product Name',
        placeholder='Enter the product name (e.g., Blue Dream)',
        required=True,
        max_length=50
    )
    
    harvest_date = discord.ui.TextInput(
        label='Harvest Date',
        placeholder='DD-MM-YYYY (e.g., 01-12-2024)',
        required=True,
        max_length=10
    )
    
    package_date = discord.ui.TextInput(
        label='Package Date',
        placeholder='DD-MM-YYYY (e.g., 15-12-2024)',
        required=True,
        max_length=10
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        bot = interaction.client
        
        # Simple immediate response
        try:
            await interaction.response.send_message("Processing your submission...", ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to respond to modal: {e}")
            return
        
        # Rate limiting check
        if not await bot.rate_limiter.check_user_limit(
            interaction.user.id, 
            Config.RATE_LIMIT_PER_USER, 
            Config.RATE_LIMIT_WINDOW
        ):
            try:
                await interaction.edit_original_response(
                    content="⏰ You're submitting too quickly! Please wait before trying again."
                )
            except Exception as e:
                logger.error(f"Failed to send rate limit message: {e}")
            await bot.log_command_usage(interaction, 'submit_strain', success=False)
            return
        
        try:
            # Validate inputs
            validated_name = bot.validator.validate_strain_name(str(self.strain_name))
            if not validated_name:
                await interaction.edit_original_response(
                    content="❌ Invalid product name. Please use 2-50 characters with letters, numbers, spaces, and basic punctuation only."
                )
                await bot.log_command_usage(interaction, 'submit_strain', success=False)
                return
            
            # Validate dates in DD-MM-YYYY format
            harvest_date_str = str(self.harvest_date).strip()
            package_date_str = str(self.package_date).strip()
            
            if not bot.validator.validate_date_dd_mm_yyyy(harvest_date_str) or not bot.validator.validate_date_dd_mm_yyyy(package_date_str):
                await interaction.edit_original_response(
                    content="❌ Invalid date format. Please use DD-MM-YYYY (e.g., 01-12-2024)."
                )
                await bot.log_command_usage(interaction, 'submit_strain', success=False)
                return
            
            # Check for duplicate strain with enhanced detection (including category and producer)
            existing_unique_id = await bot.sheets_manager.check_strain_duplicate(
                validated_name, harvest_date_str, package_date_str, self.category, self.producer
            )
            if existing_unique_id:
                # Found exact duplicate (same normalized name + same dates + same category + same producer)
                category_name = {"flower": "flower", "hash": "hash", "rosin": "rosin"}[self.category]
                await interaction.edit_original_response(
                    content=f"❌ A {category_name} product with a similar name '{validated_name}' from {self.producer} and the same harvest/package dates was already submitted.\n"
                           f"**Existing product ID:** `{existing_unique_id}`\n"
                           f"Use this ID to reference the existing product, or submit with different dates if this is a different batch."
                )
                await bot.log_command_usage(interaction, 'submit_strain', success=False)
                return
            
            # Get user display name
            username = bot.get_user_display_name(interaction.user)
            
            # Add strain submission with category, producer, and username
            unique_id = await bot.sheets_manager.add_strain_submission(
                validated_name, harvest_date_str, package_date_str, self.category, self.producer, interaction.user.id, username
            )
            
            if unique_id:
                # Create success embed
                category_emojis = {"flower": "🌿", "hash": "🍯", "rosin": "🧈"}
                category_names = {"flower": "Flower", "hash": "Hash", "rosin": "Rosin"}
                
                embed = discord.Embed(
                    title=f"✅ {category_names[self.category]} Submitted Successfully",
                    description=f"{category_emojis[self.category]} **{validated_name}** has been submitted for moderator approval.",
                    color=discord.Color.green()
                )
                embed.add_field(name="Unique ID", value=f"`{unique_id}`", inline=False)
                embed.add_field(name="Category", value=f"{category_emojis[self.category]} {category_names[self.category]}", inline=True)
                embed.add_field(name="Producer", value=self.producer, inline=True)
                embed.add_field(name="Harvest Date", value=harvest_date_str, inline=True)
                embed.add_field(name="Package Date", value=package_date_str, inline=True)
                embed.add_field(name="Submitted by", value=username, inline=True)
                embed.add_field(name="Status", value="Pending moderator approval", inline=False)
                embed.set_footer(text="Save your unique ID for future reference")
                
                try:
                    await interaction.edit_original_response(content=None, embed=embed)
                except Exception as e:
                    logger.error(f"Failed to edit response with embed: {e}")
                    # Fallback to text response
                    try:
                        await interaction.edit_original_response(
                            content=f"✅ **{validated_name}** ({category_names[self.category]}) from {self.producer} submitted successfully!\n"
                                   f"**Unique ID:** `{unique_id}`\n"
                                   f"**Submitted by:** {username}\n"
                                   f"**Harvest:** {harvest_date_str}\n"
                                   f"**Package:** {package_date_str}\n"
                                   f"Status: Pending moderator approval"
                        )
                    except Exception as fallback_error:
                        logger.error(f"Fallback response also failed: {fallback_error}")
                        return
                
                await bot.log_command_usage(interaction, 'submit_strain', success=True)
                
                # Check for moderator notification
                try:
                    await bot.check_and_notify_moderators(interaction.guild)
                except Exception as notify_error:
                    logger.error(f"Failed to notify moderators: {notify_error}")
                
            else:
                await interaction.edit_original_response(
                    content="❌ Failed to submit product. Please check your inputs and try again."
                )
                await bot.log_command_usage(interaction, 'submit_strain', success=False)
        
        except Exception as e:
            logger.error(f"Error in strain submission modal: {e}", exc_info=True)
            try:
                await interaction.edit_original_response(
                    content="❌ An error occurred while submitting the product. Please try again."
                )
            except Exception as edit_error:
                logger.error(f"Failed to send error message: {edit_error}")
            await bot.log_command_usage(interaction, 'submit_strain', success=False)

class CategoryProducerSelectView(discord.ui.View):
    """View for category and producer selection before submission"""
    def __init__(self, valid_producers: List[str]):
        super().__init__(timeout=300)
        self.valid_producers = valid_producers
        self.selected_category = None
        self.selected_producer = None
        
        # Add category select
        self.category_select = discord.ui.Select(
            placeholder="1. Select a category...",
            options=[
                discord.SelectOption(label="🌿 Flower", description="Cannabis flower/bud", value="flower"),
                discord.SelectOption(label="🍯 Hash", description="Hash products", value="hash"),
                discord.SelectOption(label="🧈 Rosin", description="Rosin products", value="rosin")
            ]
        )
        self.category_select.callback = self.category_callback
        self.add_item(self.category_select)
        
        # Add producer select
        self.producer_select = discord.ui.Select(
            placeholder="2. Select a producer...",
            options=[
                discord.SelectOption(label=producer, value=producer)
                for producer in valid_producers
            ],
            disabled=True  # Initially disabled until category is selected
        )
        self.producer_select.callback = self.producer_callback
        self.add_item(self.producer_select)
    
    async def category_callback(self, interaction: discord.Interaction):
        self.selected_category = self.category_select.values[0]
        
        # Enable producer select
        self.producer_select.disabled = False
        self.producer_select.placeholder = "2. Select a producer..."
        
        # Update category select to show selection
        category_names = {"flower": "🌿 Flower", "hash": "🍯 Hash", "rosin": "🧈 Rosin"}
        self.category_select.placeholder = f"✅ {category_names[self.selected_category]}"
        
        await interaction.response.edit_message(view=self)
    
    async def producer_callback(self, interaction: discord.Interaction):
        self.selected_producer = self.producer_select.values[0]
        
        # Both selections made, show modal
        modal = StrainSubmissionModal(self.selected_category, self.selected_producer)
        await interaction.response.send_modal(modal)

class PendingApprovalView(discord.ui.View):
    """View with numbered buttons for approving pending strains - FIXED VERSION"""
    
    def __init__(self, pending_strains: List[Dict], bot):
        super().__init__(timeout=600)  # 10 minute timeout
        self.pending_strains = pending_strains
        self.bot = bot
        self.approved_indices = set()  # Track which strains have been approved
        
        # Add numbered buttons (up to 9)
        for i in range(min(len(pending_strains), 9)):
            button = discord.ui.Button(
                label=str(i + 1),
                style=discord.ButtonStyle.primary,
                custom_id=f"approve_{i}"
            )
            button.callback = self.create_approval_callback(i)
            self.add_item(button)
    
    def create_approval_callback(self, index):
        async def approval_callback(interaction: discord.Interaction):
            # Check if user is moderator
            if not self.bot.is_moderator(interaction.user):
                await interaction.response.send_message("❌ You don't have permission to approve products.", ephemeral=True)
                return
            
            # Check if already approved
            if index in self.approved_indices:
                await interaction.response.send_message("❌ This product has already been approved.", ephemeral=True)
                return
            
            try:
                strain = self.pending_strains[index]
                unique_id = strain.get('Unique_ID', '')
                strain_name = strain.get('Strain_Name', 'Unknown')
                category = strain.get('Category', 'flower')
                
                # Approve the strain
                success = await self.bot.sheets_manager.approve_strain(unique_id)
                
                if success:
                    # Mark as approved
                    self.approved_indices.add(index)
                    
                    # Update the button that was clicked IMMEDIATELY
                    for item in self.children:
                        if hasattr(item, 'custom_id') and item.custom_id == f"approve_{index}":
                            item.disabled = True
                            item.label = f"{index + 1} ✓"
                            item.style = discord.ButtonStyle.success
                            break
                    
                    # Update the message immediately with the new button state and embed
                    embed = await self.create_updated_embed()
                    await interaction.response.edit_message(embed=embed, view=self)
                    
                    # Log the approval
                    await self.bot.log_command_usage(interaction, 'approve_strain', success=True)
                    
                    # Update status messages after approval
                    await self.bot.update_status_messages()
                    
                else:
                    await interaction.response.send_message("❌ Failed to approve product. Please try again.", ephemeral=True)
                    await self.bot.log_command_usage(interaction, 'approve_strain', success=False)
                    
            except Exception as e:
                logger.error(f"Error in approval callback: {e}", exc_info=True)
                await interaction.response.send_message("❌ An error occurred while approving the product.", ephemeral=True)
                await self.bot.log_command_usage(interaction, 'approve_strain', success=False)
        
        return approval_callback
    
    async def create_updated_embed(self):
        """Create the updated embed showing current status of all strains"""
        try:
            # Check if all strains are approved
            all_approved = len(self.approved_indices) == len(self.pending_strains[:9])
            
            if all_approved:
                # All strains approved - show completion message
                embed = discord.Embed(
                    title="✅ All Products Approved",
                    description="All pending products have been approved and are now available for rating!",
                    color=discord.Color.green()
                )
                
                # Show summary of approved strains
                for i, strain in enumerate(self.pending_strains[:9], 1):
                    category = strain.get('Category', 'flower')
                    category_emojis = {"flower": "🌿", "hash": "🍯", "rosin": "🧈"}
                    producer = strain.get('Producer', 'Unknown')
                    
                    embed.add_field(
                        name=f"{i}. {category_emojis.get(category, '🌿')} {strain['Strain_Name']}",
                        value=f"ID: `{strain.get('Unique_ID', 'N/A')}`\nProducer: {producer}\n✅ **Approved**",
                        inline=True
                    )
                
                embed.set_footer(text="All products are now ready for rating!")
                
                # Disable all remaining buttons
                for item in self.children:
                    if hasattr(item, 'disabled'):
                        item.disabled = True
            else:
                # Some strains still pending - show current status
                embed = discord.Embed(
                    title="📋 Pending Product Submissions",
                    description="Click the number buttons to approve products:",
                    color=discord.Color.orange()
                )
                
                for i, strain in enumerate(self.pending_strains[:9], 1):
                    index = i - 1  # Convert to 0-based index
                    is_approved = index in self.approved_indices
                    status = "✅ **Approved**" if is_approved else "⏳ **Pending**"
                    category = strain.get('Category', 'flower')
                    category_emojis = {"flower": "🌿", "hash": "🍯", "rosin": "🧈"}
                    producer = strain.get('Producer', 'Unknown')
                    
                    embed.add_field(
                        name=f"{i}. {category_emojis.get(category, '🌿')} {strain['Strain_Name']}",
                        value=f"ID: `{strain.get('Unique_ID', 'N/A')}`\nCategory: {self.bot.category_names.get(category, 'Flower')}\nProducer: {producer}\nHarvest: {strain.get('Harvest_Date', 'N/A')}\nPackage: {strain.get('Package_Date', 'N/A')}\nStatus: {status}",
                        inline=True
                    )
                
                pending_count = len(self.pending_strains[:9]) - len(self.approved_indices)
                if len(self.pending_strains) > 9:
                    embed.set_footer(text=f"Showing first 9 of {len(self.pending_strains)} pending submissions • {pending_count} remaining to approve")
                else:
                    embed.set_footer(text=f"{pending_count} remaining to approve")
            
            return embed
            
        except Exception as e:
            logger.error(f"Failed to create updated embed: {e}")
            # Fallback embed
            return discord.Embed(
                title="📋 Pending Product Submissions",
                description="Error updating display. Please try refreshing.",
                color=discord.Color.red()
            )
    
    async def update_original_message(self, message):
        """Update the original message to show current status of all strains - DEPRECATED"""
        # This method is now deprecated in favor of immediate updates in the callback
        # Keeping for backwards compatibility but it's no longer used
        pass

class EnhancedStrainBot(commands.Bot):
    """Enhanced strain bot with categories support, producer tracking, and fixed approval flow"""
    
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        
        super().__init__(
            command_prefix='!',
            intents=intents,
            description="Enhanced cannabis product rating bot with categories and producer support"
        )
        
        # Initialize components
        self.sheets_manager = OptimizedSheetsManager(Config.CREDENTIALS_PATH, Config.SPREADSHEET_ID)
        self.rate_limiter = AdvancedRateLimiter()
        self.health_monitor = HealthMonitor(self)
        self.validator = InputValidator()
        
        # Categories
        self.valid_categories = ['flower', 'hash', 'rosin']
        self.category_emojis = {"flower": "🌿", "hash": "🍯", "rosin": "🧈"}
        self.category_names = {"flower": "Flower", "hash": "Hash", "rosin": "Rosin"}
        
        # Producers - will be loaded from Google Sheets on startup
        self.valid_producers = []  # Empty initially, loaded from sheets
        
        # Statistics tracking
        self.command_stats = {
            'submit_strain': 0,
            'rate_strain': 0,
            'view_strain': 0,
            'search_strain': 0,
            'approve_strain': 0,
            'pending_strains': 0,
            'last_submissions': 0,
            'last_ratings': 0,
            'list_strains': 0,
            'rename_strain': 0,
            'refresh_status': 0,
            'bot_stats': 0,
            'add_producer': 0,
            'remove_producer': 0,  # NEW command
            'list_producers': 0
        }
        
        # Moderator notification tracking
        self.last_moderator_notification = 0
        self.notification_cooldown = 3600  # 1 hour cooldown
        
        # Persistent status messages (one for each category + recent submissions)
        self.status_channel = None
        self.top_strains_messages = {}  # {"flower": message, "hash": message, "rosin": message}
        self.recent_ratings_message = None
        self.recent_submissions_message = None  # NEW: for last submissions
        self.status_update_lock = asyncio.Lock()
    
    def get_user_display_name(self, user) -> str:
        """Get the best display name with explicit fallback handling"""
        try:
            # Server nickname takes highest priority (for Member objects)
            if hasattr(user, 'nick') and user.nick:
                return user.nick
            
            # Global display name second (for User/Member objects)
            if hasattr(user, 'global_name') and user.global_name:
                return user.global_name
            
            # Username fallback (for User/Member objects)
            if hasattr(user, 'name') and user.name:
                return user.name
            
            # If all else fails, try display_name property
            if hasattr(user, 'display_name'):
                return user.display_name
            
            # Ultimate fallback - convert to string
            return str(user)
        except Exception as e:
            logger.warning(f"Error getting display name for user: {e}")
            # Try one more fallback with user ID
            try:
                return f"User-{user.id}" if hasattr(user, 'id') else "Unknown User"
            except:
                return "Unknown User"
    
    async def resolve_user_display_name(self, user_id: int) -> str:
        """Comprehensive user display resolution with fallbacks for any user ID"""
        try:
            # Primary: Try to fetch from Discord API
            user = await self.fetch_user(user_id)
            if user:
                display_name = self.get_user_display_name(user)
                return display_name
        except discord.NotFound:
            # User deleted their account
            pass
        except discord.HTTPException:
            # API error, temporary issue
            pass
        except Exception as e:
            logger.warning(f"Error fetching user {user_id}: {e}")
        
        # Secondary: Try the bot's cache
        try:
            user = self.get_user(user_id)
            if user:
                return self.get_user_display_name(user)
        except Exception as e:
            logger.warning(f"Error getting cached user {user_id}: {e}")
        
        # Tertiary: Generic fallback with partial ID
        return f"Former Member ({str(user_id)[-4:]})"
    
    async def setup_hook(self):
        """Called when bot is starting up"""
        try:
            # Start health monitoring server
            await self.health_monitor.start_health_server(Config.HEALTH_CHECK_PORT)
            
            # Sync commands for testing guild
            if Config.GUILD_ID:
                guild = discord.Object(id=Config.GUILD_ID)
                self.tree.copy_global_to(guild=guild)
                await self.tree.sync(guild=guild)
                logger.info(f"Commands synced to guild {Config.GUILD_ID}")
            
            logger.info("Bot setup completed successfully")
        except Exception as e:
            logger.error(f"Error during bot setup: {e}", exc_info=True)
            raise
    
    async def on_ready(self):
        logger.info(f'{self.user} has connected to Discord!')
        logger.info(f'Bot is in {len(self.guilds)} guilds')
        
        # Load valid producers from Google Sheets
        await self.load_producers()
        
        # Setup persistent status messages
        await self.setup_status_messages()
    
    async def load_producers(self):
        """Load valid producers from Google Sheets"""
        try:
            producers = await self.sheets_manager.get_all_producers()
            if producers:
                self.valid_producers = producers
                logger.info(f"Loaded {len(producers)} producers from Google Sheets: {', '.join(producers)}")
            else:
                # Fallback to defaults if sheets loading fails
                self.valid_producers = [
                    "Hollandse Hoogtes", "Q-Farms", "Fyta", 
                    "Aardachtig", "Canadelaar", "Holigram"
                ]
                logger.warning("Failed to load producers from sheets, using defaults")
        except Exception as e:
            logger.error(f"Error loading producers: {e}")
            # Fallback to defaults
            self.valid_producers = [
                "Hollandse Hoogtes", "Q-Farms", "Fyta", 
                "Aardachtig", "Canadelaar", "Holigram"
            ]
    
    async def setup_status_messages(self):
        """Setup persistent status messages in the designated channel"""
        if not Config.STATUS_CHANNEL_ID:
            logger.info("Status channel not configured - skipping persistent messages")
            return
        
        try:
            self.status_channel = self.get_channel(Config.STATUS_CHANNEL_ID)
            if not self.status_channel:
                logger.warning(f"Could not find status channel with ID {Config.STATUS_CHANNEL_ID}")
                return
            
            # Look for existing status messages
            async for message in self.status_channel.history(limit=100):
                if message.author == self.user and message.embeds:
                    title = message.embeds[0].title
                    if "🏆 Top 10" in title:
                        if "Flower" in title:
                            self.top_strains_messages["flower"] = message
                        elif "Hash" in title:
                            self.top_strains_messages["hash"] = message
                        elif "Rosin" in title:
                            self.top_strains_messages["rosin"] = message
                    elif title == "⭐ Recent Ratings":
                        self.recent_ratings_message = message
                    elif title == "📋 Recent Submissions":  # NEW
                        self.recent_submissions_message = message
            
            # Create initial status messages if they don't exist
            for category in self.valid_categories:
                if category not in self.top_strains_messages:
                    embed = discord.Embed(
                        title=f"🏆 Top 10 {self.category_names[category]} Products",
                        description="Loading product data...",
                        color=discord.Color.gold()
                    )
                    embed.set_footer(text="Updates automatically when new ratings are added")
                    self.top_strains_messages[category] = await self.status_channel.send(embed=embed)
            
            if not self.recent_ratings_message:
                embed = discord.Embed(
                    title="⭐ Recent Ratings",
                    description="Loading recent activity...",
                    color=discord.Color.blue()
                )
                embed.set_footer(text="Shows the last 10 ratings • Updates automatically")
                self.recent_ratings_message = await self.status_channel.send(embed=embed)
            
            # NEW: Create recent submissions message
            if not self.recent_submissions_message:
                embed = discord.Embed(
                    title="📋 Recent Submissions",
                    description="Loading recent submissions...",
                    color=discord.Color.purple()
                )
                embed.set_footer(text="Shows the last 10 submissions • Updates automatically")
                self.recent_submissions_message = await self.status_channel.send(embed=embed)
            
            # Update with current data
            await self.update_status_messages()
            
            logger.info("Status messages setup complete")
            
        except Exception as e:
            logger.error(f"Error setting up status messages: {e}")
    
    async def update_status_messages(self):
        """Update the persistent status messages with current data (with proper usernames)"""
        if not self.status_channel:
            return
        
        async with self.status_update_lock:
            try:
                # Update top strains messages for each category
                for category in self.valid_categories:
                    if category in self.top_strains_messages:
                        top_strains = await self.sheets_manager.get_top_strains_for_status(category, 10)
                        
                        top_embed = discord.Embed(
                            title=f"🏆 Top 10 {self.category_names[category]} Products",
                            color=discord.Color.gold(),
                            timestamp=datetime.utcnow()
                        )
                        
                        if top_strains:
                            strain_list = []
                            for i, strain in enumerate(top_strains, 1):
                                harvest_date = strain.get('Harvest_Date', 'N/A')
                                package_date = strain.get('Package_Date', 'N/A')
                                producer = strain.get('Producer', 'Unknown')
                                rating = float(strain.get('Average_Rating', 0))
                                # Create star emoji representation (round to nearest whole number for display)
                                star_count = round(rating)
                                rating_stars = "⭐" * star_count
                                
                                strain_list.append(
                                    f"**{i}.** {strain['Strain_Name']} - {strain['Average_Rating']}/10 {rating_stars}\n"
                                    f"     `{strain.get('Unique_ID', 'N/A')}` • {strain['Total_Ratings']} ratings • {producer}\n"
                                    f"     Harvest: {harvest_date} • Package: {package_date}"
                                )
                            top_embed.description = "\n\n".join(strain_list)
                        else:
                            top_embed.description = f"No rated {category} products yet. Be the first to rate!"
                        
                        top_embed.set_footer(text="Updates automatically when new ratings are added")
                        await self.top_strains_messages[category].edit(embed=top_embed)
                
                # Update recent ratings message with usernames and categories
                if self.recent_ratings_message:
                    recent_ratings = await self.sheets_manager.get_recent_ratings_for_status(10)
                    
                    ratings_embed = discord.Embed(
                        title="⭐ Recent Ratings",
                        color=discord.Color.blue(),
                        timestamp=datetime.utcnow()
                    )
                    
                    if recent_ratings:
                        ratings_list = []
                        for rating in recent_ratings:
                            rating_stars = "⭐" * int(rating.get('Rating', 0))
                            date_str = rating.get('Date_Rated', 'Unknown')[:10]  # Just the date part
                            harvest_date = rating.get('Harvest_Date', 'N/A')
                            package_date = rating.get('Package_Date', 'N/A')
                            category = rating.get('Category', 'flower')
                            producer = rating.get('Producer', 'Unknown')
                            
                            # Use stored username from the database (already handled by sheets manager)
                            username = rating.get('Username_Display', 'Unknown User')
                            category_emoji = self.category_emojis.get(category, '🌿')
                            
                            ratings_list.append(
                                f"{category_emoji} **{rating.get('Strain_Name', 'Unknown')}** - {rating.get('Rating', 'N/A')}/10 {rating_stars}\n"
                                f"     By: {username} • {date_str} • {producer}\n"
                                f"     Harvest: {harvest_date} • Package: {package_date}"
                            )
                        ratings_embed.description = "\n\n".join(ratings_list)
                    else:
                        ratings_embed.description = "No ratings yet. Submit `/rate_strain` to get started!"
                    
                    ratings_embed.set_footer(text="Shows the last 10 ratings • Updates automatically")
                    await self.recent_ratings_message.edit(embed=ratings_embed)
                
                # NEW: Update recent submissions message
                if self.recent_submissions_message:
                    recent_submissions = await self.sheets_manager.get_last_submissions(10)
                    
                    submissions_embed = discord.Embed(
                        title="📋 Recent Submissions",
                        color=discord.Color.purple(),
                        timestamp=datetime.utcnow()
                    )
                    
                    if recent_submissions:
                        submissions_list = []
                        for submission in recent_submissions:
                            category = submission.get('Category', 'flower')
                            category_emoji = self.category_emojis.get(category, '🌿')
                            producer = submission.get('Producer', 'Unknown')
                            date_str = submission.get('Date_Added', 'Unknown')[:10]  # Just the date part
                            
                            # Use stored username from database or resolve from Discord
                            user_id = submission.get('User_ID_Clean', 0)
                            stored_username = submission.get('Username', '').strip()
                            username = stored_username if stored_username else await self.resolve_user_display_name(user_id)
                            
                            submissions_list.append(
                                f"{category_emoji} **{submission.get('Strain_Name', 'Unknown')}**\n"
                                f"     By: {username} • {date_str} • {producer}\n"
                                f"     ID: `{submission.get('Unique_ID', 'N/A')}`"
                            )
                        submissions_embed.description = "\n\n".join(submissions_list)
                    else:
                        submissions_embed.description = "No submissions yet. Submit `/submit_strain` to get started!"
                    
                    submissions_embed.set_footer(text="Shows the last 10 submissions • Updates automatically")
                    await self.recent_submissions_message.edit(embed=submissions_embed)
                
                logger.debug("Status messages updated successfully")
                
            except Exception as e:
                logger.error(f"Error updating status messages: {e}")
    
    def is_moderator(self, user: discord.Member) -> bool:
        """Check if user has moderator permissions with support for multiple roles and hierarchy"""
        if not user or not user.roles:
            return False
        
        user_roles = [role.id for role in user.roles]
        user_highest_position = max(role.position for role in user.roles)
        
        # Check if user has any of the specified moderator roles
        moderator_role_ids = getattr(Config, 'MODERATOR_ROLE_IDS', [Config.MODERATOR_ROLE_ID])
        if not isinstance(moderator_role_ids, list):
            moderator_role_ids = [moderator_role_ids]
        
        # Direct role match
        if any(role_id in user_roles for role_id in moderator_role_ids):
            return True
        
        # Check hierarchical permissions if enabled
        if getattr(Config, 'HIERARCHICAL_PERMISSIONS', False):
            # Find the position of the moderator role in the guild
            moderator_role_positions = []
            for role_id in moderator_role_ids:
                role = discord.utils.get(user.guild.roles, id=role_id)
                if role:
                    moderator_role_positions.append(role.position)
            
            if moderator_role_positions:
                min_moderator_position = min(moderator_role_positions)
                return user_highest_position >= min_moderator_position
        
        return False
    
    async def log_command_usage(self, interaction: discord.Interaction, command_name: str, success: bool = True):
        """Log command usage for monitoring"""
        if command_name in self.command_stats:
            self.command_stats[command_name] += 1
        
        extra = {
            'user_id': interaction.user.id,
            'guild_id': interaction.guild.id if interaction.guild else None,
            'command_name': command_name,
            'success': success
        }
        
        if success:
            logger.info(f"Command executed successfully: {command_name}", extra=extra)
        else:
            logger.warning(f"Command failed: {command_name}", extra=extra)
    
    async def check_and_notify_moderators(self, guild: discord.Guild):
        """Check for pending strains and notify moderators if needed"""
        try:
            current_time = datetime.now().timestamp()
            
            # Check cooldown
            if current_time - self.last_moderator_notification < self.notification_cooldown:
                return
            
            # Get pending count
            pending_count = await self.sheets_manager.get_pending_strains_count()
            
            if pending_count > 0:
                # Find moderators - support multiple roles
                moderator_role_ids = getattr(Config, 'MODERATOR_ROLE_IDS', [Config.MODERATOR_ROLE_ID])
                if not isinstance(moderator_role_ids, list):
                    moderator_role_ids = [moderator_role_ids]
                
                moderator_mentions = []
                for role_id in moderator_role_ids:
                    role = discord.utils.get(guild.roles, id=role_id)
                    if role and role.members:
                        moderator_mentions.append(role.mention)
                
                if moderator_mentions and self.status_channel:
                    embed = discord.Embed(
                        title="📋 Moderator Alert",
                        description=f"There {'is' if pending_count == 1 else 'are'} **{pending_count}** product{'s' if pending_count != 1 else ''} pending approval.",
                        color=discord.Color.orange()
                    )
                    embed.add_field(
                        name="Action Required",
                        value="Use `/pending_strains` to view and approve with buttons.",
                        inline=False
                    )
                    embed.set_footer(text="This notification appears once per hour when products are pending.")
                    
                    # Send message and delete after 30 minutes
                    mentions_text = " ".join(moderator_mentions)
                    msg = await self.status_channel.send(mentions_text, embed=embed)
                    await asyncio.sleep(1800)  # 30 minutes
                    try:
                        await msg.delete()
                    except:
                        pass
                    
                    self.last_moderator_notification = current_time
                    logger.info(f"Moderator notification sent for {pending_count} pending products")
        except Exception as e:
            logger.error(f"Error in moderator notification: {e}")

# Initialize bot instance
bot = EnhancedStrainBot()

# Enhanced input validator with DD-MM-YYYY support
class EnhancedInputValidator(InputValidator):
    @staticmethod
    def validate_date_dd_mm_yyyy(date_str: str) -> bool:
        """Validate date format (DD-MM-YYYY)"""
        try:
            datetime.strptime(date_str, '%d-%m-%Y')
            return True
        except ValueError:
            return False

# Update bot validator
bot.validator = EnhancedInputValidator()

# All Commands are EPHEMERAL by default
@bot.tree.command(name="submit_strain", description="Submit a new cannabis product (flower, hash, or rosin)")
async def submit_strain(interaction: discord.Interaction):
    """Submit new product using category and producer selection and modal form"""
    view = CategoryProducerSelectView(bot.valid_producers)
    embed = discord.Embed(
        title="📝 Submit New Product",
        description="Choose a category and producer to submit your cannabis product:",
        color=discord.Color.blue()
    )
    embed.add_field(name="🌿 Flower", value="Cannabis flower/bud products", inline=True)
    embed.add_field(name="🍯 Hash", value="Hash products", inline=True)
    embed.add_field(name="🧈 Rosin", value="Rosin products", inline=True)
    embed.add_field(
        name="📋 Available Producers", 
        value=", ".join(bot.valid_producers), 
        inline=False
    )
    embed.set_footer(text="Select a category and producer from the dropdowns below")
    
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

@bot.tree.command(name="rate_strain", description="Rate an approved product (1-10 scale)")
@app_commands.describe(
    identifier="Product name or unique ID",
    rating="Rating from 1-10 (10 being the best)",
    category="Filter by category (optional)"
)
@app_commands.choices(category=[
    app_commands.Choice(name="🌿 Flower", value="flower"),
    app_commands.Choice(name="🍯 Hash", value="hash"),
    app_commands.Choice(name="🧈 Rosin", value="rosin")
])
async def rate_strain(
    interaction: discord.Interaction, 
    identifier: str, 
    rating: app_commands.Range[int, 1, 10],
    category: Optional[str] = None
):
    """Rate product with enhanced identifier support and category filtering"""
    
    # Rate limiting check
    if not await bot.rate_limiter.check_user_limit(interaction.user.id, Config.RATE_LIMIT_PER_USER):
        await interaction.response.send_message(
            "⏰ You're rating too quickly! Please wait before trying again.",
            ephemeral=True
        )
        await bot.log_command_usage(interaction, 'rate_strain', success=False)
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        # Validate inputs
        cleaned_identifier = identifier.strip()
        if not cleaned_identifier or not bot.validator.validate_rating(rating):
            await interaction.edit_original_response(content="❌ Invalid product identifier or rating.")
            await bot.log_command_usage(interaction, 'rate_strain', success=False)
            return
        
        # Check if strain exists and is approved
        strain_data = await bot.sheets_manager.get_strain_by_identifier(cleaned_identifier, category)
        if not strain_data:
            category_filter = f" in {bot.category_names.get(category, 'unknown')} category" if category else ""
            await interaction.edit_original_response(content=f"❌ Product '{cleaned_identifier}'{category_filter} not found.")
            await bot.log_command_usage(interaction, 'rate_strain', success=False)
            return
        
        if strain_data['Status'] != 'Approved':
            await interaction.edit_original_response(content=f"❌ Product '{strain_data['Strain_Name']}' is not yet approved for rating.")
            await bot.log_command_usage(interaction, 'rate_strain', success=False)
            return
        
        # Get user display name
        username = bot.get_user_display_name(interaction.user)
        
        # Add rating with username
        success = await bot.sheets_manager.add_rating(cleaned_identifier, interaction.user.id, rating, username, category)
        
        if success:
            # Get updated strain data
            updated_strain = await bot.sheets_manager.get_strain_by_identifier(cleaned_identifier, category)
            
            product_category = strain_data.get('Category', 'flower')
            category_emoji = bot.category_emojis.get(product_category, '🌿')
            category_name = bot.category_names.get(product_category, 'Flower')
            producer = strain_data.get('Producer', 'Unknown')
            
            embed = discord.Embed(
                title="✅ Rating Submitted Successfully",
                description=f"You rated {category_emoji} **{strain_data['Strain_Name']}** {rating}/10",
                color=discord.Color.green()
            )
            
            embed.add_field(name="Unique ID", value=f"`{strain_data.get('Unique_ID', 'N/A')}`", inline=True)
            embed.add_field(name="Category", value=f"{category_emoji} {category_name}", inline=True)
            embed.add_field(name="Producer", value=producer, inline=True)
            embed.add_field(name="Rated by", value=username, inline=True)
            
            if updated_strain:
                avg_rating = updated_strain.get('Average_Rating', 0)
                total_ratings = updated_strain.get('Total_Ratings', 0)
                if total_ratings > 0:
                    embed.add_field(
                        name="Current Stats", 
                        value=f"Average: {avg_rating}/10\nTotal Ratings: {total_ratings}", 
                        inline=True
                    )
                else:
                    embed.add_field(name="Current Stats", value="First rating!", inline=True)
            
            embed.set_footer(text="Check the status channel for public updates")
            
            await interaction.edit_original_response(embed=embed)
            await bot.log_command_usage(interaction, 'rate_strain', success=True)
            
            # Update status messages after successful rating
            await bot.update_status_messages()
            
        else:
            await interaction.edit_original_response(content="❌ Failed to submit rating. You may have already rated this product.")
            await bot.log_command_usage(interaction, 'rate_strain', success=False)
    
    except Exception as e:
        logger.error(f"Error in rate_strain: {e}", exc_info=True)
        await interaction.edit_original_response(content="❌ An error occurred while submitting your rating.")
        await bot.log_command_usage(interaction, 'rate_strain', success=False)

@bot.tree.command(name="view_strain", description="View product information and ratings with recent activity")
@app_commands.describe(
    identifier="Product name or unique ID",
    category="Filter by category (optional)"
)
@app_commands.choices(category=[
    app_commands.Choice(name="🌿 Flower", value="flower"),
    app_commands.Choice(name="🍯 Hash", value="hash"),
    app_commands.Choice(name="🧈 Rosin", value="rosin")
])
async def view_strain(interaction: discord.Interaction, identifier: str, category: Optional[str] = None):
    """Display product information with last 5 ratings, proper usernames, and producer info"""
    await interaction.response.defer(ephemeral=True)
    
    try:
        cleaned_identifier = identifier.strip()
        strain_data = await bot.sheets_manager.get_strain_by_identifier(cleaned_identifier, category)
        
        if not strain_data:
            category_filter = f" in {bot.category_names.get(category, 'unknown')} category" if category else ""
            await interaction.edit_original_response(content=f"❌ Product '{cleaned_identifier}'{category_filter} not found.")
            await bot.log_command_usage(interaction, 'view_strain', success=False)
            return
        
        product_category = strain_data.get('Category', 'flower')
        category_emoji = bot.category_emojis.get(product_category, '🌿')
        category_name = bot.category_names.get(product_category, 'Flower')
        producer = strain_data.get('Producer', 'Unknown')
        
        embed = discord.Embed(
            title=f"{category_emoji} {strain_data['Strain_Name']}",
            color=discord.Color.green() if strain_data['Status'] == 'Approved' else discord.Color.orange()
        )
        
        embed.add_field(name="Unique ID", value=f"`{strain_data.get('Unique_ID', 'N/A')}`", inline=True)
        embed.add_field(name="Category", value=f"{category_emoji} {category_name}", inline=True)
        embed.add_field(name="Producer", value=producer, inline=True)
        embed.add_field(name="Status", value=strain_data['Status'], inline=True)
        
        # Add harvest and package dates
        if strain_data.get('Harvest_Date'):
            embed.add_field(name="Harvest Date", value=strain_data['Harvest_Date'], inline=True)
        if strain_data.get('Package_Date'):
            embed.add_field(name="Package Date", value=strain_data['Package_Date'], inline=True)
        
        embed.add_field(name="Date Added", value=strain_data['Date_Added'], inline=True)
        
        if strain_data['Status'] == 'Approved':
            total_ratings = int(strain_data.get('Total_Ratings', 0))
            if total_ratings > 0:
                rating_display = f"{strain_data['Average_Rating']}/10"
                embed.add_field(name="Average Rating", value=rating_display, inline=True)
                embed.add_field(name="Total Ratings", value=str(total_ratings), inline=True)
                
                # Get last 5 ratings with usernames (already handled by sheets manager)
                recent_ratings = await bot.sheets_manager.get_strain_ratings_with_users(
                    strain_data.get('Unique_ID', ''), 5
                )
                
                if recent_ratings:
                    ratings_text = []
                    for rating in recent_ratings:
                        rating_value = rating.get('Rating', 'N/A')
                        date_rated = rating.get('Date_Rated', 'Unknown')[:10]
                        
                        # Use username already provided by sheets manager
                        username = rating.get('Username_Display', 'Unknown User')
                        
                        ratings_text.append(f"{rating_value}/10 by {username} ({date_rated})")
                    
                    embed.add_field(
                        name="Recent Ratings",
                        value="\n".join(ratings_text),
                        inline=False
                    )
            else:
                embed.add_field(name="Rating", value="No ratings yet", inline=True)
                embed.add_field(name="Total Ratings", value="0", inline=True)
        else:
            embed.add_field(name="Note", value="Pending moderator approval", inline=False)
        
        embed.set_footer(text="Use /rate_strain to add your rating!" if strain_data['Status'] == 'Approved' else "Waiting for moderator approval")
        
        await interaction.edit_original_response(embed=embed)
        await bot.log_command_usage(interaction, 'view_strain', success=True)
    
    except Exception as e:
        logger.error(f"Error in view_strain: {e}", exc_info=True)
        await interaction.edit_original_response(content="❌ An error occurred while fetching product information.")
        await bot.log_command_usage(interaction, 'view_strain', success=False)

@bot.tree.command(name="pending_strains", description="[MODERATOR] List pending products with numbered approval buttons")
async def pending_strains(interaction: discord.Interaction):
    """List pending products with clickable numbered buttons for approval"""
    if not bot.is_moderator(interaction.user):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        await bot.log_command_usage(interaction, 'pending_strains', success=False)
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        pending = await bot.sheets_manager.get_pending_strains()
        
        if not pending:
            embed = discord.Embed(
                title="📋 Pending Product Submissions",
                description="No products pending approval.",
                color=discord.Color.blue()
            )
            await interaction.edit_original_response(embed=embed)
            await bot.log_command_usage(interaction, 'pending_strains', success=True)
            return
        
        # Show up to 9 pending strains with numbered buttons
        display_count = min(len(pending), 9)
        
        embed = discord.Embed(
            title="📋 Pending Product Submissions",
            description="Click the number buttons to approve products:",
            color=discord.Color.orange()
        )
        
        for i, strain in enumerate(pending[:9], 1):
            category = strain.get('Category', 'flower')
            category_emoji = bot.category_emojis.get(category, '🌿')
            producer = strain.get('Producer', 'Unknown')
            
            embed.add_field(
                name=f"{i}. {category_emoji} {strain['Strain_Name']}",
                value=f"ID: `{strain.get('Unique_ID', 'N/A')}`\nCategory: {bot.category_names.get(category, 'Flower')}\nProducer: {producer}\nHarvest: {strain.get('Harvest_Date', 'N/A')}\nPackage: {strain.get('Package_Date', 'N/A')}\nStatus: ⏳ **Pending**",
                inline=True
            )
        
        if len(pending) > 9:
            embed.set_footer(text=f"Showing first 9 of {len(pending)} pending submissions")
        else:
            embed.set_footer(text=f"{len(pending)} products pending approval")
        
        # Create view with numbered buttons
        view = PendingApprovalView(pending[:9], bot)
        
        await interaction.edit_original_response(embed=embed, view=view)
        await bot.log_command_usage(interaction, 'pending_strains', success=True)
    
    except Exception as e:
        logger.error(f"Error in pending_strains: {e}", exc_info=True)
        await interaction.edit_original_response(content="❌ An error occurred while fetching pending products.")
        await bot.log_command_usage(interaction, 'pending_strains', success=False)

@bot.tree.command(name="add_producer", description="[MODERATOR] Add a new producer to the list")
@app_commands.describe(producer_name="Name of the producer to add")
async def add_producer(interaction: discord.Interaction, producer_name: str):
    """Add a new producer to the valid list with persistent storage"""
    if not bot.is_moderator(interaction.user):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        await bot.log_command_usage(interaction, 'add_producer', success=False)
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        # Validate producer name
        cleaned_name = producer_name.strip()
        if not cleaned_name or len(cleaned_name) < 2 or len(cleaned_name) > 50:
            await interaction.edit_original_response(
                content="❌ Invalid producer name. Please use 2-50 characters."
            )
            await bot.log_command_usage(interaction, 'add_producer', success=False)
            return
        
        # Add producer to Google Sheets (this checks for duplicates)
        success = await bot.sheets_manager.add_producer(cleaned_name)
        
        if success:
            # Reload producers from sheets to update the bot's list
            await bot.load_producers()
            
            embed = discord.Embed(
                title="✅ Producer Added Successfully",
                description=f"**{cleaned_name}** has been added to the list of valid producers.",
                color=discord.Color.green()
            )
            embed.add_field(
                name="Total Producers", 
                value=str(len(bot.valid_producers)), 
                inline=True
            )
            embed.add_field(
                name="All Producers", 
                value=", ".join(bot.valid_producers), 
                inline=False
            )
            embed.add_field(
                name="Storage", 
                value="✅ Saved to Google Sheets (persistent across bot restarts)", 
                inline=False
            )
            embed.set_footer(text=f"Added by {bot.get_user_display_name(interaction.user)}")
            
            await interaction.edit_original_response(embed=embed)
            await bot.log_command_usage(interaction, 'add_producer', success=True)
        else:
            await interaction.edit_original_response(
                content=f"❌ Producer '{cleaned_name}' already exists or failed to add."
            )
            await bot.log_command_usage(interaction, 'add_producer', success=False)
        
    except Exception as e:
        logger.error(f"Error in add_producer: {e}", exc_info=True)
        await interaction.edit_original_response(content="❌ An error occurred while adding the producer.")
        await bot.log_command_usage(interaction, 'add_producer', success=False)

@bot.tree.command(name="remove_producer", description="[MODERATOR] Remove a producer from the list")
@app_commands.describe(producer_name="Name of the producer to remove")
async def remove_producer(interaction: discord.Interaction, producer_name: str):
    """Remove a producer from the valid list with persistent storage"""
    if not bot.is_moderator(interaction.user):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        await bot.log_command_usage(interaction, 'remove_producer', success=False)
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        # Validate producer name
        cleaned_name = producer_name.strip()
        if not cleaned_name:
            await interaction.edit_original_response(
                content="❌ Invalid producer name."
            )
            await bot.log_command_usage(interaction, 'remove_producer', success=False)
            return
        
        # Check if producer exists in current list
        if cleaned_name not in bot.valid_producers:
            await interaction.edit_original_response(
                content=f"❌ Producer '{cleaned_name}' not found in the current list."
            )
            await bot.log_command_usage(interaction, 'remove_producer', success=False)
            return
        
        # Remove producer from Google Sheets
        success = await bot.sheets_manager.remove_producer(cleaned_name)
        
        if success:
            # Reload producers from sheets to update the bot's list
            await bot.load_producers()
            
            embed = discord.Embed(
                title="✅ Producer Removed Successfully",
                description=f"**{cleaned_name}** has been removed from the list of valid producers.",
                color=discord.Color.green()
            )
            embed.add_field(
                name="Total Producers", 
                value=str(len(bot.valid_producers)), 
                inline=True
            )
            embed.add_field(
                name="Remaining Producers", 
                value=", ".join(bot.valid_producers) if bot.valid_producers else "None", 
                inline=False
            )
            embed.add_field(
                name="Storage", 
                value="✅ Removed from Google Sheets (persistent across bot restarts)", 
                inline=False
            )
            embed.set_footer(text=f"Removed by {bot.get_user_display_name(interaction.user)}")
            
            await interaction.edit_original_response(embed=embed)
            await bot.log_command_usage(interaction, 'remove_producer', success=True)
        else:
            await interaction.edit_original_response(
                content=f"❌ Failed to remove producer '{cleaned_name}'. Please try again."
            )
            await bot.log_command_usage(interaction, 'remove_producer', success=False)
        
    except Exception as e:
        logger.error(f"Error in remove_producer: {e}", exc_info=True)
        await interaction.edit_original_response(content="❌ An error occurred while removing the producer.")
        await bot.log_command_usage(interaction, 'remove_producer', success=False)

@bot.tree.command(name="list_producers", description="View all available producers")
async def list_producers(interaction: discord.Interaction):
    """List all valid producers with persistent storage info"""
    embed = discord.Embed(
        title="📋 Available Producers",
        description=f"Currently {len(bot.valid_producers)} producers available:",
        color=discord.Color.blue()
    )
    
    if bot.valid_producers:
        # Display producers in a nice format
        producers_text = "\n".join([f"• {producer}" for producer in bot.valid_producers])
        embed.add_field(name="Producers", value=producers_text, inline=False)
    else:
        embed.add_field(name="Producers", value="No producers configured", inline=False)
    
    embed.add_field(
        name="Storage", 
        value="📊 Data stored in Google Sheets (persistent across bot restarts)", 
        inline=False
    )
    embed.set_footer(text="Moderators can add/remove producers with /add_producer and /remove_producer")
    
    await interaction.response.send_message(embed=embed, ephemeral=True)
    await bot.log_command_usage(interaction, 'list_producers', success=True)

@bot.tree.command(name="search_strain", description="Search for products with wildcard support")
@app_commands.describe(
    query="Search query (supports * and ? wildcards)",
    category="Filter by category (optional)"
)
@app_commands.choices(category=[
    app_commands.Choice(name="🌿 Flower", value="flower"),
    app_commands.Choice(name="🍯 Hash", value="hash"),
    app_commands.Choice(name="🧈 Rosin", value="rosin")
])
async def search_strain(interaction: discord.Interaction, query: str, category: Optional[str] = None):
    """Search for products with harvest/package date display, producer info, and category filtering"""
    await interaction.response.defer(ephemeral=True)
    
    try:
        cleaned_query = query.strip()
        if len(cleaned_query) < 2:
            await interaction.edit_original_response(content="❌ Search query must be at least 2 characters long.")
            await bot.log_command_usage(interaction, 'search_strain', success=False)
            return
        
        results = await bot.sheets_manager.search_strains(cleaned_query, category)
        
        if not results:
            category_filter = f" in {bot.category_names.get(category, 'unknown')} category" if category else ""
            await interaction.edit_original_response(content=f"❌ No products found matching '{cleaned_query}'{category_filter}.")
            await bot.log_command_usage(interaction, 'search_strain', success=False)
            return
        
        category_filter = f" ({bot.category_names.get(category, 'All categories')})" if category else ""
        embed = discord.Embed(
            title=f"🔍 Search Results for '{cleaned_query}'{category_filter}",
            description=f"Found {len(results)} product(s):",
            color=discord.Color.blue()
        )
        
        for strain in results:
            status_emoji = "✅" if strain['Status'] == 'Approved' else "⏳"
            harvest_date = strain.get('Harvest_Date', 'N/A')
            package_date = strain.get('Package_Date', 'N/A')
            product_category = strain.get('Category', 'flower')
            category_emoji = bot.category_emojis.get(product_category, '🌿')
            producer = strain.get('Producer', 'Unknown')
            
            embed.add_field(
                name=f"{status_emoji} {category_emoji} {strain['Strain_Name']}",
                value=f"ID: `{strain.get('Unique_ID', 'N/A')}`\nCategory: {bot.category_names.get(product_category, 'Flower')}\nProducer: {producer}\nHarvest: {harvest_date}\nPackage: {package_date}",
                inline=True
            )
        
        embed.set_footer(text="Use /view_strain <name or ID> for detailed information")
        
        await interaction.edit_original_response(embed=embed)
        await bot.log_command_usage(interaction, 'search_strain', success=True)
    
    except Exception as e:
        logger.error(f"Error in search_strain: {e}", exc_info=True)
        await interaction.edit_original_response(content="❌ An error occurred while searching products.")
        await bot.log_command_usage(interaction, 'search_strain', success=False)

@bot.tree.command(name="list_strains", description="List all approved products")
@app_commands.describe(category="Filter by category (optional)")
@app_commands.choices(category=[
    app_commands.Choice(name="🌿 Flower", value="flower"),
    app_commands.Choice(name="🍯 Hash", value="hash"),
    app_commands.Choice(name="🧈 Rosin", value="rosin")
])
async def list_strains(interaction: discord.Interaction, category: Optional[str] = None):
    """List all approved products with category filtering and producer info"""
    await interaction.response.defer(ephemeral=True)
    
    try:
        strains = await bot.sheets_manager.get_all_approved_strains(category)
        
        if not strains:
            category_filter = f" {bot.category_names.get(category, 'unknown')} " if category else " "
            await interaction.edit_original_response(content=f"❌ No approved{category_filter}products found.")
            await bot.log_command_usage(interaction, 'list_strains', success=True)
            return
        
        category_filter = f" {bot.category_names.get(category, 'All')} " if category else " "
        category_emoji = bot.category_emojis.get(category, '🌿') if category else '📋'
        
        embed = discord.Embed(
            title=f"{category_emoji} All Approved{category_filter}Products ({len(strains)} total)",
            description="Sorted by average rating (highest first, unrated at end)",
            color=discord.Color.green()
        )
        
        # Show first 15 strains to avoid embed limits
        for i, strain in enumerate(strains[:15], 1):
            total_ratings = int(strain.get('Total_Ratings', 0))
            harvest_date = strain.get('Harvest_Date', 'N/A')
            package_date = strain.get('Package_Date', 'N/A')
            product_category = strain.get('Category', 'flower')
            strain_category_emoji = bot.category_emojis.get(product_category, '🌿')
            producer = strain.get('Producer', 'Unknown')
            
            if total_ratings > 0:
                rating_text = f"{strain['Average_Rating']}/10"
            else:
                rating_text = "No ratings yet"
            
            embed.add_field(
                name=f"{i}. {strain_category_emoji} {strain['Strain_Name']}",
                value=f"ID: `{strain.get('Unique_ID', 'N/A')}`\nRating: {rating_text} ({total_ratings} ratings)\nProducer: {producer}\nHarvest: {harvest_date}\nPackage: {package_date}",
                inline=True
            )
        
        if len(strains) > 15:
            embed.set_footer(text=f"Showing first 15 of {len(strains)} products • Check status channel for top products")
        else:
            embed.set_footer(text="Check status channel for top products")
        
        await interaction.edit_original_response(embed=embed)
        await bot.log_command_usage(interaction, 'list_strains', success=True)
    
    except Exception as e:
        logger.error(f"Error in list_strains: {e}", exc_info=True)
        await interaction.edit_original_response(content="❌ An error occurred while fetching product list.")
        await bot.log_command_usage(interaction, 'list_strains', success=False)

@bot.tree.command(name="last_submissions", description="View the last 10 product submissions")
async def last_submissions(interaction: discord.Interaction):
    """Show last 10 product submissions with proper usernames and producer info"""
    await interaction.response.defer(ephemeral=True)
    
    try:
        submissions = await bot.sheets_manager.get_last_submissions(10)
        
        if not submissions:
            embed = discord.Embed(
                title="📋 Recent Submissions",
                description="No recent submissions found.",
                color=discord.Color.blue()
            )
            await interaction.edit_original_response(embed=embed)
            await bot.log_command_usage(interaction, 'last_submissions', success=True)
            return
        
        embed = discord.Embed(
            title="📋 Last 10 Product Submissions",
            color=discord.Color.blue()
        )
        
        for i, submission in enumerate(submissions, 1):
            user_id = submission.get('User_ID_Clean', 0)
            # Use stored username from database if available, otherwise resolve from Discord
            stored_username = submission.get('Username', '').strip()
            username = stored_username if stored_username else await bot.resolve_user_display_name(user_id)
            
            category = submission.get('Category', 'flower')
            category_emoji = bot.category_emojis.get(category, '🌿')
            producer = submission.get('Producer', 'Unknown')
            
            embed.add_field(
                name=f"{i}. {category_emoji} {submission['Strain_Name']}",
                value=f"ID: `{submission.get('Unique_ID', 'N/A')}`\nCategory: {bot.category_names.get(category, 'Flower')}\nProducer: {producer}\nBy: {username}\nHarvest: {submission.get('Harvest_Date', 'N/A')}\nPackage: {submission.get('Package_Date', 'N/A')}\nSubmitted: {submission.get('Date_Added', 'N/A')}",
                inline=False
            )
        
        embed.set_footer(text="This information is only visible to you")
        await interaction.edit_original_response(embed=embed)
        await bot.log_command_usage(interaction, 'last_submissions', success=True)
    
    except Exception as e:
        logger.error(f"Error in last_submissions: {e}", exc_info=True)
        await interaction.edit_original_response(content="❌ An error occurred while fetching recent submissions.")
        await bot.log_command_usage(interaction, 'last_submissions', success=False)

@bot.tree.command(name="last_ratings", description="View the last 10 product ratings")
async def last_ratings(interaction: discord.Interaction):
    """Show last 10 product ratings with proper usernames and producer info"""
    await interaction.response.defer(ephemeral=True)
    
    try:
        ratings = await bot.sheets_manager.get_last_ratings(10)
        
        if not ratings:
            embed = discord.Embed(
                title="⭐ Recent Ratings",
                description="No recent ratings found.",
                color=discord.Color.blue()
            )
            await interaction.edit_original_response(embed=embed)
            await bot.log_command_usage(interaction, 'last_ratings', success=True)
            return
        
        embed = discord.Embed(
            title="⭐ Last 10 Product Ratings",
            color=discord.Color.gold()
        )
        
        for i, rating in enumerate(ratings, 1):
            rating_stars = "⭐" * int(rating.get('Rating', 0))
            # Use username already provided by sheets manager
            username = rating.get('Username_Display', 'Unknown User')
            category = rating.get('Category', 'flower')
            category_emoji = bot.category_emojis.get(category, '🌿')
            producer = rating.get('Producer', 'Unknown')
            
            embed.add_field(
                name=f"{i}. {category_emoji} {rating.get('Strain_Name', 'Unknown')}",
                value=f"ID: `{rating.get('Unique_ID', 'N/A')}`\nCategory: {bot.category_names.get(category, 'Flower')}\nProducer: {producer}\nRating: {rating.get('Rating', 'N/A')}/10 {rating_stars}\nBy: {username}\nRated: {rating.get('Date_Rated', 'N/A')}",
                inline=False
            )
        
        embed.set_footer(text="Check the status channel for public recent ratings")
        await interaction.edit_original_response(embed=embed)
        await bot.log_command_usage(interaction, 'last_ratings', success=True)
    
    except Exception as e:
        logger.error(f"Error in last_ratings: {e}", exc_info=True)
        await interaction.edit_original_response(content="❌ An error occurred while fetching recent ratings.")
        await bot.log_command_usage(interaction, 'last_ratings', success=False)

@bot.tree.command(name="approve_strain", description="[MODERATOR] Approve a pending product submission")
@app_commands.describe(identifier="Product name or unique ID")
async def approve_strain(interaction: discord.Interaction, identifier: str):
    """Approve product with enhanced identifier support (traditional command)"""
    if not bot.is_moderator(interaction.user):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        await bot.log_command_usage(interaction, 'approve_strain', success=False)
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        cleaned_identifier = identifier.strip()
        if not cleaned_identifier:
            await interaction.edit_original_response(content="❌ Invalid product identifier.")
            await bot.log_command_usage(interaction, 'approve_strain', success=False)
            return
        
        # Check strain status
        strain_data = await bot.sheets_manager.get_strain_by_identifier(cleaned_identifier)
        if not strain_data:
            await interaction.edit_original_response(content=f"❌ Product '{cleaned_identifier}' not found.")
            await bot.log_command_usage(interaction, 'approve_strain', success=False)
            return
        
        if strain_data['Status'] == 'Approved':
            await interaction.edit_original_response(content=f"❌ Product '{strain_data['Strain_Name']}' is already approved.")
            await bot.log_command_usage(interaction, 'approve_strain', success=False)
            return
        
        # Approve strain
        success = await bot.sheets_manager.approve_strain(cleaned_identifier)
        
        if success:
            category = strain_data.get('Category', 'flower')
            category_emoji = bot.category_emojis.get(category, '🌿')
            category_name = bot.category_names.get(category, 'Flower')
            producer = strain_data.get('Producer', 'Unknown')
            
            embed = discord.Embed(
                title="✅ Product Approved",
                description=f"{category_emoji} **{strain_data['Strain_Name']}** has been approved and is now available for rating.",
                color=discord.Color.green()
            )
            embed.add_field(name="Unique ID", value=f"`{strain_data.get('Unique_ID', 'N/A')}`", inline=True)
            embed.add_field(name="Category", value=f"{category_emoji} {category_name}", inline=True)
            embed.add_field(name="Producer", value=producer, inline=True)
            embed.add_field(name="Harvest Date", value=strain_data.get('Harvest_Date', 'N/A'), inline=True)
            embed.add_field(name="Package Date", value=strain_data.get('Package_Date', 'N/A'), inline=True)
            embed.set_footer(text=f"Approved by {bot.get_user_display_name(interaction.user)}")
            await interaction.edit_original_response(embed=embed)
            await bot.log_command_usage(interaction, 'approve_strain', success=True)
            
            # Update status messages after approval
            await bot.update_status_messages()
            
        else:
            await interaction.edit_original_response(content="❌ Failed to approve product. Please try again.")
            await bot.log_command_usage(interaction, 'approve_strain', success=False)
    
    except Exception as e:
        logger.error(f"Error in approve_strain: {e}", exc_info=True)
        await interaction.edit_original_response(content="❌ An error occurred while approving the product.")
        await bot.log_command_usage(interaction, 'approve_strain', success=False)

@bot.tree.command(name="rename_strain", description="[MODERATOR] Rename a product using its unique ID")
@app_commands.describe(
    unique_id="Unique ID of the product to rename",
    new_name="New name for the product"
)
async def rename_strain(interaction: discord.Interaction, unique_id: str, new_name: str):
    """Rename a product by unique ID"""
    if not bot.is_moderator(interaction.user):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        await bot.log_command_usage(interaction, 'rename_strain', success=False)
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        # Validate new name
        validated_name = bot.validator.validate_strain_name(new_name)
        if not validated_name:
            await interaction.edit_original_response(
                content="❌ Invalid new product name. Please use 2-50 characters with letters, numbers, spaces, and basic punctuation only."
            )
            await bot.log_command_usage(interaction, 'rename_strain', success=False)
            return
        
        # Get current strain data
        strain_data = await bot.sheets_manager.get_strain_by_identifier(unique_id.strip())
        if not strain_data:
            await interaction.edit_original_response(content=f"❌ Product with ID '{unique_id}' not found.")
            await bot.log_command_usage(interaction, 'rename_strain', success=False)
            return
        
        old_name = strain_data.get('Strain_Name', 'Unknown')
        category = strain_data.get('Category', 'flower')
        category_emoji = bot.category_emojis.get(category, '🌿')
        producer = strain_data.get('Producer', 'Unknown')
        
        # Rename the strain
        success = await bot.sheets_manager.rename_strain(unique_id.strip(), validated_name)
        
        if success:
            embed = discord.Embed(
                title="✅ Product Renamed Successfully",
                description=f"{category_emoji} Product has been renamed from **{old_name}** to **{validated_name}**",
                color=discord.Color.green()
            )
            embed.add_field(name="Unique ID", value=f"`{unique_id}`", inline=True)
            embed.add_field(name="Category", value=f"{category_emoji} {bot.category_names.get(category, 'Flower')}", inline=True)
            embed.add_field(name="Producer", value=producer, inline=True)
            embed.add_field(name="Old Name", value=old_name, inline=True)
            embed.add_field(name="New Name", value=validated_name, inline=True)
            embed.set_footer(text=f"Renamed by {bot.get_user_display_name(interaction.user)}")
            
            await interaction.edit_original_response(embed=embed)
            await bot.log_command_usage(interaction, 'rename_strain', success=True)
            
            # Update status messages after rename
            await bot.update_status_messages()
            
        else:
            await interaction.edit_original_response(content="❌ Failed to rename product. Please try again.")
            await bot.log_command_usage(interaction, 'rename_strain', success=False)
    
    except Exception as e:
        logger.error(f"Error in rename_strain: {e}", exc_info=True)
        await interaction.edit_original_response(content="❌ An error occurred while renaming the product.")
        await bot.log_command_usage(interaction, 'rename_strain', success=False)

@bot.tree.command(name="refresh_status", description="[MODERATOR] Remove old status messages and repost fresh ones")
async def refresh_status(interaction: discord.Interaction):
    """Remove old status messages and create fresh ones"""
    if not bot.is_moderator(interaction.user):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        await bot.log_command_usage(interaction, 'refresh_status', success=False)
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        if not bot.status_channel:
            await interaction.edit_original_response(content="❌ Status channel not configured.")
            await bot.log_command_usage(interaction, 'refresh_status', success=False)
            return
        
        # Count messages to delete
        messages_to_delete = []
        
        # Look through recent message history for bot's status messages
        async for message in bot.status_channel.history(limit=200):
            if message.author == bot.user and message.embeds:
                embed_title = message.embeds[0].title
                # Check if it's one of our status message types
                if any(pattern in embed_title for pattern in [
                    "🏆 Top 10", "⭐ Recent Ratings", "📋 Recent Submissions", "Top 10 Flower", 
                    "Top 10 Hash", "Top 10 Rosin"
                ]):
                    messages_to_delete.append(message)
        
        # Delete old status messages
        deleted_count = 0
        for message in messages_to_delete:
            try:
                await message.delete()
                deleted_count += 1
                # Small delay to avoid rate limits
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning(f"Failed to delete message {message.id}: {e}")
        
        # Clear the bot's internal message references
        bot.top_strains_messages.clear()
        bot.recent_ratings_message = None
        bot.recent_submissions_message = None
        
        # Wait a moment before creating new messages
        await asyncio.sleep(1)
        
        # Create fresh status messages
        for category in bot.valid_categories:
            embed = discord.Embed(
                title=f"🏆 Top 10 {bot.category_names[category]} Products",
                description="Loading product data...",
                color=discord.Color.gold()
            )
            embed.set_footer(text="Updates automatically when new ratings are added")
            bot.top_strains_messages[category] = await bot.status_channel.send(embed=embed)
            
            # Small delay between messages
            await asyncio.sleep(0.5)
        
        # Create recent ratings message
        embed = discord.Embed(
            title="⭐ Recent Ratings",
            description="Loading recent activity...",
            color=discord.Color.blue()
        )
        embed.set_footer(text="Shows the last 10 ratings • Updates automatically")
        bot.recent_ratings_message = await bot.status_channel.send(embed=embed)
        
        # Create recent submissions message
        embed = discord.Embed(
            title="📋 Recent Submissions",
            description="Loading recent submissions...",
            color=discord.Color.purple()
        )
        embed.set_footer(text="Shows the last 10 submissions • Updates automatically")
        bot.recent_submissions_message = await bot.status_channel.send(embed=embed)
        
        # Update with current data
        await bot.update_status_messages()
        
        # Send confirmation
        embed = discord.Embed(
            title="✅ Status Messages Refreshed",
            description=f"Successfully cleaned up and reposted status messages.",
            color=discord.Color.green()
        )
        embed.add_field(name="Messages Deleted", value=str(deleted_count), inline=True)
        embed.add_field(name="Messages Created", value=str(len(bot.valid_categories) + 2), inline=True)  # +2 for ratings and submissions
        embed.add_field(name="Channel", value=bot.status_channel.mention, inline=True)
        embed.set_footer(text=f"Refreshed by {bot.get_user_display_name(interaction.user)}")
        
        await interaction.edit_original_response(embed=embed)
        await bot.log_command_usage(interaction, 'refresh_status', success=True)
        
        logger.info(f"Status messages refreshed by {bot.get_user_display_name(interaction.user)}. Deleted: {deleted_count}, Created: {len(bot.valid_categories) + 2}")
    
    except Exception as e:
        logger.error(f"Error in refresh_status: {e}", exc_info=True)
        await interaction.edit_original_response(content="❌ An error occurred while refreshing status messages.")
        await bot.log_command_usage(interaction, 'refresh_status', success=False)

@bot.tree.command(name="bot_stats", description="[MODERATOR] View bot statistics and health")
async def bot_stats(interaction: discord.Interaction):
    """Display enhanced bot statistics"""
    if not bot.is_moderator(interaction.user):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        # Check for pending strains notification
        await bot.check_and_notify_moderators(interaction.guild)
        
        embed = discord.Embed(
            title="🤖 Bot Statistics",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        
        # Basic stats
        embed.add_field(name="Guilds", value=str(len(bot.guilds)), inline=True)
        embed.add_field(name="Latency", value=f"{round(bot.latency * 1000, 2)}ms", inline=True)
        
        # Pending strains count
        pending_count = await bot.sheets_manager.get_pending_strains_count()
        embed.add_field(name="Pending Products", value=str(pending_count), inline=True)
        
        # Status channel info
        status_channel_status = "✅ Active" if bot.status_channel else "❌ Not configured"
        embed.add_field(name="Status Channel", value=status_channel_status, inline=True)
        
        # Command usage stats
        total_commands = sum(bot.command_stats.values())
        embed.add_field(name="Total Commands Used", value=str(total_commands), inline=True)
        
        # Most used commands
        if total_commands > 0:
            most_used = max(bot.command_stats.items(), key=lambda x: x[1])
            embed.add_field(name="Most Used Command", value=f"{most_used[0]} ({most_used[1]})", inline=True)
        
        # Cache stats
        cache_size = len(bot.sheets_manager.cache)
        embed.add_field(name="Cache Entries", value=str(cache_size), inline=True)
        
        # Producers count
        embed.add_field(name="Producers", value=str(len(bot.valid_producers)), inline=True)
        
        # Categories info
        embed.add_field(name="Supported Categories", value="🌿 Flower, 🍯 Hash, 🧈 Rosin", inline=False)
        
        # Command breakdown
        command_list = "\n".join([f"{cmd}: {count}" for cmd, count in bot.command_stats.items() if count > 0])
        if command_list:
            embed.add_field(name="Command Usage Breakdown", value=f"```{command_list}```", inline=False)
        
        if pending_count > 0:
            embed.add_field(name="⚠️ Action Required", value=f"{pending_count} product(s) need approval! Use `/pending_strains` for quick approval.", inline=False)
        
        embed.set_footer(text="This information is only visible to you")
        await interaction.edit_original_response(embed=embed)
        await bot.log_command_usage(interaction, 'bot_stats', success=True)
    
    except Exception as e:
        logger.error(f"Error in bot_stats: {e}", exc_info=True)
        await interaction.edit_original_response(content="❌ An error occurred while fetching bot statistics.")
        await bot.log_command_usage(interaction, 'bot_stats', success=False)

# Error handling and shutdown
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Enhanced error handling with logging"""
    if isinstance(error, app_commands.CommandOnCooldown):
        await interaction.response.send_message(
            f"⏰ Command is on cooldown. Try again in {error.retry_after:.1f} seconds.",
            ephemeral=True
        )
    elif isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message(
            "❌ You don't have permission to use this command.",
            ephemeral=True
        )
    else:
        logger.error(f"Unhandled command error: {error}", exc_info=True)
        try:
            await interaction.response.send_message(
                "❌ An unexpected error occurred. Please try again later.",
                ephemeral=True
            )
        except:
            pass

async def shutdown_handler():
    """Handle graceful shutdown"""
    logger.info("Shutting down bot...")
    
    # Stop health monitor
    if hasattr(bot, 'health_monitor'):
        await bot.health_monitor.stop_health_server()
    
    # Close database connections
    if hasattr(bot, 'sheets_manager') and hasattr(bot.sheets_manager, 'executor'):
        bot.sheets_manager.executor.shutdown(wait=True)
    
    logger.info("Bot shutdown complete")

async def main():
    try:
        Config.validate()
        logger.info("Starting enhanced Discord cannabis product rating bot v7 with fixed approval flow, producer support, and multiple moderator roles...")
        
        async with bot:
            try:
                await bot.start(Config.TOKEN)
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
            finally:
                await shutdown_handler()
                
    except Exception as e:
        logger.error(f"Failed to start bot: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
