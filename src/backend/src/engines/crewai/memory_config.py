"""
Memory configuration module for CrewAI engine.

This module provides utilities to configure and manage memory settings
for CrewAI agents and crews, including short-term, long-term, and entity memory.
"""

import os
import logging
import shutil
import json
import sqlite3
import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

# Configure logging
logger = logging.getLogger(__name__)

# Get the absolute path to the project directory
current_dir = Path(__file__).parent  # /src/engines/crewai
project_root = current_dir.parent.parent.parent  # Go up to the project root

# Define memory storage directory
MEMORY_DIR = Path(os.environ.get('CREWAI_STORAGE_DIR', project_root / 'memory'))
os.makedirs(MEMORY_DIR, exist_ok=True)

logger.info(f"Memory directory initialized at: {MEMORY_DIR}")

class MemoryConfig:
    """Memory configuration for CrewAI agents and crews."""
    
    @staticmethod
    def list_crew_memories(custom_path: Optional[str] = None) -> List[str]:
        """
        List all crew memory directories.
        
        Args:
            custom_path: Optional custom path to look for memories
            
        Returns:
            List[str]: List of crew names with memory storage
        """
        memory_dir = Path(custom_path) if custom_path else MEMORY_DIR
        
        if not memory_dir.exists():
            logger.warning(f"Memory directory does not exist: {memory_dir}")
            return []
            
        return [d.name for d in memory_dir.iterdir() if d.is_dir()]
    
    @staticmethod
    def reset_crew_memory(crew_name: str, custom_path: Optional[str] = None) -> bool:
        """
        Reset memory for a specific crew.
        
        Args:
            crew_name: Name of the crew to reset memory for
            custom_path: Optional custom path to look for memories
            
        Returns:
            bool: True if successful, False otherwise
        """
        memory_dir = Path(custom_path) if custom_path else MEMORY_DIR
        crew_dir = memory_dir / crew_name
        
        if not crew_dir.exists():
            logger.warning(f"No memory found for crew '{crew_name}' in {memory_dir}")
            return False
            
        try:
            # Remove and recreate the directory
            shutil.rmtree(crew_dir)
            os.makedirs(crew_dir, exist_ok=True)
            logger.info(f"Memory reset for crew '{crew_name}' in {memory_dir}")
            return True
        except Exception as e:
            logger.error(f"Error resetting memory for crew '{crew_name}': {e}")
            return False
    
    @staticmethod
    def delete_crew_memory(crew_name: str, custom_path: Optional[str] = None) -> bool:
        """
        Delete memory for a specific crew.
        
        Args:
            crew_name: Name of the crew to delete memory for
            custom_path: Optional custom path to look for memories
            
        Returns:
            bool: True if successful, False otherwise
        """
        memory_dir = Path(custom_path) if custom_path else MEMORY_DIR
        crew_dir = memory_dir / crew_name
        
        if not crew_dir.exists():
            logger.warning(f"No memory found for crew '{crew_name}' in {memory_dir}")
            return False
            
        try:
            # Force delete the directory without recreating it
            logger.info(f"Attempting to delete directory: {crew_dir}")
            # Try a more robust deletion method
            shutil.rmtree(crew_dir, ignore_errors=True)
            
            # Double-check if the directory was actually removed
            if crew_dir.exists():
                logger.warning(f"Directory still exists after rmtree: {crew_dir}")
                # Try again with os.rmdir as a fallback
                for item in crew_dir.glob('*'):
                    if item.is_file():
                        item.unlink()
                    elif item.is_dir():
                        shutil.rmtree(item)
                os.rmdir(crew_dir)
            
            logger.info(f"Memory deleted for crew '{crew_name}' in {memory_dir}")
            return True
        except Exception as e:
            logger.error(f"Error deleting memory for crew '{crew_name}': {e}", exc_info=True)
            return False
    
    @staticmethod
    def reset_all_memories(custom_path: Optional[str] = None) -> bool:
        """
        Reset all crew memories.
        
        Args:
            custom_path: Optional custom path to look for memories
            
        Returns:
            bool: True if successful, False otherwise
        """
        memory_dir = Path(custom_path) if custom_path else MEMORY_DIR
        
        if not memory_dir.exists():
            logger.warning(f"Memory directory does not exist: {memory_dir}")
            return False
            
        try:
            # Get all crew directories
            crew_dirs = [d for d in memory_dir.iterdir() if d.is_dir()]
            
            # Reset each crew's memory
            for crew_dir in crew_dirs:
                shutil.rmtree(crew_dir)
                os.makedirs(crew_dir, exist_ok=True)
                logger.info(f"Memory reset for crew '{crew_dir.name}' in {memory_dir}")
                
            logger.info(f"All memories reset successfully ({len(crew_dirs)} crews) in {memory_dir}")
            return True
        except Exception as e:
            logger.error(f"Error resetting all memories: {e}")
            return False
            
    @staticmethod
    def get_crew_memory_details(crew_name: str, custom_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a crew's memory.
        
        Args:
            crew_name: Name of the crew
            custom_path: Optional custom path to look for memories
            
        Returns:
            Optional[Dict[str, Any]]: Dictionary with memory details, or None if not found
        """
        memory_dir = Path(custom_path) if custom_path else MEMORY_DIR
        crew_dir = memory_dir / crew_name
        
        if not crew_dir.exists():
            logger.warning(f"No memory found for crew '{crew_name}' in {memory_dir}")
            return None
            
        try:
            # Get basic memory information
            memory_info = {
                'crew_name': crew_name,
                'memory_path': str(crew_dir),
                'creation_date': datetime.datetime.fromtimestamp(crew_dir.stat().st_ctime).isoformat(),
                'last_modified': datetime.datetime.fromtimestamp(crew_dir.stat().st_mtime).isoformat(),
                'size_bytes': sum(f.stat().st_size for f in crew_dir.glob('**/*') if f.is_file()),
            }
            
            # Check for long-term memory database
            ltm_db_path = crew_dir / "long_term_memory.db"
            if ltm_db_path.exists():
                memory_info['long_term_memory'] = {
                    'path': str(ltm_db_path),
                    'size_bytes': ltm_db_path.stat().st_size,
                }
                
                # Try to get record count from database
                try:
                    conn = sqlite3.connect(str(ltm_db_path))
                    cursor = conn.cursor()
                    
                    # Get table names first
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    tables = [row[0] for row in cursor.fetchall()]
                    memory_info['long_term_memory']['tables'] = tables
                    
                    if tables:
                        # Look for likely memory tables - either "memories" or something with "memory"
                        memory_table = None
                        for table in tables:
                            if table.lower() == "memories" or "memory" in table.lower():
                                memory_table = table
                                break
                        
                        # If no memory table found, use the first table
                        if not memory_table and tables:
                            memory_table = tables[0]
                        
                        if memory_table:
                            # Get column names
                            cursor.execute(f"PRAGMA table_info({memory_table})")
                            columns = [row[1] for row in cursor.fetchall()]
                            memory_info['long_term_memory']['columns'] = columns
                            
                            # Get record count
                            cursor.execute(f"SELECT COUNT(*) FROM {memory_table}")
                            memory_info['long_term_memory']['record_count'] = cursor.fetchone()[0]
                            
                            # Try to get content fields (typically would be text, content, memory, etc.)
                            content_field = None
                            for column in columns:
                                if column.lower() in ['text', 'content', 'memory', 'message']:
                                    content_field = column
                                    break
                            
                            id_field = 'id' if 'id' in columns else 'rowid'
                            
                            # Get timestamp field if available
                            timestamp_field = None
                            for column in columns:
                                if 'time' in column.lower() or 'date' in column.lower():
                                    timestamp_field = column
                                    break
                            
                            # Get sample records
                            query = f"SELECT {id_field}"
                            if content_field:
                                query += f", {content_field}"
                            if timestamp_field:
                                query += f", {timestamp_field}"
                            query += f" FROM {memory_table} LIMIT 5"
                            
                            cursor.execute(query)
                            rows = cursor.fetchall()
                            
                            # Prepare sample data
                            samples = []
                            for row in rows:
                                sample = {'id': row[0]}
                                if content_field:
                                    # Limit content length for readability
                                    content = row[1] if row[1] else ""
                                    if isinstance(content, str) and len(content) > 200:
                                        content = content[:200] + "..."
                                    sample['content'] = content
                                if timestamp_field:
                                    sample['timestamp'] = row[-1]
                                samples.append(sample)
                            
                            memory_info['long_term_memory']['samples'] = samples
                    
                    conn.close()
                except Exception as e:
                    logger.error(f"Error querying long-term memory database: {e}")
                    memory_info['long_term_memory']['error'] = str(e)
            
            # Check for other memory files
            memory_info['files'] = []
            for file_path in crew_dir.glob('*'):
                if file_path.is_file() and file_path.name != "long_term_memory.db":
                    file_info = {
                        'name': file_path.name,
                        'size_bytes': file_path.stat().st_size,
                        'last_modified': datetime.datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
                    }
                    
                    # Try to interpret file contents based on extension
                    if file_path.suffix.lower() == '.json':
                        try:
                            with open(file_path, 'r') as f:
                                file_info['content'] = json.load(f)
                        except:
                            file_info['content_error'] = "Could not parse JSON file"
                    
                    memory_info['files'].append(file_info)
            
            return memory_info
        except Exception as e:
            logger.error(f"Error getting memory details for crew '{crew_name}': {e}")
            return None
    
    @staticmethod
    def search_memories(query: str, custom_path: Optional[str] = None) -> List[Tuple[str, str]]:
        """
        Search all crew memories for a query string.
        
        Args:
            query: Search query string
            custom_path: Optional custom path to look for memories
            
        Returns:
            List[Tuple[str, str]]: List of (crew_name, memory_text) matching the query
        """
        memory_dir = Path(custom_path) if custom_path else MEMORY_DIR
        
        if not memory_dir.exists():
            logger.warning(f"Memory directory does not exist: {memory_dir}")
            return []
            
        results = []
        try:
            # Get all crew directories
            crew_dirs = [d for d in memory_dir.iterdir() if d.is_dir()]
            
            for crew_dir in crew_dirs:
                crew_name = crew_dir.name
                
                # Check long-term memory database
                ltm_db_path = crew_dir / "long_term_memory.db"
                if ltm_db_path.exists():
                    try:
                        conn = sqlite3.connect(str(ltm_db_path))
                        cursor = conn.cursor()
                        
                        # Get table names first
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                        tables = [row[0] for row in cursor.fetchall()]
                        
                        for table in tables:
                            # Get column names
                            cursor.execute(f"PRAGMA table_info({table})")
                            columns = [row[1] for row in cursor.fetchall()]
                            
                            # Try to identify content columns
                            content_columns = []
                            for column in columns:
                                if column.lower() in ['text', 'content', 'memory', 'message']:
                                    content_columns.append(column)
                            
                            # If no content columns found, use all columns
                            if not content_columns:
                                content_columns = columns
                            
                            # Search each content column
                            for column in content_columns:
                                # Use LIKE for case-insensitive substring search
                                cursor.execute(f"SELECT {column} FROM {table} WHERE {column} LIKE ?", (f"%{query}%",))
                                matches = cursor.fetchall()
                                
                                for match in matches:
                                    if match[0]:  # Skip null values
                                        results.append((crew_name, match[0]))
                        
                        conn.close()
                    except Exception as e:
                        logger.error(f"Error searching memory database for crew '{crew_name}': {e}")
                
                # Check other memory files
                for file_path in crew_dir.glob('*'):
                    if file_path.is_file() and file_path.suffix.lower() == '.json':
                        try:
                            with open(file_path, 'r') as f:
                                content = f.read()
                                if query.lower() in content.lower():
                                    results.append((crew_name, content))
                        except:
                            pass
            
            return results
        except Exception as e:
            logger.error(f"Error searching memories: {e}")
            return []
    
    @staticmethod
    def cleanup_old_memories(days: int = 30, custom_path: Optional[str] = None) -> int:
        """
        Delete memories older than the specified number of days.
        
        Args:
            days: Number of days to retain memories (older than this will be deleted)
            custom_path: Optional custom path to look for memories
            
        Returns:
            int: Number of crew memories deleted
        """
        memory_dir = Path(custom_path) if custom_path else MEMORY_DIR
        
        if not memory_dir.exists():
            logger.warning(f"Memory directory does not exist: {memory_dir}")
            return 0
            
        try:
            # Calculate cutoff date
            cutoff_time = datetime.datetime.now() - datetime.timedelta(days=days)
            cutoff_timestamp = cutoff_time.timestamp()
            
            deleted_count = 0
            # Get all crew directories
            crew_dirs = [d for d in memory_dir.iterdir() if d.is_dir()]
            
            for crew_dir in crew_dirs:
                mod_time = crew_dir.stat().st_mtime
                if mod_time < cutoff_timestamp:
                    crew_name = crew_dir.name
                    if MemoryConfig.delete_crew_memory(crew_name, custom_path):
                        deleted_count += 1
                        logger.info(f"Deleted old memory for crew '{crew_name}' (last modified: {datetime.datetime.fromtimestamp(mod_time).isoformat()})")
            
            return deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up old memories: {e}")
            return 0
    
    @staticmethod
    def get_memory_stats(custom_path: Optional[str] = None, detailed: bool = False) -> Dict[str, Any]:
        """
        Get statistics about memory usage.
        
        Args:
            custom_path: Optional custom path to look for memories
            detailed: Whether to include detailed information about each crew memory
            
        Returns:
            Dict[str, Any]: Memory statistics
        """
        memory_dir = Path(custom_path) if custom_path else MEMORY_DIR
        
        if not memory_dir.exists():
            logger.warning(f"Memory directory does not exist: {memory_dir}")
            return {
                "exists": False,
                "path": str(memory_dir)
            }
            
        try:
            # Get all crew directories
            crew_dirs = [d for d in memory_dir.iterdir() if d.is_dir()]
            
            # Calculate total size
            total_size = 0
            for crew_dir in crew_dirs:
                # Sum the size of all files in the directory
                crew_size = sum(f.stat().st_size for f in crew_dir.glob('**/*') if f.is_file())
                total_size += crew_size
            
            # Basic stats
            memory_stats = {
                "exists": True,
                "path": str(memory_dir),
                "crew_count": len(crew_dirs),
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "created": datetime.datetime.fromtimestamp(memory_dir.stat().st_ctime).isoformat(),
                "last_modified": datetime.datetime.fromtimestamp(memory_dir.stat().st_mtime).isoformat(),
            }
            
            # Add detailed stats if requested
            if detailed:
                crew_stats = []
                for crew_dir in crew_dirs:
                    crew_name = crew_dir.name
                    crew_size = sum(f.stat().st_size for f in crew_dir.glob('**/*') if f.is_file())
                    
                    # Count files and memory records
                    file_count = len(list(crew_dir.glob('*')))
                    memory_record_count = 0
                    
                    # Check for long-term memory database
                    ltm_db_path = crew_dir / "long_term_memory.db"
                    if ltm_db_path.exists():
                        try:
                            conn = sqlite3.connect(str(ltm_db_path))
                            cursor = conn.cursor()
                            
                            # Get table names
                            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                            tables = [row[0] for row in cursor.fetchall()]
                            
                            for table in tables:
                                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                                memory_record_count += cursor.fetchone()[0]
                            
                            conn.close()
                        except:
                            pass
                    
                    crew_stats.append({
                        "crew_name": crew_name,
                        "size_bytes": crew_size,
                        "size_mb": round(crew_size / (1024 * 1024), 2),
                        "file_count": file_count,
                        "memory_record_count": memory_record_count,
                        "created": datetime.datetime.fromtimestamp(crew_dir.stat().st_ctime).isoformat(),
                        "last_modified": datetime.datetime.fromtimestamp(crew_dir.stat().st_mtime).isoformat(),
                    })
                
                # Sort by size (largest first)
                crew_stats.sort(key=lambda x: x["size_bytes"], reverse=True)
                memory_stats["crews"] = crew_stats
            
            return memory_stats
        except Exception as e:
            logger.error(f"Error getting memory stats: {e}")
            return {
                "exists": True,
                "path": str(memory_dir),
                "error": str(e)
            } 