"""
Centralized privilege definitions for the application.

This module defines all available privileges as constants, providing:
- Type safety and IDE autocompletion
- Single source of truth for all privileges
- Easy refactoring and maintenance
- Clear documentation of what each privilege allows
"""

from typing import Dict, List, Tuple


class Privileges:
    """
    Centralized privilege definitions.
    
    All privileges follow the pattern "resource:action" for consistency.
    Use these constants throughout the application instead of hardcoded strings.
    """
    
    # Group Management Privileges
    GROUP_CREATE = "group:create"
    GROUP_READ = "group:read"
    GROUP_UPDATE = "group:update"
    GROUP_DELETE = "group:delete"
    GROUP_MANAGE_USERS = "group:manage_users"
    GROUP_MANAGE_ROLES = "group:manage_roles"
    
    # Legacy Tenant Privileges (for backward compatibility - will be removed)
    TENANT_CREATE = "tenant:create"
    TENANT_READ = "tenant:read"
    TENANT_UPDATE = "tenant:update"
    TENANT_DELETE = "tenant:delete"
    TENANT_MANAGE_USERS = "tenant:manage_users"
    TENANT_MANAGE_ROLES = "tenant:manage_roles"
    
    # Agent Management Privileges
    AGENT_CREATE = "agent:create"
    AGENT_READ = "agent:read"
    AGENT_UPDATE = "agent:update"
    AGENT_DELETE = "agent:delete"
    
    # Task Management Privileges
    TASK_CREATE = "task:create"
    TASK_READ = "task:read"
    TASK_UPDATE = "task:update"
    TASK_DELETE = "task:delete"
    TASK_EXECUTE = "task:execute"
    
    # Crew Management Privileges
    CREW_CREATE = "crew:create"
    CREW_READ = "crew:read"
    CREW_UPDATE = "crew:update"
    CREW_DELETE = "crew:delete"
    CREW_EXECUTE = "crew:execute"
    
    # Execution Management Privileges
    EXECUTION_CREATE = "execution:create"
    EXECUTION_READ = "execution:read"
    EXECUTION_MANAGE = "execution:manage"
    
    # Settings Management Privileges (granular)
    SETTINGS_READ = "settings:read"          # General settings overview
    SETTINGS_UPDATE = "settings:update"      # General settings update
    
    # Tool Management Privileges
    TOOL_CREATE = "tool:create"
    TOOL_READ = "tool:read"
    TOOL_UPDATE = "tool:update"
    TOOL_DELETE = "tool:delete"
    TOOL_CONFIGURE = "tool:configure"        # Configure tool settings/parameters
    
    # Model Configuration Privileges
    MODEL_CREATE = "model:create"
    MODEL_READ = "model:read"
    MODEL_UPDATE = "model:update"
    MODEL_DELETE = "model:delete"
    MODEL_CONFIGURE = "model:configure"      # Configure model parameters/endpoints
    
    # MCP (Model Context Protocol) Privileges
    MCP_CREATE = "mcp:create"
    MCP_READ = "mcp:read"
    MCP_UPDATE = "mcp:update"
    MCP_DELETE = "mcp:delete"
    MCP_CONFIGURE = "mcp:configure"          # Configure MCP servers/settings
    
    # API Key Management Privileges
    API_KEY_CREATE = "api_key:create"
    API_KEY_READ = "api_key:read"
    API_KEY_UPDATE = "api_key:update"
    API_KEY_DELETE = "api_key:delete"
    API_KEY_MANAGE = "api_key:manage"        # Full API key lifecycle management
    
    # User Management Privileges (for group context)
    USER_INVITE = "user:invite"
    USER_REMOVE = "user:remove"
    USER_UPDATE_ROLE = "user:update_role"

    @classmethod
    def get_all_privileges(cls) -> List[Tuple[str, str]]:
        """
        Get all privileges with their descriptions.
        
        Returns:
            List of tuples (privilege_name, description)
        """
        return [
            # Group Management
            (cls.GROUP_CREATE, "Create new groups"),
            (cls.GROUP_READ, "View group information"),
            (cls.GROUP_UPDATE, "Update group details"),
            (cls.GROUP_DELETE, "Delete groups"),
            (cls.GROUP_MANAGE_USERS, "Manage group user memberships"),
            (cls.GROUP_MANAGE_ROLES, "Manage group user roles"),
            
            # Legacy Tenant (for backward compatibility)
            (cls.TENANT_CREATE, "Create new tenants (legacy)"),
            (cls.TENANT_READ, "View tenant information (legacy)"),
            (cls.TENANT_UPDATE, "Update tenant details (legacy)"),
            (cls.TENANT_DELETE, "Delete tenants (legacy)"),
            (cls.TENANT_MANAGE_USERS, "Manage tenant user memberships (legacy)"),
            (cls.TENANT_MANAGE_ROLES, "Manage tenant user roles (legacy)"),
            
            # Agent Management
            (cls.AGENT_CREATE, "Create agents"),
            (cls.AGENT_READ, "View agents"),
            (cls.AGENT_UPDATE, "Update agents"),
            (cls.AGENT_DELETE, "Delete agents"),
            
            # Task Management
            (cls.TASK_CREATE, "Create tasks"),
            (cls.TASK_READ, "View tasks"),
            (cls.TASK_UPDATE, "Update tasks"),
            (cls.TASK_DELETE, "Delete tasks"),
            (cls.TASK_EXECUTE, "Execute tasks"),
            
            # Crew Management
            (cls.CREW_CREATE, "Create crews"),
            (cls.CREW_READ, "View crews"),
            (cls.CREW_UPDATE, "Update crews"),
            (cls.CREW_DELETE, "Delete crews"),
            (cls.CREW_EXECUTE, "Execute crews"),
            
            # Execution Management
            (cls.EXECUTION_CREATE, "Create executions"),
            (cls.EXECUTION_READ, "View executions"),
            (cls.EXECUTION_MANAGE, "Manage executions"),
            
            # Settings Management
            (cls.SETTINGS_READ, "View general settings"),
            (cls.SETTINGS_UPDATE, "Update general settings"),
            
            # Tool Management
            (cls.TOOL_CREATE, "Create new tools"),
            (cls.TOOL_READ, "View tools and their configurations"),
            (cls.TOOL_UPDATE, "Update tool details"),
            (cls.TOOL_DELETE, "Delete tools"),
            (cls.TOOL_CONFIGURE, "Configure tool settings and parameters"),
            
            # Model Configuration
            (cls.MODEL_CREATE, "Create model configurations"),
            (cls.MODEL_READ, "View model configurations"),
            (cls.MODEL_UPDATE, "Update model configurations"),
            (cls.MODEL_DELETE, "Delete model configurations"),
            (cls.MODEL_CONFIGURE, "Configure model parameters and endpoints"),
            
            # MCP (Model Context Protocol)
            (cls.MCP_CREATE, "Create MCP server configurations"),
            (cls.MCP_READ, "View MCP server configurations"),
            (cls.MCP_UPDATE, "Update MCP server configurations"),
            (cls.MCP_DELETE, "Delete MCP server configurations"),
            (cls.MCP_CONFIGURE, "Configure MCP server settings"),
            
            # API Key Management
            (cls.API_KEY_CREATE, "Create API keys"),
            (cls.API_KEY_READ, "View API keys (masked)"),
            (cls.API_KEY_UPDATE, "Update API key details"),
            (cls.API_KEY_DELETE, "Delete API keys"),
            (cls.API_KEY_MANAGE, "Full API key lifecycle management"),
            
            # User Management
            (cls.USER_INVITE, "Invite users to groups"),
            (cls.USER_REMOVE, "Remove users from groups"),
            (cls.USER_UPDATE_ROLE, "Update user roles in groups"),
        ]

    @classmethod
    def get_group_privileges(cls) -> List[str]:
        """Get all group management privileges."""
        return [
            cls.GROUP_CREATE,
            cls.GROUP_READ,
            cls.GROUP_UPDATE,
            cls.GROUP_DELETE,
            cls.GROUP_MANAGE_USERS,
            cls.GROUP_MANAGE_ROLES,
        ]

    @classmethod
    def get_agent_privileges(cls) -> List[str]:
        """Get all agent management privileges."""
        return [
            cls.AGENT_CREATE,
            cls.AGENT_READ,
            cls.AGENT_UPDATE,
            cls.AGENT_DELETE,
        ]

    @classmethod
    def get_task_privileges(cls) -> List[str]:
        """Get all task management privileges."""
        return [
            cls.TASK_CREATE,
            cls.TASK_READ,
            cls.TASK_UPDATE,
            cls.TASK_DELETE,
            cls.TASK_EXECUTE,
        ]

    @classmethod
    def get_crew_privileges(cls) -> List[str]:
        """Get all crew management privileges."""
        return [
            cls.CREW_CREATE,
            cls.CREW_READ,
            cls.CREW_UPDATE,
            cls.CREW_DELETE,
            cls.CREW_EXECUTE,
        ]

    @classmethod
    def get_execution_privileges(cls) -> List[str]:
        """Get all execution management privileges."""
        return [
            cls.EXECUTION_CREATE,
            cls.EXECUTION_READ,
            cls.EXECUTION_MANAGE,
        ]

    @classmethod
    def get_settings_privileges(cls) -> List[str]:
        """Get all general settings management privileges."""
        return [
            cls.SETTINGS_READ,
            cls.SETTINGS_UPDATE,
        ]

    @classmethod
    def get_tool_privileges(cls) -> List[str]:
        """Get all tool management privileges."""
        return [
            cls.TOOL_CREATE,
            cls.TOOL_READ,
            cls.TOOL_UPDATE,
            cls.TOOL_DELETE,
            cls.TOOL_CONFIGURE,
        ]

    @classmethod
    def get_model_privileges(cls) -> List[str]:
        """Get all model configuration privileges."""
        return [
            cls.MODEL_CREATE,
            cls.MODEL_READ,
            cls.MODEL_UPDATE,
            cls.MODEL_DELETE,
            cls.MODEL_CONFIGURE,
        ]

    @classmethod
    def get_mcp_privileges(cls) -> List[str]:
        """Get all MCP (Model Context Protocol) privileges."""
        return [
            cls.MCP_CREATE,
            cls.MCP_READ,
            cls.MCP_UPDATE,
            cls.MCP_DELETE,
            cls.MCP_CONFIGURE,
        ]

    @classmethod
    def get_api_key_privileges(cls) -> List[str]:
        """Get all API key management privileges."""
        return [
            cls.API_KEY_CREATE,
            cls.API_KEY_READ,
            cls.API_KEY_UPDATE,
            cls.API_KEY_DELETE,
            cls.API_KEY_MANAGE,
        ]

    @classmethod
    def get_configuration_privileges(cls) -> List[str]:
        """Get all configuration-related privileges (tools, models, MCP, API keys, settings)."""
        return [
            *cls.get_settings_privileges(),
            *cls.get_tool_privileges(),
            *cls.get_model_privileges(),
            *cls.get_mcp_privileges(),
            *cls.get_api_key_privileges(),
        ]

    @classmethod
    def get_user_privileges(cls) -> List[str]:
        """Get all user management privileges."""
        return [
            cls.USER_INVITE,
            cls.USER_REMOVE,
            cls.USER_UPDATE_ROLE,
        ]

    @classmethod
    def get_legacy_tenant_privileges(cls) -> List[str]:
        """Get legacy tenant privileges (for backward compatibility)."""
        return [
            cls.TENANT_CREATE,
            cls.TENANT_READ,
            cls.TENANT_UPDATE,
            cls.TENANT_DELETE,
            cls.TENANT_MANAGE_USERS,
            cls.TENANT_MANAGE_ROLES,
        ]


class DefaultRoles:
    """
    Default role definitions with their assigned privileges.
    """
    
    @classmethod
    def get_admin_privileges(cls) -> List[str]:
        """Get all privileges for admin role."""
        return [
            # Group management
            *Privileges.get_group_privileges(),
            # Legacy tenant support
            *Privileges.get_legacy_tenant_privileges(),
            # Full resource access
            *Privileges.get_agent_privileges(),
            *Privileges.get_task_privileges(),
            *Privileges.get_crew_privileges(),
            *Privileges.get_execution_privileges(),
            # Full configuration access
            *Privileges.get_configuration_privileges(),
            *Privileges.get_user_privileges(),
        ]

    @classmethod
    def get_manager_privileges(cls) -> List[str]:
        """Get all privileges for manager role."""
        return [
            # User management
            Privileges.USER_INVITE,
            Privileges.USER_UPDATE_ROLE,
            # Resource management (full access)
            Privileges.AGENT_CREATE,
            Privileges.AGENT_READ,
            Privileges.AGENT_UPDATE,
            Privileges.AGENT_DELETE,
            Privileges.TASK_CREATE,
            Privileges.TASK_READ,
            Privileges.TASK_UPDATE,
            Privileges.TASK_DELETE,
            Privileges.CREW_CREATE,
            Privileges.CREW_READ,
            Privileges.CREW_UPDATE,
            Privileges.CREW_DELETE,
            # Execution access
            Privileges.EXECUTION_READ,
            # Limited configuration access (read + configure existing)
            Privileges.TOOL_READ,
            Privileges.TOOL_CONFIGURE,
            Privileges.MODEL_READ,
            Privileges.MODEL_CONFIGURE,
            Privileges.MCP_READ,
            Privileges.SETTINGS_READ,
            # Can view API keys but not create/delete them
            Privileges.API_KEY_READ,
        ]

    @classmethod
    def get_user_privileges(cls) -> List[str]:
        """Get all privileges for user role."""
        return [
            # Read access
            Privileges.AGENT_READ,
            Privileges.TASK_READ,
            Privileges.CREW_READ,
            # Execution permissions
            Privileges.TASK_EXECUTE,
            Privileges.CREW_EXECUTE,
            Privileges.EXECUTION_CREATE,
            Privileges.EXECUTION_READ,
            # Basic configuration read access
            Privileges.TOOL_READ,
            Privileges.MODEL_READ,
            Privileges.SETTINGS_READ,
        ]

    @classmethod
    def get_viewer_privileges(cls) -> List[str]:
        """Get all privileges for viewer role."""
        return [
            # Read-only access
            Privileges.AGENT_READ,
            Privileges.TASK_READ,
            Privileges.CREW_READ,
            Privileges.EXECUTION_READ,
            # Basic read access to configuration
            Privileges.TOOL_READ,
            Privileges.MODEL_READ,
            Privileges.SETTINGS_READ,
        ]

    @classmethod
    def get_all_roles(cls) -> Dict[str, Dict[str, any]]:
        """Get all default roles with their privileges and descriptions."""
        return {
            "admin": {
                "description": "System administrator with full access including group management",
                "privileges": cls.get_admin_privileges()
            },
            "manager": {
                "description": "Team manager who can manage workflows and team members",
                "privileges": cls.get_manager_privileges()
            },
            "user": {
                "description": "Regular user who can execute workflows and view data",
                "privileges": cls.get_user_privileges()
            },
            "viewer": {
                "description": "Read-only access to view workflows and data",
                "privileges": cls.get_viewer_privileges()
            }
        }