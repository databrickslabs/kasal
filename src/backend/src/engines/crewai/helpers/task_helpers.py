"""
Helper functions for working with tasks.

This module provides utility functions for working with CrewAI tasks.
"""

from pathlib import Path
from typing import Optional, Dict, Any, List, Callable, Type
import os
import json
import traceback
from pydantic import BaseModel, create_model

from crewai import Agent, Task
from crewai.tasks.task_output import TaskOutput

from src.core.logger import LoggerManager
from src.engines.crewai.helpers.tool_helpers import resolve_tool_ids_to_names
from src.core.unit_of_work import UnitOfWork


# Get loggers from the centralized logging system
logger = LoggerManager.get_instance().crew
guardrail_logger = LoggerManager.get_instance().guardrails

def is_data_missing(output: TaskOutput) -> bool:
    """
    Check if data is missing from task output
    
    Args:
        output: TaskOutput to check
        
    Returns:
        True if data is missing, False otherwise
    """
    logger.info("=== is_data_missing function called ===")
    logger.info(f"TaskOutput type: {type(output)}")
    
    if not hasattr(output, 'pydantic'):
        logger.info("No pydantic model found in output, returning True")
        return True
    
    logger.info(f"Pydantic model: {output.pydantic}")
    logger.info(f"Checking events length in: {output.pydantic.events}")
    events_count = len(output.pydantic.events)
    result = events_count < 10
    
    logger.info(f"Found {events_count} events. Need at least 10.")
    logger.info(f"Is data missing? {result}")
    return result

async def get_pydantic_class_from_name(schema_name: str) -> Optional[Type[BaseModel]]:
    """
    Get a Pydantic model class by its name from the schema database.
    
    Args:
        schema_name: Name of the schema to retrieve
        
    Returns:
        Pydantic model class if found, else None
    """
    logger.info(f"Looking up schema '{schema_name}' in the database")
    
    try:
        # Use async unit of work
        async with UnitOfWork() as uow:
            # Look up the schema in the database
            schema = await uow.schema_repository.find_by_name(schema_name)
        if not schema:
            logger.warning(f"Schema '{schema_name}' not found in database")
            return None
            
        logger.info(f"Found schema '{schema_name}' in database")
        
        # Get the schema definition
        schema_def = schema.schema_definition
        if not schema_def or not isinstance(schema_def, dict):
            logger.error(f"Invalid schema definition for '{schema_name}': {schema_def}")
            return None
            
        logger.debug(f"Schema definition: {schema_def}")
        
        # Create field definitions for the Pydantic model
        fields = {}
        required_fields = schema_def.get("required", [])
        
        for field_name, field_def in schema_def.get("properties", {}).items():
            field_type = field_def.get("type")
            field_nullable = field_def.get("nullable", False)
            field_default = None if field_name in required_fields else ...
            
            try:
                if field_type == "string":
                    fields[field_name] = (str, field_default)
                elif field_type == "integer":
                    fields[field_name] = (int, field_default)
                elif field_type == "number":
                    fields[field_name] = (float, field_default)
                elif field_type == "boolean":
                    fields[field_name] = (bool, field_default)
                elif field_type == "array":
                    item_type = field_def.get("items", {}).get("type", "string")
                    if item_type == "string":
                        fields[field_name] = (List[str], field_default)
                    elif item_type == "integer":
                        fields[field_name] = (List[int], field_default)
                    elif item_type == "number":
                        fields[field_name] = (List[float], field_default)
                    elif item_type == "boolean":
                        fields[field_name] = (List[bool], field_default)
                    else:
                        fields[field_name] = (List[Any], field_default)
                elif field_type == "object":
                    fields[field_name] = (Dict[str, Any], field_default)
                else:
                    fields[field_name] = (Any, field_default)
                
                # If the field is nullable, make the type Optional
                if field_nullable and field_name not in required_fields:
                    current_type = fields[field_name][0]
                    fields[field_name] = (Optional[current_type], field_default)
            except Exception as e:
                logger.warning(f"Error defining field '{field_name}': {str(e)}. Using Any type.")
                fields[field_name] = (Any, field_default)
        
        # Create the Pydantic model class dynamically
        try:
            model_class = create_model(
                schema_name,
                **fields,
                __doc__=schema_def.get("description", f"Model for {schema_name}")
            )
            
            logger.info(f"Successfully created Pydantic model class for '{schema_name}'")
            return model_class
        except Exception as e:
            logger.error(f"Error creating Pydantic model for '{schema_name}': {str(e)}")
            return None
    
    except Exception as e:
        logger.error(f"Error getting Pydantic model class for '{schema_name}': {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return None
    finally:
        pass

# Removed duplicate functions - now using centralized MCPIntegration module
# The MCPIntegration.create_mcp_tools_for_task function handles both
# explicit MCP servers from task config and global MCP servers


async def create_task(
    task_key: str, 
    task_config: dict, 
    agent: Agent, 
    tools: List[Any] = None,
    output_dir: Optional[str] = None, 
    config: dict = None,
    tool_service = None,
    tool_factory = None
) -> Task:
    """
    Creates a Task instance from the provided configuration.
    
    Args:
        task_key: The unique identifier for the task
        task_config: Dictionary containing task configuration
        agent: The agent that will perform this task
        tools: Optional list of tools to make available for this task specifically
        output_dir: Optional directory for output files
        config: Global configuration dictionary containing API keys
        tool_service: Optional tool service for resolving tool IDs to names
        tool_factory: Optional tool factory for creating tool instances
        
    Returns:
        Task: A configured CrewAI Task instance
    """
    logger.info(f"Creating task: {task_key}")
    logger.info(f"Task config keys: {list(task_config.keys()) if isinstance(task_config, dict) else 'not a dict'}")
    logger.info(f"Task tool_configs: {task_config.get('tool_configs', {}) if isinstance(task_config, dict) else 'N/A'}")
    
    # Log agent information
    agent_name = getattr(agent, "_agent_key", getattr(agent, "name", "unknown"))
    agent_role = getattr(agent, "role", "unknown")
    logger.info(f"Task {task_key} will be performed by agent {agent_name} with role '{agent_role}'")
    
    # Handle tool resolution if tool_service is provided and task has tool_ids
    task_tools = tools if tools else []
    
    # Use centralized MCP integration module for task MCP tools
    try:
        from src.core.unit_of_work import UnitOfWork
        from src.services.mcp_service import MCPService
        from src.engines.crewai.tools.mcp_integration import MCPIntegration
        
        async with UnitOfWork() as uow:
            mcp_service = await MCPService.from_unit_of_work(uow)
            # This will automatically handle global + explicit servers
            mcp_tools = await MCPIntegration.create_mcp_tools_for_task(
                task_config, task_key, mcp_service
            )
            task_tools.extend(mcp_tools)
            logger.info(f"Added {len(mcp_tools)} MCP tools to task {task_key}")
    except Exception as e:
        logger.error(f"Error processing MCP servers for task {task_key}: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    
    # Continue with normal tool resolution
    # Check for tools in task_config or in a backup field (for when MCP clears the tools array)
    tools_to_resolve = task_config.get('tools', []) or task_config.get('_original_tools', [])
    if tool_service and tools_to_resolve:
        logger.info(f"Resolving tool IDs for task {task_key}: {tools_to_resolve}")
        try:
            # Resolve tool IDs to names
            tool_names = await resolve_tool_ids_to_names(tools_to_resolve, tool_service)
            logger.info(f"Resolved tool names for task {task_key}: {tool_names}")
            
            # Create actual tool instances using the tool factory if available
            if tool_factory:
                for tool_name in tool_names:
                    if not tool_name:
                        continue
                        
                    try:
                        # Get the tool configuration if available
                        tool_config = {}
                        if hasattr(tool_service, 'get_tool_config_by_name'):
                            tool_config = await tool_service.get_tool_config_by_name(tool_name) or {}
                        
                        # Get task-specific tool config overrides
                        task_tool_configs = task_config.get('tool_configs', {})
                        tool_override = task_tool_configs.get(tool_name, {})
                        
                        # Debug logging for GenieTool spaceId
                        if tool_name == "GenieTool":
                            logger.info(f"Task {task_key} - GenieTool task_tool_configs: {task_tool_configs}")
                            logger.info(f"Task {task_key} - GenieTool tool_override: {tool_override}")
                        
                        # Create the tool instance with overrides
                        tool_instance = tool_factory.create_tool(
                            tool_name, 
                            result_as_answer=tool_config.get('result_as_answer', False),
                            tool_config_override=tool_override
                        )
                        if tool_instance:
                            # Check if this is a special MCP tool that returns a tuple with (is_mcp, tools_list)
                            if isinstance(tool_instance, tuple) and len(tool_instance) == 2 and tool_instance[0] is True:
                                # This is an MCP tool - Add all the individual tools from the list
                                mcp_tools = tool_instance[1]
                                
                                # Special case for mcp_service_adapter - async fetch from service
                                if mcp_tools == 'mcp_service_adapter':
                                    # Skip this case since we've removed the service adapter
                                    logger.info(f"MCP service adapter requested but not supported anymore")
                                    continue
                                elif isinstance(mcp_tools, list):
                                    # Regular MCP tools list
                                    for mcp_tool in mcp_tools:
                                        task_tools.append(mcp_tool)
                                    logger.info(f"Added {len(mcp_tools)} MCP tools from {tool_name} to task {task_key}")
                                else:
                                    logger.warning(f"Unexpected MCP tools format: {mcp_tools}")
                            else:
                                # Regular tool
                                task_tools.append(tool_instance)
                                # Add more debugging information about the tool
                                tool_info = {
                                    "name": getattr(tool_instance, "name", "unknown"),
                                    "type": type(tool_instance).__name__,
                                    "has_func": hasattr(tool_instance, "__call__"),
                                    "result_as_answer": tool_config.get('result_as_answer', False)
                                }
                                logger.info(f"Added tool instance {tool_name} to task {task_key} with details: {tool_info}")
                        else:
                            logger.warning(f"Could not create tool instance for {tool_name}")
                    except Exception as e:
                        logger.error(f"Error creating tool {tool_name}: {str(e)}")
            else:
                # Without tool_factory, just append the tool names (this won't work for CrewAI)
                task_tools.extend([name for name in tool_names if name])
                logger.warning("No tool_factory provided, using tool names which may not work with CrewAI")
        except Exception as e:
            logger.error(f"Error resolving tool IDs for task {task_key}: {str(e)}")
    
    logger.info(f"This is the tools: {task_tools}")
    # Log tool information if provided
    if task_tools:
        logger.info(f"Task {task_key} has {len(task_tools)} specific tools assigned:")
        for tool in task_tools:
            tool_name = getattr(tool, "name", str(tool)) if not isinstance(tool, str) else tool
            tool_desc = getattr(tool, "description", "No description") if not isinstance(tool, str) else "String tool name"
            desc_str = str(tool_desc)[:50] if tool_desc else "No description"
            logger.info(f"  - Task tool: {tool_name} - {desc_str}...")
    else:
        logger.info(f"Task {task_key} will use agent's default tools")
    
    # Prepare task arguments
    task_args = {
        "description": task_config["description"],
        "expected_output": task_config["expected_output"],
        "tools": task_tools,
        "agent": agent,
        "async_execution": task_config.get("async_execution", False),
        "retry_on_fail": task_config.get("retry_on_fail", False),
        "max_retries": task_config.get("max_retries", 3),
        "markdown": task_config.get("markdown", False)
    }
    
    # If markdown is enabled, append markdown instructions
    if task_args["markdown"]:
        task_args["description"] += "\n\nPlease format your response using markdown syntax."
        task_args["expected_output"] += "\n\nYour response should be formatted in markdown."

    # Store any existing callback from the task_config for later use
    existing_callback = task_config.get('callback', None)
    
    # Handle guardrail if present in the task configuration
    if 'guardrail' in task_config and task_config['guardrail']:
        guardrail_config = task_config['guardrail']
        guardrail_logger.info(f"Task {task_key} has guardrail configuration: {guardrail_config}")
        
        try:
            # Import guardrail factory only when needed
            from src.engines.crewai.guardrails.guardrail_factory import GuardrailFactory
            
            # Convert guardrail config to JSON string if it's a dictionary
            if isinstance(guardrail_config, dict):
                guardrail_config = json.dumps(guardrail_config)
            
            # Create the guardrail instance
            guardrail = GuardrailFactory.create_guardrail(guardrail_config)
            if guardrail:
                # Create a validation callback for this guardrail that returns tuple format
                # compatible with CrewAI's native guardrail mechanism
                def validate_with_guardrail(output, guardrail=guardrail):
                    """Validate task output with the specified guardrail"""
                    # Direct file writing for debugging
                    import datetime
                    import os
                    import traceback
                    
                    # Get log directory from the logger manager instead of hardcoding
                    logger_manager = LoggerManager.get_instance()
                    log_dir = logger_manager._log_dir
                    os.makedirs(log_dir, exist_ok=True)
                    
                    # Write to debug log
                    with open(os.path.join(log_dir, "guardrail_debug.log"), "a") as f:
                        f.write(f"\n\n{'='*50}\n")
                        f.write(f"VALIDATION CALLBACK CALLED at {datetime.datetime.now().isoformat()}\n")
                        f.write(f"Task: {task_key}\n")
                        f.write(f"Output type: {type(output)}\n")
                        f.write(f"Output: {str(output)[:1000]}\n")
                        f.write(f"{'='*50}\n")
                    
                    guardrail_logger.info("=" * 80)
                    guardrail_logger.info(f"VALIDATING TASK {task_key} OUTPUT WITH GUARDRAIL")
                    guardrail_logger.info("=" * 80)
                    guardrail_logger.info(f"Task output type: {type(output)}")
                    guardrail_logger.info(f"Task output: {output}")
                    
                    # Call the guardrail's validate method
                    try:
                        result = guardrail.validate(output)
                        
                        if result.get("valid", False):
                            guardrail_logger.info(f"Task {task_key} output passed guardrail validation")
                            guardrail_logger.info(f"Validation result: {result}")
                            # Direct file writing for debugging
                            with open(os.path.join(log_dir, "guardrail_debug.log"), "a") as f:
                                f.write(f"Validation PASSED\n")
                            
                            # Return a tuple indicating success (True, output)
                            return (True, output)
                        else:
                            # If validation fails, return a tuple for CrewAI guardrail mechanism
                            feedback = result.get("feedback", "Output does not meet requirements. Please try again.")
                            guardrail_logger.warning(f"Task {task_key} output failed guardrail validation")
                            guardrail_logger.warning(f"Validation feedback: {feedback}")
                            guardrail_logger.info(f"Full validation result: {result}")
                            # Direct file writing for debugging
                            with open(os.path.join(log_dir, "guardrail_debug.log"), "a") as f:
                                f.write(f"Validation FAILED: {feedback}\n")
                            
                            # Return a tuple indicating failure (False, error_message)
                            return (False, feedback)
                    except Exception as e:
                        guardrail_logger.error(f"Exception during guardrail validation: {str(e)}")
                        guardrail_logger.error(f"Stack trace: {traceback.format_exc()}")
                        # Direct file writing for debugging
                        with open(os.path.join(log_dir, "guardrail_debug.log"), "a") as f:
                            f.write(f"Validation ERROR: {str(e)}\n")
                        
                        # Return a tuple indicating failure (False, error_message)
                        return (False, f"Validation error: {str(e)}")
                
                # Instead of setting as callback, set as guardrail function
                # This integrates with CrewAI's native retry mechanism
                task_args['guardrail'] = validate_with_guardrail
                
                # Make sure retry_on_fail is set to True
                if 'retry_on_fail' not in task_config:
                    task_args['retry_on_fail'] = True
                
                guardrail_logger.info(f"Added guardrail validation to task {task_key}")
            else:
                guardrail_logger.warning(f"Failed to create guardrail for task {task_key}, guardrail will not be applied")
                # If guardrail creation failed but we have an existing callback, use it
                if existing_callback:
                    task_args['callback'] = existing_callback
        except Exception as e:
            guardrail_logger.error(f"Error setting up guardrail for task {task_key}: {str(e)}")
            guardrail_logger.error(f"Stack trace: {traceback.format_exc()}")
            # If guardrail setup failed but we have an existing callback, use it
            if existing_callback:
                task_args['callback'] = existing_callback
    elif existing_callback:
        # No guardrail, but we have an existing callback
        task_args['callback'] = existing_callback

    # Add output file for tasks that need it
    if output_dir and task_config.get('output_file_enabled', False):
        # Use task_key as filename by default, or specified filename
        filename = task_config.get('output_filename', f"{task_key}.md")
        file_path = str(Path(output_dir) / filename)
        logger.info(f"Using output path: {file_path}")
        task_args['output_file'] = file_path
        
        # Create directory for output file if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    # Check if there's an output_file in the task configuration
    elif 'output_file' in task_config and task_config['output_file']:
        output_file = task_config['output_file']
        logger.info(f"Using output file from task config: {output_file}")
        
        # Create directory for output file if it doesn't exist
        output_dir = os.path.dirname(output_file)
        if output_dir:
            try:
                os.makedirs(output_dir, exist_ok=True)
                logger.info(f"Created directory for output file: {output_dir}")
            except Exception as e:
                logger.warning(f"Failed to create directory for output file: {e}")
        
        task_args['output_file'] = output_file

    # Special handling for output_pydantic - look up the model in the database
    if 'output_pydantic' in task_config and task_config['output_pydantic']:
        output_pydantic_name = task_config['output_pydantic']
        logger.info(f"Task {task_key} has output_pydantic: {output_pydantic_name}")
        
        # Look up the Pydantic model class
        pydantic_class = await get_pydantic_class_from_name(output_pydantic_name)
        if pydantic_class:
            logger.info(f"Using Pydantic model class {pydantic_class.__name__} for output_pydantic")
            
            # Import the model conversion handler
            from src.engines.crewai.helpers.model_conversion_handler import (
                get_compatible_converter_for_model,
                configure_output_json_approach
            )
            
            # Get compatible converter and settings for this model
            converter_cls, pydantic_cls, use_output_json, is_compatible = get_compatible_converter_for_model(
                agent, pydantic_class
            )
            
            if is_compatible and use_output_json:
                # Use the output_json approach for compatibility
                task_args = configure_output_json_approach(task_args, pydantic_class)
                logger.info(f"Using output_json approach for compatibility with {agent.llm.model}")
            elif is_compatible and converter_cls:
                # Use the custom converter class
                task_args['converter_cls'] = converter_cls
                task_args['output_pydantic'] = pydantic_cls
                converter_name = getattr(converter_cls, '__name__', str(converter_cls))
                logger.info(f"Using custom converter {converter_name} for compatibility")
            else:
                # Standard approach
                task_args['output_pydantic'] = pydantic_class
        else:
            logger.warning(f"Could not resolve Pydantic model '{output_pydantic_name}', removing output_pydantic from task arguments")
            # Do not include output_pydantic if we can't get a proper model class
            # Including a string value would cause a validation error as CrewAI expects a Pydantic model class
    
    # Handle other optional task configurations
    optional_fields = [
        'async_execution',
        'context',
        'human_input',
        'converter_cls',
        'output_json'
    ]
    
    # Include optional fields if they exist in the config
    for field in optional_fields:
        if field in task_config:
            # Special case for output_json that might be a string
            if field == 'output_json' and isinstance(task_config[field], str):
                # Skip string values for output_json as CrewAI expects a BaseModel class
                # The string handling was for legacy compatibility but doesn't work with current CrewAI
                continue
            else:
                task_args[field] = task_config[field]
    
    # Create the task instance
    try:
        # Create the task with properly separated parameters
        task = Task(**task_args)
        
        logger.info(f"Successfully created task: {task_key}")
        
        # Debug the task to see if tools are properly attached
        task_debug_info = {
            "name": getattr(task, "name", "unknown"),
            "has_tools": hasattr(task, "tools"),
            "tools_length": len(getattr(task, "tools", [])),
            "tools_types": [type(t).__name__ for t in getattr(task, "tools", [])],
            "agent": getattr(task, "agent", None) and getattr(task.agent, "name", "unknown")
        }
        logger.info(f"Task debug info: {task_debug_info}")
        
        return task
    except Exception as e:
        logger.error(f"Error creating task: {e}")
        # Log the task_args for debugging
        logger.error(f"Task args: {task_args}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        raise 