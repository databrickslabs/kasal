"""
Service for crew generation operations.

This module provides business logic for generating crew setups
using LLM models to convert natural language descriptions into
structured CrewAI configurations.
"""

import json
import logging
import os
import traceback
import uuid
from typing import Dict, Any, List, Tuple, Optional


import litellm

from src.utils.prompt_utils import robust_json_parser
from src.services.template_service import TemplateService
from src.services.tool_service import ToolService
from src.services.documentation_embedding_service import DocumentationEmbeddingService
from src.schemas.crew import CrewGenerationRequest, CrewGenerationResponse
from src.services.log_service import LLMLogService
from src.core.llm_manager import LLMManager
from src.models.agent import Agent
from src.models.task import Task
from src.repositories.crew_generator_repository import CrewGeneratorRepository
from src.core.unit_of_work import UnitOfWork

# Configure logging
logger = logging.getLogger(__name__)

class CrewGenerationService:
    """Service for crew generation operations."""
    
    def __init__(self, log_service: LLMLogService):
        """
        Initialize the service.
        
        Args:
            log_service: Service for logging LLM interactions
        """
        self.log_service = log_service
        self.tool_service = None  # Will be initialized when needed
        # Initialize the crew generator repository directly
        self.crew_generator_repository = CrewGeneratorRepository()
        logger.info("Initialized CrewGeneratorRepository during service creation")
    
    @classmethod
    def create(cls) -> 'CrewGenerationService':
        """
        Factory method to create a properly configured instance of the service.
        
        This method abstracts the creation of dependencies while maintaining
        proper separation of concerns.
        
        Returns:
            An instance of CrewGenerationService with all required dependencies
        """
        log_service = LLMLogService.create()
        return cls(log_service=log_service)
    
    async def _log_llm_interaction(self, endpoint: str, prompt: str, response: str, model: str, 
                                  status: str = 'success', error_message: str = None) -> None:
        """
        Log LLM interaction using the log service.
        
        Args:
            endpoint: API endpoint that was called
            prompt: Input prompt text
            response: Response from the LLM
            model: Model used for generation
            status: Status of the interaction ('success' or 'error')
            error_message: Error message if status is 'error'
        """
        try:
            await self.log_service.create_log(
                endpoint=endpoint,
                prompt=prompt,
                response=response,
                model=model,
                status=status,
                error_message=error_message
            )
            logger.info(f"Logged {endpoint} interaction to database")
        except Exception as e:
            logger.error(f"Failed to log LLM interaction: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def _prepare_prompt_template(self, tools: List[Dict[str, Any]]) -> str:
        """
        Prepare the prompt template with tools context including tool descriptions.
        
        Args:
            tools: List of tool dictionaries, each containing name, description, parameters, etc.
            
        Returns:
            str: Complete system message with tools context
            
        Raises:
            ValueError: If prompt template is not found
        """
        # Get prompt template from database using the TemplateService
        system_message = await TemplateService.get_template_content("generate_crew")
        
        if not system_message:
            raise ValueError("Required prompt template 'generate_crew' not found in database")
        
        # Build tools context for the prompt with detailed descriptions
        tools_context = ""
        if tools:
            tools_context = "\n\nAvailable tools:\n"
            for tool in tools:
                # Add full tool details including name, description, and parameters
                name = tool.get('name', 'Unknown Tool')
                description = tool.get('description', 'No description available')
                parameters = tool.get('parameters', {})
                
                tools_context += f"- {name}: {description}\n"
                
                # Add parameter details if available
                if parameters:
                    tools_context += "  Parameters:\n"
                    for param_name, param_details in parameters.items():
                        param_desc = param_details.get('description', 'No description')
                        param_type = param_details.get('type', 'any')
                        tools_context += f"    - {param_name} ({param_type}): {param_desc}\n"
            
            tools_context += "\n\nEnsure that agents and tasks only use tools from this list. Assign tools to agents based on their capabilities and the tools' functionalities."
            
            # Add specific usage example for the NL2SQLTool if it's in the tools list
            if any(tool.get('name') == 'NL2SQLTool' for tool in tools):
                tools_context += "\n\nFor NL2SQLTool, use the following format for input: {'sql_query': <your_query>}"
        
        # Add tools context to the system message
        return system_message + tools_context
    
    def _process_crew_setup(self, setup: Dict[str, Any], allowed_tools: List[Dict[str, Any]], tool_name_to_id_map: Dict[str, str]) -> Dict[str, Any]:
        """
        Process and validate crew setup.
        
        Args:
            setup: Raw crew setup from LLM
            allowed_tools: List of allowed tools with descriptions
            tool_name_to_id_map: Mapping from tool names to their IDs
            
        Returns:
            Processed crew setup
            
        Raises:
            ValueError: If setup is invalid
        """
        # Extract just the tool names for filtering
        allowed_tool_names = [t.get('name') for t in allowed_tools if t.get('name')]
        
        # Log the raw setup from LLM
        agent_names = [a.get('name', 'Unknown') for a in setup.get('agents', [])]
        task_names = [t.get('name', 'Unknown') for t in setup.get('tasks', [])]
        logger.info(f"PROCESSING: LLM crew setup with {len(setup.get('agents', []))} agents and {len(setup.get('tasks', []))} tasks")
        logger.info(f"Agent names: {agent_names}")
        logger.info(f"Task names: {task_names}")
        
        # Log agent assignments from LLM
        for task in setup.get('tasks', []):
            task_name = task.get('name', 'Unknown')
            agent_name = task.get('agent')
            if not agent_name:
                agent_name = task.get('assigned_agent')
                
            if agent_name:
                logger.info(f"RAW LLM OUTPUT: Task '{task_name}' assigned to agent '{agent_name}'")
                # IMPORTANT: Make sure assignments are preserved by explicitly setting both fields
                task['agent'] = agent_name  # Ensure 'agent' field exists
                if 'assigned_agent' not in task:
                    task['assigned_agent'] = agent_name  # Also set assigned_agent as fallback
            else:
                logger.warning(f"RAW LLM OUTPUT: Task '{task_name}' has no agent assignment in LLM output")
        
        # Validate required fields
        if "agents" not in setup or not isinstance(setup["agents"], list) or len(setup["agents"]) == 0:
            logger.error("Missing or empty 'agents' array in LLM response")
            raise ValueError("Missing or empty 'agents' array in response")
            
        if "tasks" not in setup or not isinstance(setup["tasks"], list) or len(setup["tasks"]) == 0:
            logger.error("Missing or empty 'tasks' array in LLM response")
            raise ValueError("Missing or empty 'tasks' array in response")
            
        # Validate agent fields
        for i, agent in enumerate(setup["agents"]):
            agent_name = agent.get('name', f'Agent_{i}')
            logger.info(f"VALIDATING: Agent '{agent_name}'")
            
            required_agent_fields = ["name", "role", "goal", "backstory"]
            for field in required_agent_fields:
                if field not in agent:
                    logger.error(f"Agent '{agent_name}' is missing required field: {field}")
                    raise ValueError(f"Missing required field '{field}' in agent {i}")
        
        # Filter agent tools to only include allowed tools and convert tool names to IDs
        for agent in setup['agents']:
            agent_name = agent.get('name', 'Unknown')
            
            if 'tools' in agent and isinstance(agent['tools'], list):
                original_tools = agent['tools'].copy()
                
                # First filter tools to include only allowed ones
                filtered_tools = [tool for tool in agent['tools'] if tool in allowed_tool_names]
                
                if len(filtered_tools) != len(original_tools):
                    removed_tools = [tool for tool in original_tools if tool not in allowed_tool_names]
                    logger.info(f"TOOLS: Removed tools from agent '{agent_name}': {removed_tools}")
                    logger.info(f"TOOLS: Remaining tools for agent '{agent_name}': {filtered_tools}")
                
                # Convert tool names to IDs
                tool_ids = []
                for tool_name in filtered_tools:
                    if tool_name in tool_name_to_id_map:
                        tool_ids.append(tool_name_to_id_map[tool_name])
                    else:
                        logger.warning(f"Could not find ID for tool: {tool_name}")
                        # Keep the name as is if ID not found
                        tool_ids.append(tool_name)
                
                agent['tools'] = tool_ids
                logger.info(f"TOOLS: Converted tool names to IDs for agent '{agent_name}': {agent['tools']}")
                
            # Remove any existing ID to let the database generate it
            if 'id' in agent:
                logger.info(f"PROCESSING: Removing existing ID from agent '{agent_name}': {agent['id']}")
                del agent['id']
            
            # Ensure tools is a list
            if not isinstance(agent.get('tools'), list):
                logger.info(f"PROCESSING: Initializing empty tools list for agent '{agent_name}'")
                agent['tools'] = []
        
        # Filter task tools to only include allowed tools and convert to IDs
        for task in setup['tasks']:
            task_name = task.get('name', 'Unknown')
            
            # Debug log task fields
            logger.info(f"TASK FIELDS: Task '{task_name}' has fields: {list(task.keys())}")
            
            # Process Tools (existing logic)
            if 'tools' in task and isinstance(task['tools'], list):
                original_tools = task['tools'].copy()
                filtered_tools = [tool for tool in task['tools'] if tool in allowed_tool_names]
                
                # Convert tool names to IDs
                tool_ids = []
                for tool_name in filtered_tools:
                    if tool_name in tool_name_to_id_map:
                        tool_ids.append(tool_name_to_id_map[tool_name])
                    else:
                        logger.warning(f"Could not find ID for tool: {tool_name}")
                        # Keep the name as is if ID not found
                        tool_ids.append(tool_name)
                
                task['tools'] = tool_ids
                
                if len(filtered_tools) != len(original_tools):
                    removed_tools = [tool for tool in original_tools if tool not in allowed_tool_names]
                    logger.info(f"TOOLS: Removed tools from task '{task_name}': {removed_tools}")
                logger.info(f"TOOLS: Converted tool names to IDs for task '{task_name}': {task['tools']}")
            
            if not isinstance(task.get('tools'), list):
                 task['tools'] = [] # Ensure tools is a list
                 
            # Remove any existing ID to let the database generate it
            if 'id' in task:
                logger.info(f"PROCESSING: Removing existing ID from task '{task_name}': {task['id']}")
                del task['id']
                
            # --- Start: Process Context/Dependencies ---
            raw_context = task.get('context')
            if isinstance(raw_context, list) and len(raw_context) > 0:
                # Assume context from LLM contains dependency names/refs
                # Store these raw refs temporarily for the repository to resolve later
                task['_context_refs'] = raw_context
                logger.info(f"PROCESSING: Stored {len(raw_context)} context refs for task '{task_name}': {raw_context}")
            else:
                # Ensure _context_refs doesn't exist if context is empty/invalid
                if '_context_refs' in task:
                    del task['_context_refs']
                    
            # Explicitly set the main context field to an empty list for initial creation
            # The repository will populate this later using _context_refs
            task['context'] = []
            logger.info(f"PROCESSING: Initialized empty context list for task '{task_name}' (refs stored separately)")
            # --- End: Process Context/Dependencies ---
                
            # Log agent assignment for this task AGAIN to ensure it's preserved
            agent_name = task.get('agent')
            if not agent_name:
                agent_name = task.get('assigned_agent')
                
            if agent_name:
                logger.info(f"FINAL LLM STRUCTURE: Task '{task_name}' will be assigned to agent '{agent_name}'")
                # Double-check both fields are set
                task['agent'] = agent_name
                task['assigned_agent'] = agent_name
            else:
                logger.warning(f"FINAL LLM STRUCTURE: Task '{task_name}' has no agent assignment")
        
        logger.info("PROCESSING: Finished processing crew setup")
        return setup
        
    
            
    def _safe_get_attr(self, obj, attr, default=None):
        """
        Safely get an attribute from an object, whether it's a dictionary or an object.
        
        Args:
            obj: The object or dictionary to get the attribute from
            attr: The attribute name to get
            default: The default value to return if the attribute is not found
            
        Returns:
            The attribute value or default
        """
        if hasattr(obj, 'get') and callable(obj.get):
            # Dictionary-like access
            return obj.get(attr, default)
        elif hasattr(obj, attr):
            # Object attribute access
            return getattr(obj, attr, default)
        else:
            return default

    async def _get_relevant_documentation(self, user_prompt: str, limit: int = 3) -> str:
        """
        Retrieve relevant documentation embeddings based on the user's prompt.
        
        Args:
            user_prompt: The user's prompt
            limit: Maximum number of documentation chunks to retrieve
            
        Returns:
            String containing relevant documentation formatted for context
        """
        try:
            # Create embedding for the user's prompt
            logger.info("Creating embedding for user prompt to find relevant documentation")
            
            # Initialize the LLM manager
            llm_manager = LLMManager()
            
            # Get the embedding for the user prompt
            # Using OpenAI's embedding service for consistency with documentation embeddings
            embedding_response = await llm_manager.get_embedding(user_prompt)
            if not embedding_response:
                logger.warning("Failed to create embedding for user prompt")
                return ""
                
            # Get the embedding vector
            query_embedding = embedding_response
            
            # Initialize the documentation embedding service
            doc_service = DocumentationEmbeddingService()
            
            # Retrieve similar documentation based on the embedding
            async with UnitOfWork() as uow:
                logger.info(f"Searching for {limit} most relevant documentation chunks")
                # Access the session directly from the internal _session attribute
                session = await uow._session.__aenter__()
                similar_docs = await doc_service.search_similar_embeddings(
                    query_embedding=query_embedding,
                    limit=limit,
                    db=session
                )
                
                if not similar_docs or len(similar_docs) == 0:
                    logger.warning("No relevant documentation found")
                    return ""
                
                # Format the documentation for context
                docs_context = "\n\n## CrewAI Relevant Documentation\n\n"
                
                for i, doc in enumerate(similar_docs):
                    source = doc.source.split('/')[-1].capitalize() if doc.source else "Unknown"
                    docs_context += f"### {source} - {doc.title}\n\n"
                    docs_context += f"{doc.content}\n\n"
                    
                logger.info(f"Retrieved {len(similar_docs)} relevant documentation chunks")
                return docs_context
                
        except Exception as e:
            logger.error(f"Error retrieving documentation: {str(e)}")
            logger.error(traceback.format_exc())
            return ""

    async def create_crew_complete(self, request: CrewGenerationRequest) -> Dict[str, Any]:
        """
        Create a crew with agents and tasks.
        
        Args:
            request: The crew generation request with prompt, model, and tool information
        
        Returns:
            Dictionary containing the created agents and tasks
        """
        try:
            logger.info("CREATE CREW: Starting crew generation process")
            
            # Get tool details using the Unit of Work pattern to access the tool service
            async with UnitOfWork() as uow:
                # Create tool service from UnitOfWork
                tool_service = await ToolService.from_unit_of_work(uow)
                # Process tools to ensure we have complete tool information
                tools_with_details = await self._get_tool_details(request.tools or [], tool_service)
                
                # Create a mapping from tool names to tool IDs for later use
                tool_name_to_id_map = self._create_tool_name_to_id_map(tools_with_details)
                logger.info(f"Tool name to ID mapping: {tool_name_to_id_map}")
                
                # Generate the crew using the LLM
                model = request.model or os.getenv("CREW_MODEL", "databricks-llama-4-maverick")
                
                # Get and prepare the prompt template with tool descriptions
                system_message = await self._prepare_prompt_template(tools_with_details)
                logger.info("CREATE CREW: Prepared prompt template with detailed tool information")
                
                # Get relevant documentation based on the user's prompt
                documentation_context = await self._get_relevant_documentation(request.prompt)
                
                # Prepare messages for the LLM
                messages = [
                    {"role": "system", "content": system_message}
                ]
                
                # Add documentation context if available
                if documentation_context:
                    messages.append({
                        "role": "system", 
                        "content": "Here is some relevant documentation about CrewAI that may help you generate a better crew:\n\n" + documentation_context
                    })
                    logger.info("Added relevant documentation to enhance context")
                
                # Add the user's prompt
                messages.append({"role": "user", "content": request.prompt})
                
                # Configure litellm using the LLMManager
                model_params = await LLMManager.configure_litellm(model)
                logger.info(f"CREATE CREW: Configured LiteLLM with model: {model}")
                
                # Generate completion with litellm
                try:
                    logger.info("CREATE CREW: Calling LLM API...")
                    response = await litellm.acompletion(
                        **model_params,
                        messages=messages,
                        temperature=0.7,
                        max_tokens=4000
                    )
                    
                    # Extract and parse the content
                    content = response["choices"][0]["message"]["content"]
                    logger.info(f"CREATE CREW: Extracted content from LLM response (length: {len(content)})")
                    
                    # Log the LLM interaction
                    await self._log_llm_interaction(
                        endpoint='generate-crew',
                        prompt=f"System: {system_message}\nDocumentation: {documentation_context}\nUser: {request.prompt}",
                        response=content,
                        model=model
                    )
                    
                    # Parse JSON setup
                    logger.info("CREATE CREW: Parsing JSON response from LLM")
                    crew_setup = robust_json_parser(content)
                    logger.info(f"CREATE CREW: Successfully parsed JSON")
                    
                    # Process and validate LLM response with the tool name to ID mapping
                    processed_setup = self._process_crew_setup(crew_setup, tools_with_details, tool_name_to_id_map)
                    
                except Exception as e:
                    error_msg = f"Error generating crew: {str(e)}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                
                # Log agent assignments before converting to dictionaries
                logger.info("CREATE CREW: Current agent assignments:")
                for task in processed_setup.get('tasks', []):
                    task_name = task.get('name', 'Unknown')
                    agent_name = task.get('agent')
                    if not agent_name:
                        agent_name = task.get('assigned_agent')
                    
                    if agent_name:
                        logger.info(f"ASSIGNMENTS: Task '{task_name}' assigned to agent '{agent_name}'")
                    else:
                        logger.warning(f"ASSIGNMENTS: Task '{task_name}' HAS NO AGENT ASSIGNMENT")
                
                # Convert Pydantic models to dictionaries while preserving agent assignments
                agents_dict = []
                for agent in processed_setup.get('agents', []):
                    # If it's a Pydantic model, convert to dict
                    if hasattr(agent, 'model_dump'):
                        agent_dict = agent.model_dump()
                    else:
                        agent_dict = agent.copy() if isinstance(agent, dict) else agent
                    
                    agents_dict.append(agent_dict)
                
                tasks_dict = []
                for task in processed_setup.get('tasks', []):
                    # If it's a Pydantic model, convert to dict
                    if hasattr(task, 'model_dump'):
                        task_dict = task.model_dump()
                    else:
                        task_dict = task.copy() if isinstance(task, dict) else task
                    
                    # IMPORTANT: Ensure agent assignments are preserved
                    task_name = task_dict.get('name', 'Unknown')
                    agent_name = task.get('agent')
                    if not agent_name:
                        agent_name = task.get('assigned_agent')
                    
                    if agent_name:
                        # Make sure both fields are set in the dictionary
                        task_dict['agent'] = agent_name
                        task_dict['assigned_agent'] = agent_name
                        logger.info(f"PRESERVE: Task '{task_name}' assignment to agent '{agent_name}' preserved in dictionary conversion")
                    else:
                        logger.warning(f"PRESERVE: Task '{task_name}' HAS NO AGENT ASSIGNMENT to preserve")
                    
                    tasks_dict.append(task_dict)
                
                # Create a new dictionary to send to repository
                crew_dict = {
                    'agents': agents_dict,
                    'tasks': tasks_dict
                }
                
                # Log the data being sent to repository
                logger.info(f"CREATE CREW: Sending {len(agents_dict)} agents and {len(tasks_dict)} tasks to repository")
                for idx, agent in enumerate(agents_dict):
                    logger.info(f"AGENT {idx+1}: '{agent.get('name')}' - Role: '{agent.get('role')}', Tools: {agent.get('tools', [])}")
                
                for idx, task in enumerate(tasks_dict):
                    logger.info(f"TASK {idx+1}: '{task.get('name')}' - Agent: '{task.get('agent')}', Dependencies: {task.get('context', [])}")
                
                # Create entities in repository
                result = await self.crew_generator_repository.create_crew_entities(crew_dict)
                
                logger.info("CREATE CREW: Successfully created crew entities")
                return result
        except Exception as e:
            logger.error(f"CREATE CREW: Error creating crew: {str(e)}")
            logger.error(f"CREATE CREW: Exception traceback: {traceback.format_exc()}")
            raise

    def _create_tool_name_to_id_map(self, tools: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Create a mapping from tool names to tool IDs.
        
        Args:
            tools: List of tool dictionaries
            
        Returns:
            Dict mapping tool names to their IDs
        """
        name_to_id = {}
        for tool in tools:
            # Use title as name if available
            name = tool.get('title') or tool.get('name')
            tool_id = tool.get('id')
            
            if name and tool_id:
                # Ensure ID is a string
                name_to_id[name] = str(tool_id)
                
                # Also add the original name as a key if different from title
                if 'name' in tool and tool['name'] != name:
                    name_to_id[tool['name']] = str(tool_id)
        
        return name_to_id
            
    async def _get_tool_details(self, tool_identifiers: List[Any], tool_service: ToolService) -> List[Dict[str, Any]]:
        """
        Get detailed information about tools from the tool service.
        
        This handles different possible input formats:
        - List of strings (tool names or IDs)
        - List of dictionaries with at least 'name' or 'id' fields
        
        Args:
            tool_identifiers: List of tool identifiers in any supported format
            tool_service: ToolService instance to use for retrieving tool details
            
        Returns:
            List of dictionaries with complete tool details
        """
        detailed_tools = []
        
        try:
            # Get all available tools using the provided service
            tools_response = await tool_service.get_all_tools()
            all_tools = tools_response.tools
            logger.info(f"Retrieved {len(all_tools)} tools from tool service")
            
            # Create lookup maps for faster tool retrieval
            tools_by_name = {tool.title: tool for tool in all_tools if hasattr(tool, 'title')}
            tools_by_id = {str(tool.id): tool for tool in all_tools if hasattr(tool, 'id')}
            
            # Process each tool identifier
            for identifier in tool_identifiers:
                tool_detail = None
                
                if isinstance(identifier, str):
                    # Check if it's a name or ID
                    if identifier in tools_by_name:
                        tool_detail = tools_by_name[identifier]
                    elif identifier in tools_by_id:
                        tool_detail = tools_by_id[identifier]
                    else:
                        logger.warning(f"Tool not found: {identifier}")
                        # Add a placeholder with just the name
                        detailed_tools.append({"name": identifier, "description": f"A tool named {identifier}", "id": identifier})
                        continue
                        
                elif isinstance(identifier, dict):
                    # Extract name or ID from dictionary
                    name = identifier.get('name')
                    tool_id = identifier.get('id')
                    
                    if name and name in tools_by_name:
                        tool_detail = tools_by_name[name]
                    elif tool_id and str(tool_id) in tools_by_id:
                        tool_detail = tools_by_id[str(tool_id)]
                    elif name:
                        # If we have a name but no match, add it as is
                        logger.warning(f"Tool not found by name: {name}")
                        detailed_tools.append({
                            "name": name,
                            "description": identifier.get('description', f"A tool named {name}"),
                            "id": tool_id or name  # Use ID if available, otherwise use name
                        })
                        continue
                    else:
                        logger.warning(f"Invalid tool identifier, missing name or id: {identifier}")
                        continue
                else:
                    logger.warning(f"Unknown tool identifier format: {identifier}")
                    continue
                
                # Convert tool to dictionary with all details
                if tool_detail:
                    if hasattr(tool_detail, 'model_dump'):
                        tool_dict = tool_detail.model_dump()
                    else:
                        # If it's already a dictionary or has __dict__
                        tool_dict = tool_detail.__dict__ if hasattr(tool_detail, '__dict__') else dict(tool_detail)
                    
                    # Ensure we have name and description
                    if 'name' not in tool_dict and hasattr(tool_detail, 'title'):
                        tool_dict['name'] = tool_detail.title
                    if 'description' not in tool_dict and hasattr(tool_detail, 'description'):
                        tool_dict['description'] = tool_detail.description
                    
                    detailed_tools.append(tool_dict)
            
            logger.info(f"Processed {len(detailed_tools)} tools with detailed information")
            return detailed_tools
            
        except Exception as e:
            logger.error(f"Error retrieving tool details: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Fall back to basic processing if tool service fails
            return [{"name": t if isinstance(t, str) else t.get('name', 'Unknown'), 
                    "description": f"A tool named {t if isinstance(t, str) else t.get('name', 'Unknown')}",
                    "id": t if isinstance(t, str) else t.get('id', t.get('name', 'Unknown'))} 
                   for t in tool_identifiers] 