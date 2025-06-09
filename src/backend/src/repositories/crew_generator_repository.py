"""
Repository for crew generation operations.

This module provides functions for creating agents and tasks in the database,
managing transactions for the crew generation process.
"""

import logging
import traceback
import json
from typing import List, Tuple, Dict, Any, Optional
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.agent import Agent
from src.models.task import Task
from src.repositories.agent_repository import AgentRepository
from src.repositories.task_repository import TaskRepository
from src.schemas.agent import AgentCreate
from src.schemas.task import TaskCreate
from src.db.session import async_session_factory

# Configure logging
logger = logging.getLogger(__name__)

class CrewGeneratorRepository:
    """Repository for creating agents and tasks for a generated crew."""
    
    @classmethod
    def create(cls):
        """
        Factory method to create a properly configured instance of the repository.
        
        Returns:
            An instance of CrewGeneratorRepository
        """
        return cls()
    
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
        if isinstance(obj, dict):
            # Dictionary access
            return obj.get(attr, default)
        elif hasattr(obj, attr):
            # Object attribute access
            return getattr(obj, attr, default)
        elif hasattr(obj, 'get') and callable(obj.get):
            # Dictionary-like object access
            return obj.get(attr, default)
        else:
            return default
            
    async def create(self, entity):
        """
        Create an entity in the database.
        
        Args:
            entity: The entity to create (Agent or Task)
            
        Returns:
            The created entity
        """
        async with async_session_factory() as session:
            try:
                session.add(entity)
                await session.commit()
                await session.refresh(entity)
                return entity
            except Exception as e:
                await session.rollback()
                logger.error(f"Error creating entity: {e}")
                logger.error(traceback.format_exc())
                raise
    
    async def update(self, entity_id, update_data):
        """
        Update an entity in the database.
        
        Args:
            entity_id: The ID of the entity to update
            update_data: The data to update
            
        Returns:
            The updated entity
        """
        async with async_session_factory() as session:
            try:
                # Determine if it's a Task update by checking context field
                if "context" in update_data:
                    # This is a Task update - need to pass both the model class and session
                    task_repo = TaskRepository(Task, session)
                    # Get existing task
                    task = await task_repo.get(entity_id)
                    if task:
                        # Update context (dependencies)
                        task.context = update_data["context"]
                        await session.commit()
                        await session.refresh(task)
                        return task
                    else:
                        logger.error(f"Task with ID {entity_id} not found for update")
                else:
                    # For other entities
                    logger.error(f"Update for entity type not implemented")
                
                return None
            except Exception as e:
                await session.rollback()
                logger.error(f"Error updating entity: {e}")
                logger.error(traceback.format_exc())
                raise
            
    async def create_crew_entities(self, crew_dict):
        """
        Create agents and tasks for a crew.
        
        This is a complete workflow that handles:
        1. Creating agents in the database
        2. Creating a mapping of agent names to their database IDs
        3. Creating tasks with proper agent_id assignments based on the agent names
        4. Updating task dependencies
        
        Args:
            crew_dict: Dictionary containing 'agents' and 'tasks' lists
            
        Returns:
            Dictionary with created 'agents' and 'tasks' in serializable format
        """
        # Extract agents and tasks data from the dictionary
        agents_data = crew_dict.get('agents', [])
        tasks_data = crew_dict.get('tasks', [])
        
        logger.info(f"Creating crew with {len(agents_data)} agents and {len(tasks_data)} tasks")
        
        # Step 1: Create all agents first to get their IDs
        created_agents = await self._create_agents(agents_data)
        
        # Step 2: Create a mapping of agent names to their database IDs
        agent_name_to_id = {}
        for agent in created_agents:
            agent_name_to_id[agent.name] = agent.id
            logger.info(f"AGENT MAPPING: '{agent.name}' -> ID: {agent.id}")
        
        # Step 3: Create all tasks with proper agent_id assignments
        created_tasks = await self._create_tasks(tasks_data, agent_name_to_id)
        
        # Step 4: Update task dependencies
        await self._create_task_dependencies(created_tasks, tasks_data)
        
        # Convert SQLAlchemy models to serializable dictionaries
        serialized_agents = []
        for agent in created_agents:
            agent_dict = {
                'id': agent.id,
                'name': agent.name,
                'role': agent.role,
                'goal': agent.goal,
                'backstory': agent.backstory,
                'llm': agent.llm,
                'tools': agent.tools,
                'allow_delegation': agent.allow_delegation,
                'verbose': agent.verbose,
                'max_iter': agent.max_iter,
                'max_rpm': agent.max_rpm,
                'cache': agent.cache,
                'allow_code_execution': agent.allow_code_execution,
                'code_execution_mode': agent.code_execution_mode,
                'max_retry_limit': agent.max_retry_limit,
                'use_system_prompt': agent.use_system_prompt,
                'respect_context_window': agent.respect_context_window,
                'function_calling_llm': agent.function_calling_llm,
                'created_at': agent.created_at.isoformat() if agent.created_at else None,
                'updated_at': agent.updated_at.isoformat() if agent.updated_at else None
            }
            serialized_agents.append(agent_dict)
            
        serialized_tasks = []
        for task in created_tasks:
            task_dict = {
                'id': task.id,
                'name': task.name,
                'description': task.description,
                'agent_id': task.agent_id,
                'expected_output': task.expected_output,
                'tools': task.tools,
                'async_execution': task.async_execution,
                'context': task.context,
                'output': task.output,
                'human_input': task.human_input,
                'created_at': task.created_at.isoformat() if task.created_at else None,
                'updated_at': task.updated_at.isoformat() if task.updated_at else None
            }
            serialized_tasks.append(task_dict)
        
        # Return both created agents and tasks in a dictionary format with serializable objects
        return {
            'agents': serialized_agents,
            'tasks': serialized_tasks
        }

    async def _create_agents(self, agents_data):
        """
        Create agents in the database.
        
        Args:
            agents_data: List of agent data dictionaries
            
        Returns:
            List of created Agent models
        """
        logger.info(f"Creating {len(agents_data)} agents in database")
        created_agents = []
        
        for agent_data in agents_data:
            # Log the agent data for debugging
            logger.info(f"Creating agent: {self._safe_get_attr(agent_data, 'name')}")
            
            # Create the agent
            agent = Agent(
                id=str(uuid.uuid4()),
                name=self._safe_get_attr(agent_data, 'name'),
                role=self._safe_get_attr(agent_data, 'role'),
                goal=self._safe_get_attr(agent_data, 'goal'),
                backstory=self._safe_get_attr(agent_data, 'backstory'),
                llm=self._safe_get_attr(agent_data, 'llm'),
                tools=self._safe_get_attr(agent_data, 'tools', []),
                allow_delegation=self._safe_get_attr(agent_data, 'allow_delegation', False),
                verbose=self._safe_get_attr(agent_data, 'verbose', False),
                max_iter=self._safe_get_attr(agent_data, 'max_iter', 25),
                max_rpm=self._safe_get_attr(agent_data, 'max_rpm', 10),
                cache=self._safe_get_attr(agent_data, 'cache', True),
                allow_code_execution=self._safe_get_attr(agent_data, 'allow_code_execution', False),
                code_execution_mode=self._safe_get_attr(agent_data, 'code_execution_mode', 'safe'),
                max_retry_limit=self._safe_get_attr(agent_data, 'max_retry_limit', 2),
                use_system_prompt=self._safe_get_attr(agent_data, 'use_system_prompt', True),
                respect_context_window=self._safe_get_attr(agent_data, 'respect_context_window', True),
                function_calling_llm=self._safe_get_attr(agent_data, 'function_calling_llm')
            )
            
            # Store the agent in the database
            await self.create(agent)
            logger.info(f"Agent created: {agent.name} (ID: {agent.id})")
            created_agents.append(agent)
            
        return created_agents

    async def _create_tasks(self, tasks_data, agent_name_to_id):
        """
        Create tasks in the database.
        
        Args:
            tasks_data: List of task data dictionaries
            agent_name_to_id: Dictionary mapping agent names to IDs
            
        Returns:
            List of created Task models
        """
        logger.info(f"Creating {len(tasks_data)} tasks in the database")
        logger.info(f"Agent name to ID mapping: {json.dumps(agent_name_to_id)}")
        
        created_tasks = []
        
        # Variable to distribute tasks in round-robin fashion if no agent specified
        round_robin_idx = 0
        agent_ids = list(agent_name_to_id.values())
        
        for i, task_data in enumerate(tasks_data):
            task_name = self._safe_get_attr(task_data, 'name', f'Unknown Task {i}')
            logger.info(f"Processing task {i+1}: '{task_name}'")
            
            # Check for agent name in either field
            agent_name = self._safe_get_attr(task_data, 'agent')
            
            if not agent_name:
                agent_name = self._safe_get_attr(task_data, 'assigned_agent')
                
            # Log the agent assignment from LLM
            if agent_name:
                logger.info(f"LLM assigned task '{task_name}' to agent '{agent_name}'")
            else:
                logger.warning(f"TASK {i+1}: '{task_name}' HAS NO AGENT ASSIGNMENT")
                
            # Look up the agent's database ID using the name
            agent_id = None
            best_match = False
            
            # Try exact match first
            if agent_name and agent_name in agent_name_to_id:
                agent_id = agent_name_to_id[agent_name]
                logger.info(f"Found exact agent ID match for '{agent_name}': {agent_id}")
            # Try case-insensitive match if exact match fails
            elif agent_name:
                # Try to find a case-insensitive match
                for known_agent_name in agent_name_to_id:
                    if agent_name.lower() == known_agent_name.lower():
                        agent_id = agent_name_to_id[known_agent_name]
                        logger.info(f"Found case-insensitive match for '{agent_name}' -> '{known_agent_name}': {agent_id}")
                        break
                
                # If still no match, try partial match using a scoring system
                if not agent_id:
                    best_match = True
                    best_match_score = 0
                    best_match_name = None
                    
                    for known_agent_name in agent_name_to_id:
                        # Calculate similarity score 
                        score = 0
                        
                        # Check if one is substring of the other (higher weight for this)
                        if agent_name.lower() in known_agent_name.lower():
                            score += 5
                        if known_agent_name.lower() in agent_name.lower():
                            score += 4
                            
                        # Check for common words
                        agent_words = agent_name.lower().split()
                        known_words = known_agent_name.lower().split()
                        common_words = set(agent_words).intersection(set(known_words))
                        score += len(common_words) * 3
                        
                        # Update best match if we found a better score
                        if score > best_match_score:
                            best_match_score = score
                            best_match_name = known_agent_name
                    
                    # Only consider it a match if the score is above threshold
                    if best_match_score > 2 and best_match_name:
                        agent_id = agent_name_to_id[best_match_name]
                        logger.info(f"Found best match for '{agent_name}' -> '{best_match_name}' with score {best_match_score}: {agent_id}")
                    else:
                        best_match = False
                        logger.warning(f"No good match found for '{agent_name}'. Using round-robin assignment.")
                
                # If still no match, log the issue
                if not agent_id:
                    logger.warning(f"Could not find agent ID for '{agent_name}'. Agent name not in database.")
                    logger.info(f"Available agents: {list(agent_name_to_id.keys())}")
            
            # If no agent assigned or found, use round-robin assignment
            if not agent_id and agent_ids:
                if round_robin_idx >= len(agent_ids):
                    round_robin_idx = 0
                agent_id = agent_ids[round_robin_idx]
                round_robin_idx += 1
                logger.info(f"Assigned task '{task_name}' to agent ID {agent_id} using round-robin")
            elif not agent_id:
                logger.warning(f"No agent assigned to task '{task_name}' and no agents available")
            
            # Create the task with the correct agent_id
            task = Task(
                id=str(uuid.uuid4()),
                name=task_name,
                description=self._safe_get_attr(task_data, 'description'),
                expected_output=self._safe_get_attr(task_data, 'expected_output'),
                tools=self._safe_get_attr(task_data, 'tools', []),
                agent_id=agent_id,  # Set the agent_id based on the lookup or round-robin
                async_execution=self._safe_get_attr(task_data, 'async_execution', False),
                output=self._safe_get_attr(task_data, 'output'),
                human_input=self._safe_get_attr(task_data, 'human_input', False),
                markdown=self._safe_get_attr(task_data, 'markdown', False)
            )
            
            # Store the task in the database
            await self.create(task)
            
            # Log the task creation result
            if agent_id:
                if best_match:
                    logger.info(f"Task created: '{task.name}' (ID: {task.id}) assigned to agent ID: {agent_id} using best match")
                else:
                    logger.info(f"Task created: '{task.name}' (ID: {task.id}) assigned to agent ID: {agent_id}")
            else:
                logger.warning(f"Task created: '{task.name}' (ID: {task.id}) with NO agent assignment")
                
            created_tasks.append(task)
            
        return created_tasks
        
    async def _create_task_dependencies(self, created_tasks, tasks_data):
        """
        Create task dependencies in the database using the _context_refs field.
        
        Args:
            created_tasks: List of created Task models from the database
            tasks_data: List of the original task data dictionaries from the service,
                        potentially containing a '_context_refs' list.
        """
        logger.info("Creating task dependencies in the database")
        
        # Create maps for easy lookup
        task_name_to_db_task = {task.name: task for task in created_tasks}
        task_id_to_db_task = {task.id: task for task in created_tasks}
        
        logger.info(f"Task name map created with {len(task_name_to_db_task)} entries.")

        async with async_session_factory() as session:
            # Pass both the Task model and the session to the repository
            task_repo = TaskRepository(Task, session)
            tasks_to_update = []

            for task_data in tasks_data:
                task_name = self._safe_get_attr(task_data, 'name', '')
                
                # Find the corresponding database task object
                db_task = task_name_to_db_task.get(task_name)
                if not db_task:
                    logger.warning(f"Could not find database task for name '{task_name}' when processing dependencies.")
                    continue
                    
                logger.info(f"Processing dependencies for task '{task_name}' (ID: {db_task.id})")
                
                # Get the raw context references stored by the service
                context_refs = self._safe_get_attr(task_data, '_context_refs', [])
                
                if context_refs and isinstance(context_refs, list):
                    logger.info(f"Task '{task_name}' has {len(context_refs)} raw context refs: {json.dumps(context_refs)}")
                    
                    resolved_dependency_ids = []
                    
                    for ref in context_refs:
                        # References could be names or potentially other identifiers
                        # Assuming they are names for now based on typical LLM output
                        ref_name = str(ref) # Ensure it's a string
                        logger.info(f"Looking up dependency ref '{ref_name}' for task '{task_name}'")
                        
                        # Resolve the reference name to a database task ID
                        dependency_task = task_name_to_db_task.get(ref_name)
                        if dependency_task:
                            dependency_id = dependency_task.id
                            # Ensure we don't add self-dependency (shouldn't happen ideally)
                            if dependency_id != db_task.id:
                                resolved_dependency_ids.append(dependency_id)
                                logger.info(f"Resolved dependency: Task '{task_name}' depends on '{ref_name}' (ID: {dependency_id})")
                            else:
                                logger.warning(f"Skipping self-dependency for task '{task_name}' from ref '{ref_name}'")
                        else:
                            logger.warning(f"Could not resolve context ref '{ref_name}' for task '{task_name}' - task name not found in created tasks.")
                    
                    # Update task object if dependencies were resolved
                    if resolved_dependency_ids:
                        # Ensure no duplicates
                        unique_dependency_ids = list(set(resolved_dependency_ids))
                        if len(unique_dependency_ids) != len(resolved_dependency_ids):
                             logger.info(f"Removed duplicate dependency IDs for task '{task_name}'")
                             
                        logger.info(f"Updating task '{task_name}' (ID: {db_task.id}) context in DB with {len(unique_dependency_ids)} resolved dependency IDs: {unique_dependency_ids}")
                        # Prepare update data
                        db_task.context = unique_dependency_ids # Update the ORM object directly
                        tasks_to_update.append(db_task) # Add to list for bulk update/commit
                        
                    else:
                        logger.warning(f"Task '{task_name}' had context refs but none could be resolved to valid task IDs.")
                else:
                     logger.info(f"Task '{task_name}' has no context refs.")
                     # Ensure context is an empty list if no refs were provided or resolved
                     if db_task.context is None or db_task.context != []:
                          db_task.context = []
                          tasks_to_update.append(db_task)
                          logger.info(f"Ensured context is empty for task '{task_name}' (ID: {db_task.id})")

            # Commit all updates at once
            if tasks_to_update:
                try:
                    logger.info(f"Committing context updates for {len(tasks_to_update)} tasks.")
                    session.add_all(tasks_to_update)
                    await session.commit()
                    logger.info("Successfully committed task dependency updates.")
                except Exception as e:
                    await session.rollback()
                    logger.error(f"Error committing task dependency updates: {e}")
                    logger.error(traceback.format_exc())
                    # Potentially re-raise or handle as needed
                    raise
            else:
                logger.info("No task context updates needed.")
                
        logger.info("Finished processing task dependencies") 