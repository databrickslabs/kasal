"""
Service for dispatching natural language requests to appropriate generation services.

This module provides business logic for analyzing user messages and determining
whether they want to generate an agent, task, or crew, then calling the appropriate service.
"""

import logging
import os
import re
from typing import Dict, Any, Optional, List, Set
import litellm

from src.schemas.dispatcher import DispatcherRequest, DispatcherResponse, IntentType
from src.schemas.task_generation import TaskGenerationRequest, TaskGenerationResponse
from src.schemas.crew import CrewGenerationRequest, CrewGenerationResponse
from src.services.agent_generation_service import AgentGenerationService
from src.services.task_generation_service import TaskGenerationService
from src.services.crew_generation_service import CrewGenerationService
from src.services.template_service import TemplateService
from src.services.log_service import LLMLogService
from src.core.llm_manager import LLMManager
from src.utils.prompt_utils import robust_json_parser
from src.utils.user_context import GroupContext

# Configure logging
logger = logging.getLogger(__name__)

# Default model for intent detection
DEFAULT_DISPATCHER_MODEL = os.getenv("DEFAULT_DISPATCHER_MODEL", "databricks-llama-4-maverick")


class DispatcherService:
    """Service for dispatching natural language requests to generation services."""
    
    # Task-related action words that indicate the user wants to create a task
    TASK_ACTION_WORDS = {
        'find', 'search', 'locate', 'discover', 'identify', 'get', 'fetch', 'retrieve',
        'analyze', 'examine', 'study', 'investigate', 'review', 'assess', 'evaluate',
        'create', 'make', 'build', 'generate', 'produce', 'develop', 'construct',
        'write', 'compose', 'draft', 'prepare', 'document', 'record', 'note',
        'calculate', 'compute', 'determine', 'measure', 'count', 'sum', 'total',
        'compare', 'contrast', 'match', 'relate', 'connect', 'link', 'associate',
        'organize', 'sort', 'arrange', 'group', 'categorize', 'classify', 'order',
        'summarize', 'abstract', 'condense', 'outline', 'highlight', 'extract',
        'process', 'handle', 'manage', 'coordinate', 'execute', 'perform', 'run',
        'check', 'verify', 'validate', 'confirm', 'test', 'inspect', 'audit',
        'monitor', 'track', 'watch', 'observe', 'follow', 'supervise', 'oversee',
        'update', 'modify', 'change', 'edit', 'revise', 'adjust', 'alter',
        'send', 'deliver', 'transmit', 'forward', 'share', 'distribute', 'dispatch',
        'collect', 'gather', 'compile', 'accumulate', 'assemble', 'combine',
        'convert', 'transform', 'translate', 'adapt', 'format', 'parse', 'decode'
    }
    
    # Conversation indicators - words that suggest general conversation
    CONVERSATION_WORDS = {
        'hello', 'hi', 'hey', 'greetings', 'good', 'morning', 'afternoon', 'evening',
        'what', 'how', 'why', 'when', 'where', 'who', 'which', 'explain', 'tell',
        'show', 'status', 'help', 'assist', 'support', 'guidance', 'information',
        'thanks', 'thank', 'please', 'sorry', 'excuse', 'pardon'
    }
    
    # Agent-related keywords
    AGENT_KEYWORDS = {
        'agent', 'assistant', 'bot', 'robot', 'ai', 'helper', 'specialist', 
        'expert', 'analyst', 'advisor', 'consultant', 'operator', 'worker'
    }
    
    # Crew-related keywords  
    CREW_KEYWORDS = {
        'team', 'crew', 'group', 'squad', 'multiple', 'several', 'many',
        'workflow', 'pipeline', 'process', 'collaboration', 'together'
    }
    
    # Configuration-related keywords
    CONFIGURE_KEYWORDS = {
        'configure', 'config', 'setup', 'set', 'change', 'update', 'modify',
        'settings', 'preferences', 'options', 'parameters', 'llm', 'model',
        'maxr', 'max', 'rpm', 'rate', 'limit', 'tools', 'tool', 'select',
        'choose', 'pick', 'adjust', 'tune', 'customize', 'personalize'
    }
    
    def __init__(self, log_service: LLMLogService):
        """
        Initialize the service.
        
        Args:
            log_service: Service for logging LLM interactions
        """
        self.log_service = log_service
        self.agent_service = AgentGenerationService.create()
        self.task_service = TaskGenerationService.create()
        self.crew_service = CrewGenerationService.create()
    
    @classmethod
    def create(cls) -> 'DispatcherService':
        """
        Factory method to create a properly configured instance of the service.
        
        Returns:
            An instance of DispatcherService with all required dependencies
        """
        log_service = LLMLogService.create()
        return cls(log_service=log_service)
    
    async def _log_llm_interaction(self, endpoint: str, prompt: str, response: str, model: str, 
                                  status: str = 'success', error_message: Optional[str] = None,
                                  group_context: Optional[GroupContext] = None):
        """
        Log LLM interaction using the log service.
        
        Args:
            endpoint: API endpoint name
            prompt: Input prompt
            response: Model response
            model: LLM model used
            status: Status of the interaction (success/error)
            error_message: Optional error message
            group_context: Optional group context for multi-group isolation
        """
        try:
            await self.log_service.create_log(
                endpoint=endpoint,
                prompt=prompt,
                response=response,
                model=model,
                status=status,
                error_message=error_message,
                group_context=group_context
            )
            logger.info(f"Logged {endpoint} interaction to database")
        except Exception as e:
            logger.error(f"Failed to log LLM interaction: {str(e)}")
    
    def _analyze_message_semantics(self, message: str) -> Dict[str, Any]:
        """
        Perform semantic analysis on the message to extract intent hints.
        
        Args:
            message: User's natural language message
            
        Returns:
            Dictionary containing semantic analysis results
        """
        # Normalize message for analysis
        words = re.findall(r'\b\w+\b', message.lower())
        word_set = set(words)
        
        # Count different types of keywords
        task_actions = word_set.intersection(self.TASK_ACTION_WORDS)
        conversation_words = word_set.intersection(self.CONVERSATION_WORDS)
        agent_keywords = word_set.intersection(self.AGENT_KEYWORDS)
        crew_keywords = word_set.intersection(self.CREW_KEYWORDS)
        configure_keywords = word_set.intersection(self.CONFIGURE_KEYWORDS)
        
        # Analyze message structure patterns
        has_imperative = any(word in words[:3] for word in self.TASK_ACTION_WORDS)  # Action word in first 3 words
        has_question = message.strip().endswith('?') or any(word in words[:2] for word in ['what', 'how', 'why', 'when', 'where', 'who'])
        has_greeting = any(word in words[:3] for word in self.CONVERSATION_WORDS)
        
        # Detect command-like structures
        command_patterns = [
            r'^(find|get|create|make|build|search|analyze)',  # Starts with action
            r'^(i need|i want|help me|can you)',              # Request patterns
            r'^(an order|a task|a job)',                      # Task-like prefixes
        ]
        
        # Detect configuration patterns
        configure_patterns = [
            r'(configure|config|setup|set up)',               # Configuration words
            r'(change|update|modify|adjust).*?(llm|model|tools|maxr|max|rpm)', # Change configuration
            r'(select|choose|pick).*?(llm|model|tools)',       # Selection patterns
            r'(llm|model|tools|maxr).*?(setting|config)',      # Configuration contexts
        ]
        
        has_command_structure = any(re.search(pattern, message.lower()) for pattern in command_patterns)
        has_configure_structure = any(re.search(pattern, message.lower()) for pattern in configure_patterns)
        
        # Calculate intent suggestions based on semantic analysis
        intent_scores = {
            'generate_task': len(task_actions) * 2 + (1 if has_imperative else 0) + (1 if has_command_structure else 0),
            'generate_agent': len(agent_keywords) * 3,
            'generate_crew': len(crew_keywords) * 3,
            'configure_crew': len(configure_keywords) * 3 + (2 if has_configure_structure else 0),
            'conversation': len(conversation_words) * 2 + (1 if has_question else 0) + (1 if has_greeting else 0)
        }
        
        # Determine semantic hints
        semantic_hints = []
        if task_actions:
            semantic_hints.append(f"Action words detected: {', '.join(task_actions)}")
        if configure_keywords:
            semantic_hints.append(f"Configuration words detected: {', '.join(configure_keywords)}")
        if has_command_structure:
            semantic_hints.append("Command-like structure detected")
        if has_configure_structure:
            semantic_hints.append("Configuration structure detected")
        if has_imperative:
            semantic_hints.append("Imperative form detected")
        if has_question:
            semantic_hints.append("Question form detected")
        if has_greeting:
            semantic_hints.append("Conversational greeting detected")
            
        return {
            "task_actions": list(task_actions),
            "conversation_words": list(conversation_words),
            "agent_keywords": list(agent_keywords),
            "crew_keywords": list(crew_keywords),
            "configure_keywords": list(configure_keywords),
            "has_imperative": has_imperative,
            "has_question": has_question,
            "has_greeting": has_greeting,
            "has_command_structure": has_command_structure,
            "has_configure_structure": has_configure_structure,
            "intent_scores": intent_scores,
            "semantic_hints": semantic_hints,
            "suggested_intent": max(intent_scores, key=intent_scores.get) if max(intent_scores.values()) > 0 else "unknown"
        }
    
    async def _detect_intent(self, message: str, model: str) -> Dict[str, Any]:
        """
        Detect the intent from the user's message using LLM enhanced with semantic analysis.
        
        Args:
            message: User's natural language message
            model: LLM model to use
            
        Returns:
            Dictionary containing intent, confidence, and extracted information
        """
        # Perform semantic analysis first
        semantic_analysis = self._analyze_message_semantics(message)
        
        # Get prompt template from database
        system_prompt = await TemplateService.get_template_content("detect_intent")
        
        if not system_prompt:
            # Use a default prompt if template not found
            system_prompt = """You are an intelligent intent detection system for a CrewAI workflow designer.

Analyze the user's message and determine their intent from these categories:

1. **generate_task**: User wants to create a single task or action. Look for:
   - Action words: find, search, analyze, create, write, calculate, etc.
   - Task descriptions: "find the best flight", "analyze this data", "write a report"
   - Instructions that could be automated: "get information about X", "compare Y and Z"
   - Casual requests that imply a task: "an order find...", "I need to...", "help me..."
   - Commands or directives: "find me", "get the", "calculate", "determine"

2. **generate_agent**: User wants to create a single agent with specific capabilities:
   - Explicit mentions of "agent", "assistant", "bot"
   - Role-based requests: "create a financial analyst", "I need a data scientist"
   - Capability-focused: "something that can analyze data and write reports"

3. **generate_crew**: User wants to create multiple agents and/or tasks working together:
   - Multiple roles mentioned: "team of agents", "research and writing team"
   - Complex workflows: "research then write then review"
   - Collaborative language: "agents working together", "workflow with multiple steps"

4. **configure_crew**: User wants to configure workflow settings (LLM, max RPM, tools):
   - Configuration requests: "configure crew", "setup llm", "change model", "select tools"
   - Settings modifications: "update max rpm", "set llm model", "modify tools"
   - Preference adjustments: "choose different model", "adjust settings", "pick tools"
   - Direct mentions: "llm", "maxr", "max rpm", "tools", "config", "settings"

5. **conversation**: User is asking questions, seeking information, or having general conversation:
   - Questions about the system: "how does this work?", "what can you do?"
   - Greetings: "hello", "hi", "good morning"
   - General questions: "what is...", "explain...", "why..."
   - Status inquiries: "what's the status of...", "show me..."

6. **unknown**: Unclear or ambiguous messages that don't fit the above categories.

**Key Insight**: Many task requests are phrased conversationally. Look for ACTION WORDS and GOALS rather than formal task language.

Return a JSON object with:
{
    "intent": "generate_task" | "generate_agent" | "generate_crew" | "configure_crew" | "conversation" | "unknown",
    "confidence": 0.0-1.0,
    "extracted_info": {
        "action_words": ["list", "of", "detected", "action", "words"],
        "entities": ["extracted", "entities", "or", "objects"],
        "goal": "what the user wants to accomplish",
        "config_type": "llm|maxr|tools|general" // Only for configure_crew intent
    },
    "suggested_prompt": "Enhanced version optimized for the specific service"
}

Examples:
- "Create an agent that can analyze data" -> generate_agent
- "I need a task to summarize documents" -> generate_task
- "an order find the best flight between zurich and montreal" -> generate_task
- "find me the cheapest hotel in paris" -> generate_task
- "get information about the weather tomorrow" -> generate_task
- "analyze this sales data and create a report" -> generate_task
- "Build a team of agents to handle customer support" -> generate_crew
- "Create a research agent and a writer agent with tasks for each" -> generate_crew
- "configure crew" -> configure_crew
- "setup llm" -> configure_crew
- "change model" -> configure_crew
- "select tools" -> configure_crew
- "update max rpm" -> configure_crew
- "adjust settings" -> configure_crew
- "How does intent detection work?" -> conversation
- "Hello, what can you help me with?" -> conversation
- "Show me my recent tasks" -> conversation
"""
        
        # Enhance the user message with semantic analysis
        enhanced_user_message = f"""Message: {message}

Semantic Analysis:
- Detected action words: {', '.join(semantic_analysis['task_actions']) if semantic_analysis['task_actions'] else 'None'}
- Conversation indicators: {', '.join(semantic_analysis['conversation_words']) if semantic_analysis['conversation_words'] else 'None'}
- Agent keywords: {', '.join(semantic_analysis['agent_keywords']) if semantic_analysis['agent_keywords'] else 'None'}
- Crew keywords: {', '.join(semantic_analysis['crew_keywords']) if semantic_analysis['crew_keywords'] else 'None'}
- Configure keywords: {', '.join(semantic_analysis['configure_keywords']) if semantic_analysis['configure_keywords'] else 'None'}
- Has imperative form: {semantic_analysis['has_imperative']}
- Has question form: {semantic_analysis['has_question']}
- Has command structure: {semantic_analysis['has_command_structure']}
- Has configure structure: {semantic_analysis['has_configure_structure']}
- Semantic hints: {'; '.join(semantic_analysis['semantic_hints']) if semantic_analysis['semantic_hints'] else 'None'}
- Suggested intent from analysis: {semantic_analysis['suggested_intent']}

Please analyze this message and provide your intent classification, considering both the semantic analysis and the natural language content."""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": enhanced_user_message}
        ]
        
        try:
            # Configure litellm using the LLMManager
            model_params = await LLMManager.configure_litellm(model)
            
            # Generate completion
            response = await litellm.acompletion(
                **model_params,
                messages=messages,
                temperature=0.3,  # Lower temperature for more consistent intent detection
                max_tokens=1000
            )
            
            content = response["choices"][0]["message"]["content"]
            
            # Parse the response
            result = robust_json_parser(content)
            
            # Validate the response
            if "intent" not in result:
                result["intent"] = semantic_analysis["suggested_intent"]
            if "confidence" not in result:
                result["confidence"] = 0.5
            if "extracted_info" not in result:
                result["extracted_info"] = {}
            if "suggested_prompt" not in result:
                result["suggested_prompt"] = message
                
            # Enhance extracted_info with semantic analysis
            result["extracted_info"]["semantic_analysis"] = semantic_analysis
            
            # If LLM result seems wrong and semantic analysis is confident, use semantic analysis
            semantic_confidence = max(semantic_analysis["intent_scores"].values()) / 5.0  # Normalize to 0-1
            if semantic_confidence > 0.6 and result["confidence"] < 0.7:
                logger.info(f"Using semantic analysis suggestion: {semantic_analysis['suggested_intent']} (confidence: {semantic_confidence:.2f}) over LLM result: {result['intent']} (confidence: {result['confidence']:.2f})")
                result["intent"] = semantic_analysis["suggested_intent"]
                result["confidence"] = max(result["confidence"], semantic_confidence)
                
            return result
            
        except Exception as e:
            logger.error(f"Error detecting intent: {str(e)}")
            # Fall back to semantic analysis if LLM fails
            semantic_confidence = max(semantic_analysis["intent_scores"].values()) / 5.0  # Normalize to 0-1
            return {
                "intent": semantic_analysis["suggested_intent"] if semantic_confidence > 0.3 else "unknown",
                "confidence": max(0.3, semantic_confidence),
                "extracted_info": {"semantic_analysis": semantic_analysis},
                "suggested_prompt": message
            }
    
    async def dispatch(self, request: DispatcherRequest, group_context: GroupContext = None) -> Dict[str, Any]:
        """
        Dispatch the user's request to the appropriate generation service.
        
        Args:
            request: Dispatcher request with user message and options
            group_context: Group context from headers for multi-group isolation
            
        Returns:
            Dictionary containing the intent detection result and generation response
        """
        model = request.model or DEFAULT_DISPATCHER_MODEL
        
        # Detect intent
        intent_result = await self._detect_intent(request.message, model)
        
        # Log the intent detection
        await self._log_llm_interaction(
            endpoint='detect-intent',
            prompt=request.message,
            response=str(intent_result),
            model=model,
            group_context=group_context
        )
        
        # Create dispatcher response
        dispatcher_response = DispatcherResponse(
            intent=IntentType(intent_result["intent"]),
            confidence=intent_result["confidence"],
            extracted_info=intent_result["extracted_info"],
            suggested_prompt=intent_result["suggested_prompt"]
        )
        
        # Dispatch to appropriate service based on intent
        generation_result = None
        
        try:
            if dispatcher_response.intent == IntentType.GENERATE_AGENT:
                # Call agent generation service with tenant context
                generation_result = await self.agent_service.generate_agent(
                    prompt_text=dispatcher_response.suggested_prompt or request.message,
                    model=request.model,
                    tools=request.tools,
                    group_context=group_context
                )
                
            elif dispatcher_response.intent == IntentType.GENERATE_TASK:
                # Call task generation service (which handles both generation and saving)
                task_request = TaskGenerationRequest(
                    text=dispatcher_response.suggested_prompt or request.message,
                    model=request.model
                )
                generation_result = await self.task_service.generate_and_save_task(task_request, group_context)
                
            elif dispatcher_response.intent == IntentType.GENERATE_CREW:
                # Call crew generation service
                crew_request = CrewGenerationRequest(
                    prompt=dispatcher_response.suggested_prompt or request.message,
                    model=request.model,
                    tools=request.tools
                )
                generation_result = await self.crew_service.create_crew_complete(crew_request, group_context)
                
            elif dispatcher_response.intent == IntentType.CONFIGURE_CREW:
                # Handle configuration intent - determine what type of configuration is needed
                config_type = dispatcher_response.extracted_info.get("config_type", "general")
                
                generation_result = {
                    "type": "configure_crew",
                    "config_type": config_type,
                    "message": f"Opening configuration dialog for {config_type} settings.",
                    "actions": {
                        "open_llm_dialog": config_type in ["llm", "general"],
                        "open_maxr_dialog": config_type in ["maxr", "general"], 
                        "open_tools_dialog": config_type in ["tools", "general"]
                    },
                    "extracted_info": dispatcher_response.extracted_info
                }
                
            elif dispatcher_response.intent == IntentType.CONVERSATION:
                # Handle conversation intent - provide helpful response
                generation_result = {
                    "type": "conversation",
                    "message": "I understand you're having a conversation. If you'd like me to create a task, agent, or crew for you, please describe what you'd like me to build. For example: 'Create a task to find flights' or 'Build an agent that can analyze data'.",
                    "suggestions": [
                        "Create a task to accomplish a specific goal",
                        "Generate an agent with particular capabilities", 
                        "Build a crew of agents working together"
                    ]
                }
                
            else:
                # Unknown intent
                logger.warning(f"Unknown intent detected: {dispatcher_response.intent}")
                generation_result = {
                    "type": "unknown",
                    "message": "I'm not sure what you'd like me to create. Could you please clarify if you want me to generate a task, agent, or crew?",
                    "suggestions": [
                        "Create a task: 'I need a task to...'",
                        "Generate an agent: 'Create an agent that can...'",
                        "Build a crew: 'Build a team that can...'"
                    ]
                }
                
        except Exception as e:
            logger.error(f"Error in generation service: {str(e)}")
            await self._log_llm_interaction(
                endpoint=f'dispatch-{dispatcher_response.intent}',
                prompt=request.message,
                response=str(e),
                model=model,
                status='error',
                error_message=str(e),
                group_context=group_context
            )
            raise
        
        # Return combined response
        return {
            "dispatcher": dispatcher_response.model_dump(),
            "generation_result": generation_result,
            "service_called": dispatcher_response.intent.value if dispatcher_response.intent != IntentType.UNKNOWN else None
        } 