# LLM Manager

## Overview

The LLM Manager (`LLMManager`) is a core component that provides centralized management for configuring and interacting with different Language Model (LLM) providers through the LiteLLM interface. It serves as a unified configuration point for all LLM-related operations in the application.

## Location

```
backend/src/core/llm_manager.py
```

## Configuration Approaches

The application uses two different approaches for LLM configuration:

1. **LiteLLM Configuration**: Uses `LLMManager.configure_litellm()` - This configuration is used for general text generation and completion tasks that utilize LiteLLM directly.

2. **CrewAI Configuration**: Uses `LLMManager.configure_crewai_llm()` - This method configures a CrewAI LLM instance with the proper provider prefixes and settings required by CrewAI.

## Provider Prefixing for CrewAI

For CrewAI integration, model names must be properly prefixed with the provider name for certain providers. The `configure_crewai_llm` method handles this automatically:

- **DeepSeek**: Uses format `deepseek/model-name`
- **Anthropic**: Uses format `anthropic/model-name`
- **Ollama**: Uses format `ollama/model-name`
- **Databricks**: Uses format `databricks/model-name`
- **OpenAI**: No prefix needed, uses format `model-name`

```python
# Example of CrewAI LLM configuration
crewai_llm = await LLMManager.configure_crewai_llm("deepseek-chat")

# Applies the agent's crew with the properly configured LLM
for agent in crew.agents:
    agent.llm = crewai_llm
```

## Key Features

- Centralized LLM configuration management
- Provider-specific configuration handling
- API key management integration
- Support for multiple LLM providers (OpenAI, Anthropic, DeepSeek, Ollama, Databricks)
- Specialized configuration for different use cases (LiteLLM vs CrewAI)
- Proper model name formatting with provider prefixes

## Methods

### `configure_litellm`

```python
@staticmethod
async def configure_litellm(model: str) -> Dict[str, Any]
```

Primary configuration method used by services that interact directly with LiteLLM. This method:
- Retrieves model configuration
- Sets up provider-specific parameters
- Handles API key management
- Configures provider-specific endpoints

**Used by:**
- Agent Generation Service
- Task Generation Service
- Connection Service
- Crew Generation Service

### `configure_crewai_llm`

```python
@staticmethod
async def configure_crewai_llm(model_name: str) -> LLM
```

Specialized configuration method for CrewAI integration. This method:
- Creates and returns a properly configured CrewAI LLM instance
- Handles provider-specific model name formatting with prefixes
- Configures provider-specific endpoints
- Manages API keys
- Returns a ready-to-use CrewAI LLM object

**Used by:**
- Crew Execution Runner

## Usage by Services

### LiteLLM Services

The following services use the standard `configure_litellm` method:

1. **Agent Generation Service**
   - Purpose: Configures LLMs for agent creation and configuration
   - Usage: During agent initialization and task processing

2. **Task Generation Service**
   - Purpose: Sets up LLMs for task generation and management
   - Usage: When creating and processing tasks

3. **Connection Service**
   - Purpose: Configures LLMs for handling service connections
   - Usage: During connection establishment and management

4. **Crew Generation Service**
   - Purpose: Sets up LLMs for crew creation and configuration
   - Usage: When initializing and managing crews

### CrewAI Execution

The Crew Execution Runner uses the specialized `configure_crewai_llm` method:

- Purpose: Creates a properly configured CrewAI LLM instance
- Usage: Before running crew operations
- Special Requirements: Needs provider-prefixed model names for compatibility with LiteLLM

## Supported Providers

The LLM Manager supports the following providers:

1. **OpenAI**
   - Models: GPT-4, GPT-3.5-Turbo, etc.
   - Configuration: API key based
   - CrewAI Format: `model-name` (no prefix needed)

2. **Anthropic**
   - Models: Claude series
   - Configuration: API key based
   - CrewAI Format: `anthropic/model-name`

3. **DeepSeek**
   - Models: DeepSeek Chat, DeepSeek Reasoner
   - Configuration: API key and custom endpoint
   - CrewAI Format: `deepseek/model-name`

4. **Ollama**
   - Models: Llama, Mistral, etc.
   - Configuration: Local endpoint configuration
   - CrewAI Format: `ollama/model-name`

5. **Databricks**
   - Models: Databricks-hosted models
   - Configuration: Token-based authentication and custom endpoint
   - CrewAI Format: `databricks/model-name`

## Best Practices

1. **Service Layer Usage**
   - Always use the appropriate configuration method for your use case
   - For direct LiteLLM usage, use `configure_litellm`
   - For CrewAI integration, use `configure_crewai_llm`

2. **Error Handling**
   - Always handle configuration errors appropriately
   - Implement retry mechanisms for transient failures
   - Log configuration issues for debugging

3. **API Key Management**
   - Never hardcode API keys
   - Use the `ApiKeysService` for key management
   - Handle missing API key scenarios gracefully

4. **Provider-Specific Configuration**
   - Be aware of provider-specific requirements
   - Use appropriate environment variables for endpoints
   - Follow provider-specific naming conventions

## Example Usage

```python
# For direct LiteLLM services
model_params = await LLMManager.configure_litellm("gpt-4")

# For CrewAI integration
crewai_llm = await LLMManager.configure_crewai_llm("deepseek-chat")
agent.llm = crewai_llm
```

## Troubleshooting

Common issues and their solutions:

1. **Provider Not Configured**
   - Ensure the provider is properly specified in the model configuration
   - Check if the provider is supported

2. **API Key Issues**
   - Verify API key is set in the environment
   - Check API key permissions
   - Ensure API key is properly configured in the service

3. **Endpoint Configuration**
   - Verify endpoint URLs are correctly set
   - Check environment variables for custom endpoints
   - Ensure network access to endpoints

4. **LLM Provider NOT provided Error**
   - This error typically occurs when using CrewAI with improperly formatted model names
   - Ensure you're using `configure_crewai_llm` for CrewAI integration
   - Verify the model name includes the proper provider prefix
   - The format should be `provider/model-name` for most providers (except OpenAI)

## Future Considerations

- Support for additional LLM providers
- Enhanced configuration options
- Performance optimization
- Caching mechanisms
- Rate limiting implementation
- Additional CrewAI-specific configuration options

## Handling the "LLM Provider NOT provided" Error

This is a common error when using CrewAI with LiteLLM. Our solution:

1. **Root Cause**: CrewAI's integration with LiteLLM requires model names to include provider prefixes.

2. **Solution**: The `configure_crewai_llm` method automatically formats model names with the correct provider prefix.

3. **Implementation**: 
   - We use CrewAI's native `LLM` class
   - We format model names with provider prefixes (e.g., `deepseek/deepseek-chat`)
   - We directly apply the configured LLM to each agent in the crew

This approach ensures proper integration between CrewAI and LiteLLM by handling the provider-specific formatting requirements in a centralized way. 