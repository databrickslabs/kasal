import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from typing import List, Any
import uuid

from src.engines.crewai.helpers.agent_helpers import process_knowledge_sources, create_agent


class TestProcessKnowledgeSources:
    """Test suite for process_knowledge_sources function."""
    
    def test_process_knowledge_sources_empty_list(self):
        """Test processing empty knowledge sources list."""
        result = process_knowledge_sources([])
        assert result == []
    
    def test_process_knowledge_sources_none(self):
        """Test processing None knowledge sources."""
        result = process_knowledge_sources(None)
        assert result is None
    
    def test_process_knowledge_sources_list_of_strings(self):
        """Test processing list of string paths."""
        sources = ["/path/to/file1.txt", "/path/to/file2.pdf", "/path/to/file3.docx"]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            assert result == sources
            mock_logger.info.assert_any_call(f"Processing knowledge sources: {sources}")
    
    def test_process_knowledge_sources_list_of_dicts_with_path(self):
        """Test processing list of dictionaries with 'path' key."""
        sources = [
            {"path": "/path/to/file1.txt", "type": "text"},
            {"path": "/path/to/file2.pdf", "type": "pdf"},
            {"path": "/path/to/file3.docx", "name": "document"}
        ]
        expected_paths = ["/path/to/file1.txt", "/path/to/file2.pdf", "/path/to/file3.docx"]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            assert result == expected_paths
            mock_logger.info.assert_any_call(f"Processing knowledge sources: {sources}")
            mock_logger.info.assert_any_call(f"Processed paths: {expected_paths}")
    
    def test_process_knowledge_sources_objects_with_path_attribute(self):
        """Test processing objects with 'path' attribute."""
        class MockSource:
            def __init__(self, path):
                self.path = path
                self.metadata = "test"
        
        sources = [
            MockSource("/path/to/file1.txt"),
            MockSource("/path/to/file2.pdf"),
            MockSource("/path/to/file3.docx")
        ]
        expected_paths = ["/path/to/file1.txt", "/path/to/file2.pdf", "/path/to/file3.docx"]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            assert result == expected_paths
            mock_logger.info.assert_any_call(f"Processed paths: {expected_paths}")
    
    def test_process_knowledge_sources_mixed_types(self):
        """Test processing mixed types of sources."""
        class MockSource:
            def __init__(self, path):
                self.path = path
        
        sources = [
            "/direct/path.txt",
            {"path": "/dict/path.pdf", "type": "pdf"},
            MockSource("/object/path.docx")
        ]
        expected_paths = ["/direct/path.txt", "/dict/path.pdf", "/object/path.docx"]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            assert result == expected_paths
            mock_logger.info.assert_any_call(f"Processed paths: {expected_paths}")
    
    def test_process_knowledge_sources_dict_without_path(self):
        """Test processing dictionary without 'path' key is ignored."""
        sources = [
            "/valid/path.txt",
            {"name": "file", "type": "text"},  # No 'path' key
            {"path": "/valid/path2.pdf"}
        ]
        expected_paths = ["/valid/path.txt", "/valid/path2.pdf"]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            assert result == expected_paths
    
    def test_process_knowledge_sources_object_without_path(self):
        """Test processing object without 'path' attribute is ignored."""
        class MockSourceWithoutPath:
            def __init__(self):
                self.name = "test"
        
        sources = [
            "/valid/path.txt",
            MockSourceWithoutPath(),  # No 'path' attribute
            {"path": "/valid/path2.pdf"}
        ]
        expected_paths = ["/valid/path.txt", "/valid/path2.pdf"]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            assert result == expected_paths
    
    def test_process_knowledge_sources_empty_path_in_dict(self):
        """Test processing dictionary with empty path."""
        sources = [
            "/valid/path.txt",
            {"path": "", "type": "text"},
            {"path": "/valid/path2.pdf"}
        ]
        expected_paths = ["/valid/path.txt", "", "/valid/path2.pdf"]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            assert result == expected_paths
    
    def test_process_knowledge_sources_none_path_in_dict(self):
        """Test processing dictionary with None path."""
        sources = [
            "/valid/path.txt",
            {"path": None, "type": "text"},
            {"path": "/valid/path2.pdf"}
        ]
        expected_paths = ["/valid/path.txt", None, "/valid/path2.pdf"]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            assert result == expected_paths
    
    def test_process_knowledge_sources_logging(self):
        """Test that appropriate logging occurs."""
        sources = ["/path1.txt", "/path2.pdf"]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            # Should log the initial sources
            mock_logger.info.assert_any_call(f"Processing knowledge sources: {sources}")
            # For list of strings, should not log processed paths (returns as is)
            assert result == sources
    
    def test_process_knowledge_sources_single_string(self):
        """Test processing single string (should still work as list)."""
        sources = ["/single/path.txt"]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            assert result == sources
            mock_logger.info.assert_any_call(f"Processing knowledge sources: {sources}")
    
    def test_process_knowledge_sources_complex_nested_dict(self):
        """Test processing complex nested dictionary structures."""
        sources = [
            {
                "path": "/complex/path.txt",
                "metadata": {
                    "author": "test",
                    "tags": ["tag1", "tag2"]
                },
                "permissions": ["read", "write"]
            }
        ]
        expected_paths = ["/complex/path.txt"]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            assert result == expected_paths
    
    def test_process_knowledge_sources_unicode_paths(self):
        """Test processing paths with unicode characters."""
        sources = [
            "/path/with/üñíçødé.txt",
            {"path": "/another/path/with/中文.pdf"},
            "/regular/path.docx"
        ]
        expected_paths = ["/path/with/üñíçødé.txt", "/another/path/with/中文.pdf", "/regular/path.docx"]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            assert result == expected_paths
    
    def test_process_knowledge_sources_windows_paths(self):
        """Test processing Windows-style paths."""
        sources = [
            "C:\\Users\\test\\file1.txt",
            {"path": "D:\\Documents\\file2.pdf"},
            "\\\\network\\share\\file3.docx"
        ]
        expected_paths = ["C:\\Users\\test\\file1.txt", "D:\\Documents\\file2.pdf", "\\\\network\\share\\file3.docx"]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            assert result == expected_paths


class TestCreateAgent:
    """Test suite for create_agent function."""
    
    @pytest.fixture
    def mock_agent_config(self):
        """Mock agent configuration"""
        return {
            "role": "Test Agent",
            "goal": "Test agent goal",
            "backstory": "Test agent backstory",
            "verbose": True,
            "allow_delegation": False,
            "tools": ["tool1", "tool2"],
            "llm": "gpt-4o",
            "knowledge_sources": ["/path/to/knowledge.txt"]
        }
    
    @pytest.fixture
    def mock_tools(self):
        """Mock tools list"""
        tool1 = MagicMock()
        tool1.name = "tool1"
        tool2 = MagicMock()
        tool2.name = "tool2"
        return [tool1, tool2]
    
    @pytest.fixture
    def mock_config(self):
        """Mock global config"""
        return {"api_keys": {"openai": "test_key"}}
    
    @pytest.mark.asyncio
    async def test_create_agent_basic_success(self, mock_agent_config, mock_tools, mock_config):
        """Test basic successful agent creation"""
        agent_key = "test_agent"
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork to prevent MCP service calls
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=mock_agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                assert result == mock_agent_instance
                mock_agent_class.assert_called_once()
                
                # Verify agent was created with correct parameters
                call_kwargs = mock_agent_class.call_args[1]
                assert call_kwargs["role"] == mock_agent_config["role"]
                assert call_kwargs["goal"] == mock_agent_config["goal"]
                assert call_kwargs["backstory"] == mock_agent_config["backstory"]
                assert call_kwargs["tools"] == mock_tools
                assert call_kwargs["llm"] == mock_llm
    
    @pytest.mark.asyncio
    async def test_create_agent_missing_required_field(self, mock_tools, mock_config):
        """Test agent creation with missing required field"""
        agent_key = "test_agent"
        incomplete_config = {
            "role": "Test Agent",
            "goal": "Test goal"
            # Missing 'backstory'
        }
        
        with pytest.raises(ValueError, match="Missing required field 'backstory'"):
            await create_agent(
                agent_key=agent_key,
                agent_config=incomplete_config,
                tools=mock_tools,
                config=mock_config
            )
    
    @pytest.mark.asyncio
    async def test_create_agent_empty_required_field(self, mock_tools, mock_config):
        """Test agent creation with empty required field"""
        agent_key = "test_agent"
        incomplete_config = {
            "role": "",  # Empty role
            "goal": "Test goal",
            "backstory": "Test backstory"
        }
        
        with pytest.raises(ValueError, match="Field 'role' cannot be empty"):
            await create_agent(
                agent_key=agent_key,
                agent_config=incomplete_config,
                tools=mock_tools,
                config=mock_config
            )
    
    @pytest.mark.asyncio
    async def test_create_agent_llm_configuration_error(self, mock_agent_config, mock_tools, mock_config):
        """Test agent creation with LLM configuration error"""
        agent_key = "test_agent"
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            # Mock LLM configuration to raise error
            mock_llm_manager.configure_crewai_llm = AsyncMock(side_effect=Exception("LLM config error"))
            
            # Mock UnitOfWork to prevent MCP service calls
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                await create_agent(
                    agent_key=agent_key,
                    agent_config=mock_agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                # Verify agent was still created with fallback LLM
                call_kwargs = mock_agent_class.call_args[1]
                assert call_kwargs["llm"] == mock_agent_config["llm"]  # Falls back to string
    
    @pytest.mark.asyncio
    async def test_create_agent_no_tools(self, mock_agent_config, mock_config):
        """Test agent creation without tools"""
        agent_key = "test_agent"
        agent_config_no_tools = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory"
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork to prevent MCP service calls
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config_no_tools,
                    tools=None,
                    config=mock_config
                )
                
                # Verify agent was created with empty tools list
                call_kwargs = mock_agent_class.call_args[1]
                assert call_kwargs["tools"] == []
    
    @pytest.mark.asyncio
    async def test_create_agent_with_knowledge_sources(self, mock_tools, mock_config):
        """Test agent creation with knowledge sources"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "knowledge_sources": [{"path": "/path/to/file.txt"}]
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                # Verify knowledge sources were processed
                call_kwargs = mock_agent_class.call_args[1]
                assert call_kwargs["knowledge_sources"] == ["/path/to/file.txt"]
    
    @pytest.mark.asyncio
    async def test_create_agent_with_llm_dict_config(self, mock_tools, mock_config):
        """Test agent creation with LLM dictionary configuration"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "llm": {
                "model": "gpt-4",
                "temperature": 0.7,
                "max_tokens": 1000
            }
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('crewai.LLM') as mock_llm_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            # Mock the configured LLM
            mock_configured_llm = MagicMock()
            mock_configured_llm.model = "openai/gpt-4"
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_configured_llm)
            
            # Mock vars() to return dict-like object
            with patch('builtins.vars', return_value={"model": "openai/gpt-4"}):
                # Mock the final LLM instance
                mock_final_llm = MagicMock()
                mock_llm_class.return_value = mock_final_llm
                
                # Mock UnitOfWork
                mock_uow_instance = AsyncMock()
                mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
                mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
                mock_uow.return_value = mock_uow_instance
                
                with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                    mock_mcp_instance = AsyncMock()
                    mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                    mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                    
                    result = await create_agent(
                        agent_key=agent_key,
                        agent_config=agent_config,
                        tools=mock_tools,
                        config=mock_config
                    )
                    
                    # Verify LLM was configured correctly
                    mock_llm_class.assert_called_once()
                    call_kwargs = mock_agent_class.call_args[1]
                    assert call_kwargs["llm"] == mock_final_llm
    
    @pytest.mark.asyncio
    async def test_create_agent_with_llm_dict_no_model(self, mock_tools, mock_config):
        """Test agent creation with LLM dict config but no model specified"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "llm": {
                "temperature": 0.7,
                "max_tokens": 1000
            }
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('crewai.LLM') as mock_llm_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            # Mock the default LLM
            mock_default_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_default_llm)
            
            # Mock vars() to return dict-like object
            with patch('builtins.vars', return_value={"model": "openai/gpt-4o"}):
                # Mock the final LLM instance
                mock_final_llm = MagicMock()
                mock_llm_class.return_value = mock_final_llm
                
                # Mock UnitOfWork
                mock_uow_instance = AsyncMock()
                mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
                mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
                mock_uow.return_value = mock_uow_instance
                
                with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                    mock_mcp_instance = AsyncMock()
                    mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                    mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                    
                    result = await create_agent(
                        agent_key=agent_key,
                        agent_config=agent_config,
                        tools=mock_tools,
                        config=mock_config
                    )
                    
                    # Verify default model was used
                    mock_llm_manager.configure_crewai_llm.assert_called_with("gpt-4o")
    
    @pytest.mark.asyncio
    async def test_create_agent_no_llm_config(self, mock_tools, mock_config):
        """Test agent creation without LLM configuration"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory"
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                # Verify default LLM was used
                mock_llm_manager.configure_crewai_llm.assert_called_with("gpt-4o")
    
    @pytest.mark.asyncio
    async def test_create_agent_with_additional_params(self, mock_tools, mock_config):
        """Test agent creation with additional parameters"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "max_iter": 5,
            "max_rpm": 10,
            "cache": True,
            "allow_code_execution": True,
            "max_retry_limit": 5,
            "reasoning": True,
            "max_reasoning_attempts": 3
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                # Verify additional parameters were set
                call_kwargs = mock_agent_class.call_args[1]
                assert call_kwargs["max_iter"] == 5
                assert call_kwargs["max_rpm"] == 10
                assert call_kwargs["cache"] == True
                assert call_kwargs["allow_code_execution"] == True
                assert call_kwargs["max_retry_limit"] == 5
                assert call_kwargs["reasoning"] == True
                assert call_kwargs["max_reasoning_attempts"] == 3
    
    @pytest.mark.asyncio
    async def test_create_agent_with_prompt_templates(self, mock_tools, mock_config):
        """Test agent creation with prompt templates"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "system_template": "Custom system prompt",
            "prompt_template": "Custom task prompt",
            "response_template": "Custom response format"
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                # Verify prompt templates were set
                call_kwargs = mock_agent_class.call_args[1]
                assert call_kwargs["system_prompt"] == "Custom system prompt"
                assert call_kwargs["task_prompt"] == "Custom task prompt"
                assert call_kwargs["format_prompt"] == "Custom response format"
    
    @pytest.mark.asyncio
    async def test_create_agent_with_llm_dict_fallback_no_model_attr(self, mock_tools, mock_config):
        """Test agent creation with LLM dict config when configured LLM has no model attribute"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "llm": {
                "model": "gpt-4",
                "temperature": 0.7
            }
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('crewai.LLM') as mock_llm_class, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            # Mock the configured LLM without model attribute
            mock_configured_llm = MagicMock()
            delattr(mock_configured_llm, 'model')  # Remove model attribute
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_configured_llm)
            
            # Mock the final LLM instance
            mock_final_llm = MagicMock()
            mock_llm_class.return_value = mock_final_llm
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                # Verify fallback kwargs were used
                mock_llm_class.assert_called_once()
                call_args = mock_llm_class.call_args[1]
                assert "model" in call_args
                assert call_args["temperature"] == 0.7
    
    @pytest.mark.asyncio 
    async def test_create_agent_with_tool_service_no_factory(self, mock_config):
        """Test agent creation with tool service but no tool factory"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "tools": ["tool1", "tool2"]
        }
        
        # Mock tool service
        mock_tool_service = AsyncMock()
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            mock_resolve.return_value = ["tool1", "tool2"]
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=None,
                    config=mock_config,
                    tool_service=mock_tool_service,
                    tool_factory=None  # No tool factory
                )
                
                # Verify tools were resolved but tool names were used (not instances)
                # Note: resolve_tool_ids_to_names actually gets called but returns empty list due to mocking
                call_kwargs = mock_agent_class.call_args[1]
                assert call_kwargs["tools"] == []  # Empty because no tool factory and resolve returns empty
    
    @pytest.mark.asyncio
    async def test_create_agent_mcp_tool_with_service_adapter(self, mock_config):
        """Test agent creation with MCP service adapter tool"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal", 
            "backstory": "Test backstory",
            "tools": ["mcp_tool"]
        }
        
        # Mock tool service
        mock_tool_service = AsyncMock()
        
        # Mock tool factory that returns MCP service adapter
        mock_tool_factory = MagicMock()
        mock_tool_factory.create_tool.return_value = (True, 'mcp_service_adapter')
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            mock_resolve.return_value = ["mcp_tool"]
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=None,
                    config=mock_config,
                    tool_service=mock_tool_service,
                    tool_factory=mock_tool_factory
                )
                
                # Verify MCP service adapter was skipped
                call_kwargs = mock_agent_class.call_args[1]
                assert len(call_kwargs["tools"]) == 0  # No tools added due to service adapter skip
    
    @pytest.mark.asyncio
    async def test_create_agent_tool_factory_returns_none(self, mock_config):
        """Test agent creation when tool factory returns None"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "tools": ["unknown_tool"]
        }
        
        # Mock tool service
        mock_tool_service = AsyncMock()
        
        # Mock tool factory that returns None
        mock_tool_factory = MagicMock()
        mock_tool_factory.create_tool.return_value = None
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            mock_resolve.return_value = ["unknown_tool"]
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=None,
                    config=mock_config,
                    tool_service=mock_tool_service,
                    tool_factory=mock_tool_factory
                )
                
                # Verify no tools were added since factory returned None
                call_kwargs = mock_agent_class.call_args[1]
                assert len(call_kwargs["tools"]) == 0
    
    @pytest.mark.asyncio
    async def test_create_agent_with_empty_tool_names(self, mock_config):
        """Test agent creation with empty tool names from resolution"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "tools": ["tool1"]
        }
        
        # Mock tool service
        mock_tool_service = AsyncMock()
        
        # Mock tool factory
        mock_tool_factory = MagicMock()
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Return empty tool name
            mock_resolve.return_value = [""]
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=None,
                    config=mock_config,
                    tool_service=mock_tool_service,
                    tool_factory=mock_tool_factory
                )
                
                # Verify empty tool name was skipped
                mock_tool_factory.create_tool.assert_not_called()
                call_kwargs = mock_agent_class.call_args[1]
                assert len(call_kwargs["tools"]) == 0
    
    @pytest.mark.asyncio
    async def test_create_agent_with_agent_llm_attribute_check(self, mock_tools, mock_config):
        """Test agent creation verifies llm attribute on agent"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory"
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            # Create mock agent without llm attribute
            mock_agent_instance = MagicMock()
            if hasattr(mock_agent_instance, 'llm'):
                delattr(mock_agent_instance, 'llm')
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                # Verify agent creation succeeded even without llm attribute
                assert result == mock_agent_instance
    
    @pytest.mark.asyncio
    async def test_create_agent_with_mcp_servers_enabled(self, mock_tools, mock_config):
        """Test agent creation with enabled MCP servers"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory"
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            # Mock MCP service with enabled servers
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                
                # Create mock server response
                mock_server = MagicMock()
                mock_server.id = "server1"
                mock_servers_response = MagicMock()
                mock_servers_response.servers = [mock_server]
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
                
                # Mock get_server_by_id to return None (server not found)
                mock_mcp_instance.get_server_by_id = AsyncMock(return_value=None)
                
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                # Verify agent was created successfully even with MCP server not found
                assert result == mock_agent_instance
                mock_mcp_instance.get_server_by_id.assert_called_once_with("server1")
    
    @pytest.mark.asyncio
    async def test_create_agent_with_tool_config_result_as_answer(self, mock_config):
        """Test agent creation with tool config having result_as_answer"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "tools": [1]  # Use integer ID
        }
        
        # Mock tool service with get_tool_config_by_name method
        mock_tool_service = AsyncMock()
        mock_tool_service.get_tool_config_by_name = AsyncMock(return_value={"result_as_answer": True})
        
        # Mock the tool returned by get_tool_by_id
        mock_tool_from_service = MagicMock()
        mock_tool_from_service.title = "tool1"
        mock_tool_service.get_tool_by_id = AsyncMock(return_value=mock_tool_from_service)
        
        # Mock tool factory
        mock_tool_factory = MagicMock()
        mock_tool_instance = MagicMock()
        mock_tool_instance.name = "tool1"
        mock_tool_factory.create_tool.return_value = mock_tool_instance
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            mock_resolve.return_value = ["tool1"]
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=None,
                    config=mock_config,
                    tool_service=mock_tool_service,
                    tool_factory=mock_tool_factory
                )
                
                # Verify tool was created with result_as_answer=True
                mock_tool_factory.create_tool.assert_called_once_with("tool1", result_as_answer=True)
                call_kwargs = mock_agent_class.call_args[1]
                assert len(call_kwargs["tools"]) == 1
    
    @pytest.mark.asyncio
    async def test_create_agent_tool_service_without_get_tool_config_method(self, mock_config):
        """Test agent creation with tool service that doesn't have get_tool_config_by_name method"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "tools": [1]  # Use integer ID
        }
        
        # Mock tool service without get_tool_config_by_name method
        mock_tool_service = MagicMock()  # Use MagicMock instead of AsyncMock
        
        # Mock the tool returned by get_tool_by_id
        mock_tool_from_service = MagicMock()
        mock_tool_from_service.title = "tool1"
        mock_tool_service.get_tool_by_id = AsyncMock(return_value=mock_tool_from_service)
        # Explicitly don't set get_tool_config_by_name method
        
        # Mock tool factory
        mock_tool_factory = MagicMock()
        mock_tool_instance = MagicMock()
        mock_tool_instance.name = "tool1"
        mock_tool_factory.create_tool.return_value = mock_tool_instance
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            mock_resolve.return_value = ["tool1"]
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=None,
                    config=mock_config,
                    tool_service=mock_tool_service,
                    tool_factory=mock_tool_factory
                )
                
                # Verify no tools were created due to await error on non-async method
                mock_tool_factory.create_tool.assert_not_called()
                call_kwargs = mock_agent_class.call_args[1]
                assert len(call_kwargs["tools"]) == 0
    
    @pytest.mark.asyncio
    async def test_create_agent_mcp_service_error(self, mock_tools, mock_config):
        """Test agent creation with MCP service error"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory"
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork to raise exception
            mock_uow.side_effect = Exception("MCP service error")
            
            result = await create_agent(
                agent_key=agent_key,
                agent_config=agent_config,
                tools=mock_tools,
                config=mock_config
            )
            
            # Verify agent was created successfully despite MCP error
            assert result == mock_agent_instance
    
    @pytest.mark.asyncio
    async def test_create_agent_tool_resolution_error(self, mock_config):
        """Test agent creation with tool resolution error"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "tools": ["tool1"]
        }
        
        # Mock tool service
        mock_tool_service = AsyncMock()
        mock_tool_factory = MagicMock()
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock resolve to raise error
            mock_resolve.side_effect = Exception("Tool resolution error")
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=None,
                    config=mock_config,
                    tool_service=mock_tool_service,
                    tool_factory=mock_tool_factory
                )
                
                # Verify agent was created successfully despite tool resolution error
                assert result == mock_agent_instance
                call_kwargs = mock_agent_class.call_args[1]
                assert len(call_kwargs["tools"]) == 0  # No tools due to error
    
    @pytest.mark.asyncio
    async def test_create_agent_with_mcp_tools_list(self, mock_config):
        """Test agent creation with MCP tools returning a list"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "tools": [1]  # Use integer ID
        }
        
        # Mock tool service
        mock_tool_service = AsyncMock()
        
        # Mock the tool returned by get_tool_by_id
        mock_tool_from_service = MagicMock()
        mock_tool_from_service.title = "mcp_tool"
        mock_tool_service.get_tool_by_id = AsyncMock(return_value=mock_tool_from_service)
        
        # Mock MCP tools
        mock_mcp_tool1 = MagicMock()
        mock_mcp_tool1.name = "mcp_tool1"
        mock_mcp_tool2 = MagicMock()
        mock_mcp_tool2.name = "mcp_tool2"
        
        # Mock tool factory that returns MCP tools list
        mock_tool_factory = MagicMock()
        mock_tool_factory.create_tool.return_value = (True, [mock_mcp_tool1, mock_mcp_tool2])
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            mock_resolve.return_value = ["mcp_tool"]
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=None,
                    config=mock_config,
                    tool_service=mock_tool_service,
                    tool_factory=mock_tool_factory
                )
                
                # Verify MCP tools were added
                call_kwargs = mock_agent_class.call_args[1]
                assert len(call_kwargs["tools"]) == 2  # Two MCP tools added
    
    @pytest.mark.asyncio
    async def test_create_agent_with_mcp_tools_unexpected_format(self, mock_config):
        """Test agent creation with MCP tools returning unexpected format"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "tools": [1]  # Use integer ID
        }
        
        # Mock tool service
        mock_tool_service = AsyncMock()
        
        # Mock the tool returned by get_tool_by_id
        mock_tool_from_service = MagicMock()
        mock_tool_from_service.title = "mcp_tool"
        mock_tool_service.get_tool_by_id = AsyncMock(return_value=mock_tool_from_service)
        
        # Mock tool factory that returns unexpected MCP format
        mock_tool_factory = MagicMock()
        mock_tool_factory.create_tool.return_value = (True, {"unexpected": "format"})
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            mock_resolve.return_value = ["mcp_tool"]
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=None,
                    config=mock_config,
                    tool_service=mock_tool_service,
                    tool_factory=mock_tool_factory
                )
                
                # Verify no tools were added due to unexpected format
                call_kwargs = mock_agent_class.call_args[1]
                assert len(call_kwargs["tools"]) == 0
    
    @pytest.mark.asyncio
    async def test_create_agent_tool_details_logging(self, mock_config):
        """Test agent creation with tool details logging"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "tools": [1]  # Use integer ID
        }
        
        # Mock tool service
        mock_tool_service = AsyncMock()
        
        # Mock the tool returned by get_tool_by_id
        mock_tool_from_service = MagicMock()
        mock_tool_from_service.title = "tool1"
        mock_tool_service.get_tool_by_id = AsyncMock(return_value=mock_tool_from_service)
        
        # Mock tool factory
        mock_tool_factory = MagicMock()
        mock_tool_instance = MagicMock()
        mock_tool_instance.name = "tool1"
        mock_tool_instance.description = "Test tool description"
        mock_tool_instance.api_key = "test_api_key"
        mock_tool_factory.create_tool.return_value = mock_tool_instance
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.engines.crewai.helpers.tool_helpers.resolve_tool_ids_to_names') as mock_resolve, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            mock_resolve.return_value = ["tool1"]
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=None,
                    config=mock_config,
                    tool_service=mock_tool_service,
                    tool_factory=mock_tool_factory
                )
                
                # Verify tool was added with proper details
                call_kwargs = mock_agent_class.call_args[1]
                assert len(call_kwargs["tools"]) == 1
                assert call_kwargs["tools"][0] == mock_tool_instance
    
    @pytest.mark.asyncio
    async def test_create_agent_tool_string_logging(self, mock_config):
        """Test agent creation with string tools logging"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory"
        }
        
        # Pass string tools directly
        string_tools = ["tool1", "tool2"]
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=string_tools,
                    config=mock_config
                )
                
                # Verify string tools were passed through
                call_kwargs = mock_agent_class.call_args[1]
                assert call_kwargs["tools"] == string_tools
    
    @pytest.mark.asyncio
    async def test_create_agent_with_sse_mcp_server(self, mock_tools, mock_config):
        """Test agent creation with SSE MCP server"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory"
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            # Mock MCP service with SSE server
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_adapter.AsyncMCPAdapter') as mock_adapter, \
                 patch('src.engines.crewai.tools.mcp_handler.wrap_mcp_tool') as mock_wrap, \
                 patch('src.engines.crewai.tools.mcp_handler.register_mcp_adapter') as mock_register:
                
                mock_mcp_instance = AsyncMock()
                
                # Create mock server
                mock_server = MagicMock()
                mock_server.id = "server1"
                mock_servers_response = MagicMock()
                mock_servers_response.servers = [mock_server]
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
                
                # Mock server details for SSE
                mock_server_detail = MagicMock()
                mock_server_detail.name = "test_sse_server"
                mock_server_detail.server_type = "sse"
                mock_server_detail.server_url = "https://test.databricksapps.com"
                mock_server_detail.api_key = "test_key"
                mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server_detail)
                
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                # Mock MCP adapter
                mock_adapter_instance = AsyncMock()
                mock_tool = MagicMock()
                mock_tool.name = "mcp_tool"
                mock_adapter_instance.tools = [mock_tool]
                mock_adapter.return_value = mock_adapter_instance
                
                # Mock wrap_mcp_tool
                mock_wrapped_tool = MagicMock()
                mock_wrap.return_value = mock_wrapped_tool
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                # Verify agent was created successfully
                assert result == mock_agent_instance
                
                # Verify adapter was initialized
                mock_adapter_instance.initialize.assert_called_once()
                
                # Verify tool was wrapped and registered
                mock_wrap.assert_called_once_with(mock_tool)
                mock_register.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_agent_with_stdio_mcp_server(self, mock_tools, mock_config):
        """Test agent creation with STDIO MCP server"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory"
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            # Mock MCP service with STDIO server
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_adapter.AsyncMCPAdapter') as mock_adapter, \
                 patch('src.engines.crewai.tools.mcp_handler.wrap_mcp_tool') as mock_wrap, \
                 patch('src.engines.crewai.tools.mcp_handler.register_mcp_adapter') as mock_register, \
                 patch('mcp.StdioServerParameters') as mock_stdio_params:
                
                mock_mcp_instance = AsyncMock()
                
                # Create mock server
                mock_server = MagicMock()
                mock_server.id = "server1"
                mock_servers_response = MagicMock()
                mock_servers_response.servers = [mock_server]
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
                
                # Mock server details for STDIO
                mock_server_detail = MagicMock()
                mock_server_detail.name = "test_stdio_server"
                mock_server_detail.server_type = "stdio"
                mock_server_detail.command = "python"
                mock_server_detail.args = ["-m", "test_server"]
                mock_server_detail.api_key = "test_key"
                mock_server_detail.additional_config = {"custom_param": "value"}
                mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server_detail)
                
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                # Mock MCP adapter
                mock_adapter_instance = AsyncMock()
                mock_tool = MagicMock()
                mock_tool.name = "stdio_tool"
                mock_adapter_instance.tools = [mock_tool]
                mock_adapter.return_value = mock_adapter_instance
                
                # Mock wrap_mcp_tool
                mock_wrapped_tool = MagicMock()
                mock_wrap.return_value = mock_wrapped_tool
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                # Verify agent was created successfully
                assert result == mock_agent_instance
                
                # Verify STDIO parameters were created
                mock_stdio_params.assert_called_once()
                
                # Verify adapter was initialized
                mock_adapter_instance.initialize.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_agent_with_unsupported_mcp_server_type(self, mock_tools, mock_config):
        """Test agent creation with unsupported MCP server type"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory"
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            # Mock MCP service with unsupported server type
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                
                # Create mock server
                mock_server = MagicMock()
                mock_server.id = "server1"
                mock_servers_response = MagicMock()
                mock_servers_response.servers = [mock_server]
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
                
                # Mock server details with unsupported type
                mock_server_detail = MagicMock()
                mock_server_detail.name = "test_unsupported_server"
                mock_server_detail.server_type = "websocket"  # Unsupported type
                mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server_detail)
                
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                # Verify agent was created successfully despite unsupported server type
                assert result == mock_agent_instance
    
    @pytest.mark.asyncio
    async def test_create_agent_mcp_adapter_error(self, mock_tools, mock_config):
        """Test agent creation with MCP adapter initialization error"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory"
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            # Mock MCP service with SSE server that fails
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service, \
                 patch('src.engines.crewai.tools.mcp_adapter.AsyncMCPAdapter') as mock_adapter:
                
                mock_mcp_instance = AsyncMock()
                
                # Create mock server
                mock_server = MagicMock()
                mock_server.id = "server1"
                mock_servers_response = MagicMock()
                mock_servers_response.servers = [mock_server]
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=mock_servers_response)
                
                # Mock server details for SSE
                mock_server_detail = MagicMock()
                mock_server_detail.name = "test_sse_server"
                mock_server_detail.server_type = "sse"
                mock_server_detail.server_url = "https://test.com/sse"
                mock_server_detail.api_key = "test_key"
                mock_mcp_instance.get_server_by_id = AsyncMock(return_value=mock_server_detail)
                
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                # Mock adapter to raise error during initialization
                mock_adapter.side_effect = Exception("Adapter initialization failed")
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                # Verify agent was created successfully despite adapter error
                assert result == mock_agent_instance
    
    @pytest.mark.asyncio
    async def test_create_agent_with_additional_params_none_values(self, mock_tools, mock_config):
        """Test agent creation with additional parameters that have None values"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "max_iter": None,  # None value should be skipped
            "max_rpm": 10,
            "memory": None,  # None value should be skipped
            "cache": True
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                # Verify None values were not passed to agent
                call_kwargs = mock_agent_class.call_args[1]
                assert "max_iter" not in call_kwargs  # None value should be skipped
                assert "memory" not in call_kwargs   # None value should be skipped
                assert call_kwargs["max_rpm"] == 10  # Non-None value should be included
                assert call_kwargs["cache"] == True  # Non-None value should be included
    
    @pytest.mark.asyncio
    async def test_create_agent_with_empty_prompt_templates(self, mock_tools, mock_config):
        """Test agent creation with empty prompt templates"""
        agent_key = "test_agent"
        agent_config = {
            "role": "Test Agent",
            "goal": "Test goal",
            "backstory": "Test backstory",
            "system_template": "",  # Empty should not be set
            "prompt_template": "Custom task prompt",
            "response_template": None  # None should not be set
        }
        
        with patch('src.engines.crewai.helpers.agent_helpers.Agent') as mock_agent_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.core.unit_of_work.UnitOfWork') as mock_uow:
            
            mock_agent_instance = MagicMock()
            mock_agent_class.return_value = mock_agent_instance
            
            mock_llm = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock(return_value=mock_llm)
            
            # Mock UnitOfWork
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
            mock_uow_instance.__aexit__ = AsyncMock(return_value=None)
            mock_uow.return_value = mock_uow_instance
            
            with patch('src.services.mcp_service.MCPService') as mock_mcp_service:
                mock_mcp_instance = AsyncMock()
                mock_mcp_instance.get_enabled_servers = AsyncMock(return_value=None)
                mock_mcp_service.from_unit_of_work = AsyncMock(return_value=mock_mcp_instance)
                
                result = await create_agent(
                    agent_key=agent_key,
                    agent_config=agent_config,
                    tools=mock_tools,
                    config=mock_config
                )
                
                # Verify only non-empty prompt templates were set
                call_kwargs = mock_agent_class.call_args[1]
                assert "system_prompt" not in call_kwargs  # Empty string should not be set
                assert call_kwargs["task_prompt"] == "Custom task prompt"  # Non-empty should be set
                assert "format_prompt" not in call_kwargs  # None should not be set