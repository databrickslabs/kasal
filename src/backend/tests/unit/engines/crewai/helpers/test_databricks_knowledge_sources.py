"""Unit tests for Databricks knowledge search tool.

NOTE: The knowledge source approach has been replaced with DatabricksKnowledgeSearchTool.
This file is kept for reference but tests are disabled.
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock, call
import os
import sys

# Add the backend src directory to the path
sys.path.insert(0, '/Users/nehme.tohme/workspace/kasal/src/backend')

# NOTE: process_knowledge_sources removed - using DatabricksKnowledgeSearchTool instead
# from src.engines.crewai.helpers.agent_helpers import create_agent

'''
# Tests commented out as knowledge sources have been replaced with tool-based approach


class TestDatabricksKnowledgeSources:
    """Test suite for Databricks volume knowledge source processing."""
    
    @patch.dict(os.environ, {
        'DATABRICKS_HOST': 'https://test.databricks.com',
        'DATABRICKS_TOKEN': 'test-token-123'
    })
    @patch('src.engines.crewai.knowledge.databricks_volume_knowledge_source.DatabricksVolumeKnowledgeSource')
    def test_process_databricks_volume_source(self, mock_databricks_class):
        """Test processing Databricks volume knowledge sources."""
        # Setup mock
        mock_instance = MagicMock()
        mock_databricks_class.return_value = mock_instance
        
        # Test data
        sources = [{
            'type': 'databricks_volume',
            'source': '/Volumes/users/test/knowledge/file.docx',
            'metadata': {
                'filename': 'file.docx',
                'execution_id': 'exec-123',
                'group_id': 'group-456',
                'uploaded_at': '2025-01-01T12:00:00Z'
            }
        }]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            # Verify DatabricksVolumeKnowledgeSource was created
            mock_databricks_class.assert_called_once_with(
                volume_path='users.test.knowledge',
                execution_id='exec-123',
                group_id='group-456',
                file_paths=['/Volumes/users/test/knowledge/file.docx'],
                workspace_url='https://test.databricks.com',
                token='test-token-123'
            )
            
            # Verify result contains the mock instance
            assert result == [mock_instance]
            
            # Verify logging
            mock_logger.info.assert_any_call(
                "[CREW] Processing 1 knowledge sources: " + str(sources)
            )
            mock_logger.info.assert_any_call(
                "[CREW] Creating DatabricksVolumeKnowledgeSource with full path: /Volumes/users/test/knowledge/file.docx, metadata: {'filename': 'file.docx', 'execution_id': 'exec-123', 'group_id': 'group-456', 'uploaded_at': '2025-01-01T12:00:00Z'}"
            )
    
    @patch.dict(os.environ, {
        'DATABRICKS_HOST': 'https://test.databricks.com',
        'DATABRICKS_TOKEN': 'test-token-123'
    })
    @patch('src.engines.crewai.knowledge.databricks_volume_knowledge_source.DatabricksVolumeKnowledgeSource')
    def test_process_multiple_databricks_sources(self, mock_databricks_class):
        """Test processing multiple Databricks volume knowledge sources."""
        # Setup mock to return different instances
        mock_instances = [MagicMock(), MagicMock()]
        mock_databricks_class.side_effect = mock_instances
        
        # Test data with multiple sources
        sources = [
            {
                'type': 'databricks_volume',
                'source': '/Volumes/users/test/knowledge/file1.pdf',
                'metadata': {
                    'filename': 'file1.pdf',
                    'execution_id': 'exec-123',
                    'group_id': 'group-456',
                    'uploaded_at': '2025-01-01T12:00:00Z'
                }
            },
            {
                'type': 'databricks_volume',
                'source': '/Volumes/users/test/knowledge/file2.docx',
                'metadata': {
                    'filename': 'file2.docx',
                    'execution_id': 'exec-456',
                    'group_id': 'group-789',
                    'uploaded_at': '2025-01-02T14:00:00Z'
                }
            }
        ]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(sources)
            
            # Verify both instances were created
            assert mock_databricks_class.call_count == 2
            assert result == mock_instances
            
            # Verify first call
            first_call = mock_databricks_class.call_args_list[0]
            assert first_call[1]['volume_path'] == 'users.test.knowledge'
            assert first_call[1]['execution_id'] == 'exec-123'
            assert first_call[1]['group_id'] == 'group-456'
            assert first_call[1]['file_paths'] == ['/Volumes/users/test/knowledge/file1.pdf']
            
            # Verify second call
            second_call = mock_databricks_class.call_args_list[1]
            assert second_call[1]['volume_path'] == 'users.test.knowledge'
            assert second_call[1]['execution_id'] == 'exec-456'
            assert second_call[1]['group_id'] == 'group-789'
            assert second_call[1]['file_paths'] == ['/Volumes/users/test/knowledge/file2.docx']
    
    @patch.dict(os.environ, {
        'DATABRICKS_HOST': 'https://test.databricks.com',
        'DATABRICKS_TOKEN': 'test-token-123'
    })
    @patch('src.engines.crewai.knowledge.databricks_volume_knowledge_source.DatabricksVolumeKnowledgeSource')
    def test_process_mixed_knowledge_sources(self, mock_databricks_class):
        """Test processing mixed types of knowledge sources."""
        mock_instance = MagicMock()
        mock_databricks_class.return_value = mock_instance
        
        # Test data with mixed source types
        sources = [
            {
                'type': 'databricks_volume',
                'source': '/Volumes/users/test/knowledge/file.pdf',
                'metadata': {
                    'filename': 'file.pdf',
                    'execution_id': 'exec-123',
                    'group_id': 'group-456'
                }
            },
            '/path/to/local/file.txt',  # String path
            {'path': '/another/path/file.docx'}  # Dict with path key
        ]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger'):
            result = process_knowledge_sources(sources)
            
            # Verify mixed results
            assert len(result) == 3
            assert result[0] == mock_instance  # Databricks source
            assert result[1] == '/path/to/local/file.txt'  # String path
            assert result[2] == '/another/path/file.docx'  # Extracted path
    
    def test_process_empty_knowledge_sources(self):
        """Test processing empty knowledge sources list."""
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources([])
            
            assert result == []
            mock_logger.info.assert_called_once_with("[CREW] No knowledge sources to process")
    
    def test_process_none_knowledge_sources(self):
        """Test processing None knowledge sources."""
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = process_knowledge_sources(None)
            
            assert result is None
            mock_logger.info.assert_called_once_with("[CREW] No knowledge sources to process")
    
    @patch.dict(os.environ, {
        'DATABRICKS_HOST': 'https://test.databricks.com',
        'DATABRICKS_TOKEN': 'test-token-123'
    })
    @patch('src.engines.crewai.knowledge.databricks_volume_knowledge_source.DatabricksVolumeKnowledgeSource')
    def test_databricks_source_without_filename(self, mock_databricks_class):
        """Test processing Databricks source without filename in metadata."""
        mock_instance = MagicMock()
        mock_databricks_class.return_value = mock_instance
        
        sources = [{
            'type': 'databricks_volume',
            'source': '/Volumes/users/test/knowledge/file.pdf',
            'metadata': {
                'execution_id': 'exec-123',
                'group_id': 'group-456'
            }
        }]
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger'):
            result = process_knowledge_sources(sources)
            
            # Verify file_paths contains full path even when no filename
            mock_databricks_class.assert_called_once()
            call_args = mock_databricks_class.call_args[1]
            assert call_args['file_paths'] == ['/Volumes/users/test/knowledge/file.pdf']
            assert result == [mock_instance]
    
    @patch('src.engines.crewai.helpers.agent_helpers.logger')
    def test_handle_invalid_source_type(self, mock_logger):
        """Test handling of invalid source types."""
        sources = [
            {'invalid': 'structure', 'no_type': 'field'},
            123,  # Invalid type
            None  # None in list
        ]
        
        result = process_knowledge_sources(sources)
        
        # Should handle gracefully and log warnings for unknown formats
        assert result == []
        assert mock_logger.warning.call_count >= 3  # One for each invalid source


class TestCreateAgentWithKnowledgeSources:
    """Test suite for create_agent function with knowledge sources."""
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.agent_helpers.Agent')
    @patch('src.engines.crewai.helpers.agent_helpers.process_knowledge_sources')
    @patch('src.core.llm_manager.LLMManager.configure_crewai_llm')
    @patch('src.core.unit_of_work.UnitOfWork')
    async def test_create_agent_with_knowledge_sources(self, mock_uow, mock_configure_llm, mock_process_ks, mock_agent_class):
        """Test creating an agent with knowledge sources."""
        # Setup mocks
        mock_processed_sources = [MagicMock(), MagicMock()]
        mock_process_ks.return_value = mock_processed_sources
        mock_agent_instance = MagicMock()
        mock_agent_class.return_value = mock_agent_instance
        
        # Agent config with knowledge sources
        original_sources = [
            {
                'type': 'databricks_volume',
                'source': '/Volumes/test/file.pdf',
                'metadata': {'filename': 'file.pdf'}
            }
        ]
        agent_config = {
            'role': 'Test Agent',
            'goal': 'Test Goal',
            'backstory': 'Test Backstory',
            'knowledge_sources': original_sources.copy()
        }
        
        # Mock services
        mock_tool_service = AsyncMock()
        mock_tool_factory = MagicMock()
        
        # Setup LLM mock
        mock_llm = MagicMock()
        mock_configure_llm.return_value = mock_llm
        
        # Setup UnitOfWork mock
        mock_uow_instance = AsyncMock()
        mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
        mock_uow_instance.__aexit__ = AsyncMock(return_value=False)
        mock_uow.return_value = mock_uow_instance
        
        with patch('src.engines.crewai.helpers.agent_helpers.logger') as mock_logger:
            result = await create_agent(
                agent_key='test_agent',
                agent_config=agent_config,
                tool_service=mock_tool_service,
                tool_factory=mock_tool_factory
            )
            
            # Verify knowledge sources were processed
            mock_process_ks.assert_called_once()
            # The actual call should be with the original knowledge_sources from config
            actual_call_args = mock_process_ks.call_args[0][0]
            assert actual_call_args == original_sources
            
            # Verify logging
            mock_logger.info.assert_any_call(
                "[CREW] Agent test_agent has 1 knowledge sources"
            )
            
            # Verify agent was created with processed sources
            agent_call_kwargs = mock_agent_class.call_args[1]
            assert 'knowledge_sources' in agent_call_kwargs
            assert agent_call_kwargs['knowledge_sources'] == mock_processed_sources
            
            assert result == mock_agent_instance
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.helpers.agent_helpers.Agent')
    @patch('src.core.llm_manager.LLMManager.configure_crewai_llm')
    @patch('src.core.unit_of_work.UnitOfWork')
    async def test_create_agent_without_knowledge_sources(self, mock_uow, mock_configure_llm, mock_agent_class):
        """Test creating an agent without knowledge sources."""
        mock_agent_instance = MagicMock()
        mock_agent_class.return_value = mock_agent_instance
        
        agent_config = {
            'role': 'Test Agent',
            'goal': 'Test Goal',
            'backstory': 'Test Backstory'
        }
        
        mock_tool_service = AsyncMock()
        mock_tool_factory = MagicMock()
        
        # Setup LLM mock
        mock_llm = MagicMock()
        mock_configure_llm.return_value = mock_llm
        
        # Setup UnitOfWork mock
        mock_uow_instance = AsyncMock()
        mock_uow_instance.__aenter__ = AsyncMock(return_value=mock_uow_instance)
        mock_uow_instance.__aexit__ = AsyncMock(return_value=False)
        mock_uow.return_value = mock_uow_instance
        
        result = await create_agent(
            agent_key='test_agent',
            agent_config=agent_config,
            tool_service=mock_tool_service,
            tool_factory=mock_tool_factory
        )
        
        # Verify agent was created without knowledge_sources parameter
        # Check if mock_agent_class was actually called
        assert mock_agent_class.called, "Agent class should have been called"
        if mock_agent_class.call_args:
            agent_call_kwargs = mock_agent_class.call_args[1]
            assert 'knowledge_sources' not in agent_call_kwargs or agent_call_kwargs.get('knowledge_sources') is None or agent_call_kwargs.get('knowledge_sources') == []
        
        assert result == mock_agent_instance'''
