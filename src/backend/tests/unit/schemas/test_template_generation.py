"""
Unit tests for template generation schemas.

Tests the functionality of Pydantic schemas for template generation operations
including validation, serialization, and field constraints.
"""
import pytest
from pydantic import ValidationError

from src.schemas.template_generation import (
    TemplateGenerationRequest, TemplateGenerationResponse
)


class TestTemplateGenerationRequest:
    """Test cases for TemplateGenerationRequest schema."""
    
    def test_valid_template_generation_request_minimal(self):
        """Test TemplateGenerationRequest with minimal required fields."""
        request_data = {
            "role": "Data Analyst",
            "goal": "Analyze sales data to identify trends",
            "backstory": "Expert analyst with 5 years of experience"
        }
        request = TemplateGenerationRequest(**request_data)
        assert request.role == "Data Analyst"
        assert request.goal == "Analyze sales data to identify trends"
        assert request.backstory == "Expert analyst with 5 years of experience"
        assert request.model == "databricks-llama-4-maverick"  # Default value
    
    def test_valid_template_generation_request_with_model(self):
        """Test TemplateGenerationRequest with custom model."""
        request_data = {
            "role": "Software Engineer",
            "goal": "Develop high-quality applications",
            "backstory": "Senior developer with expertise in Python and AI",
            "model": "gpt-4"
        }
        request = TemplateGenerationRequest(**request_data)
        assert request.role == "Software Engineer"
        assert request.goal == "Develop high-quality applications"
        assert request.backstory == "Senior developer with expertise in Python and AI"
        assert request.model == "gpt-4"
    
    def test_template_generation_request_missing_required_fields(self):
        """Test TemplateGenerationRequest validation with missing required fields."""
        # Missing role
        with pytest.raises(ValidationError) as exc_info:
            TemplateGenerationRequest(
                goal="Test goal",
                backstory="Test backstory"
            )
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "role" in missing_fields
        
        # Missing goal
        with pytest.raises(ValidationError) as exc_info:
            TemplateGenerationRequest(
                role="Test role",
                backstory="Test backstory"
            )
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "goal" in missing_fields
        
        # Missing backstory
        with pytest.raises(ValidationError) as exc_info:
            TemplateGenerationRequest(
                role="Test role",
                goal="Test goal"
            )
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "backstory" in missing_fields
    
    def test_template_generation_request_empty_fields(self):
        """Test TemplateGenerationRequest with empty string fields."""
        request_data = {
            "role": "",
            "goal": "",
            "backstory": ""
        }
        request = TemplateGenerationRequest(**request_data)
        assert request.role == ""
        assert request.goal == ""
        assert request.backstory == ""
    
    def test_template_generation_request_long_fields(self):
        """Test TemplateGenerationRequest with long field values."""
        long_text = "A" * 1000
        request_data = {
            "role": long_text,
            "goal": long_text,
            "backstory": long_text,
            "model": "claude-3-opus-20240229"
        }
        request = TemplateGenerationRequest(**request_data)
        assert request.role == long_text
        assert request.goal == long_text
        assert request.backstory == long_text
    
    def test_template_generation_request_special_characters(self):
        """Test TemplateGenerationRequest with special characters."""
        request_data = {
            "role": "AI/ML Engineer & Data Scientist",
            "goal": "Build & deploy ML models (95% accuracy)",
            "backstory": "PhD in CS, specializes in NLP/CV @TechCorp",
            "model": "anthropic-claude-3"
        }
        request = TemplateGenerationRequest(**request_data)
        assert request.role == "AI/ML Engineer & Data Scientist"
        assert request.goal == "Build & deploy ML models (95% accuracy)"
        assert request.backstory == "PhD in CS, specializes in NLP/CV @TechCorp"
    
    def test_template_generation_request_multiline_text(self):
        """Test TemplateGenerationRequest with multiline text."""
        multiline_backstory = """Senior Data Scientist with expertise in:
- Machine Learning algorithms
- Statistical analysis
- Python and R programming
- Data visualization tools"""
        
        request_data = {
            "role": "Senior Data Scientist",
            "goal": "Extract actionable insights from complex datasets",
            "backstory": multiline_backstory
        }
        request = TemplateGenerationRequest(**request_data)
        assert request.backstory == multiline_backstory
        assert "\n" in request.backstory
    
    def test_template_generation_request_various_models(self):
        """Test TemplateGenerationRequest with various model values."""
        models = [
            "gpt-4",
            "gpt-3.5-turbo",
            "claude-3-opus-20240229",
            "claude-3-sonnet",
            "databricks-meta-llama-3-1-405b-instruct",
            "local-model-v1.0"
        ]
        
        for model in models:
            request_data = {
                "role": "Test Role",
                "goal": "Test Goal",
                "backstory": "Test Backstory",
                "model": model
            }
            request = TemplateGenerationRequest(**request_data)
            assert request.model == model
    
    def test_template_generation_request_realistic_scenarios(self):
        """Test TemplateGenerationRequest with realistic scenarios."""
        scenarios = [
            {
                "role": "Financial Analyst",
                "goal": "Analyze quarterly financial reports and identify key performance indicators",
                "backstory": "CPA with 8 years of experience in financial analysis and reporting for Fortune 500 companies",
                "model": "gpt-4"
            },
            {
                "role": "Research Assistant",
                "goal": "Conduct comprehensive literature reviews and summarize research findings",
                "backstory": "PhD candidate in Computer Science with expertise in academic research methodologies",
                "model": "claude-3-opus-20240229"
            },
            {
                "role": "Content Creator",
                "goal": "Generate engaging and informative content for various platforms",
                "backstory": "Creative writer with 6 years of experience in digital marketing and content strategy",
                "model": "databricks-llama-4-maverick"
            }
        ]
        
        for scenario in scenarios:
            request = TemplateGenerationRequest(**scenario)
            assert request.role == scenario["role"]
            assert request.goal == scenario["goal"]
            assert request.backstory == scenario["backstory"]
            assert request.model == scenario["model"]


class TestTemplateGenerationResponse:
    """Test cases for TemplateGenerationResponse schema."""
    
    def test_valid_template_generation_response(self):
        """Test TemplateGenerationResponse with valid data."""
        response_data = {
            "system_template": "You are a helpful {role} assistant.",
            "prompt_template": "Please help me with: {user_input}",
            "response_template": "Based on my analysis: {response}"
        }
        response = TemplateGenerationResponse(**response_data)
        assert response.system_template == "You are a helpful {role} assistant."
        assert response.prompt_template == "Please help me with: {user_input}"
        assert response.response_template == "Based on my analysis: {response}"
    
    def test_template_generation_response_missing_required_fields(self):
        """Test TemplateGenerationResponse validation with missing required fields."""
        # Missing system_template
        with pytest.raises(ValidationError) as exc_info:
            TemplateGenerationResponse(
                prompt_template="Test prompt",
                response_template="Test response"
            )
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "system_template" in missing_fields
        
        # Missing prompt_template
        with pytest.raises(ValidationError) as exc_info:
            TemplateGenerationResponse(
                system_template="Test system",
                response_template="Test response"
            )
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "prompt_template" in missing_fields
        
        # Missing response_template
        with pytest.raises(ValidationError) as exc_info:
            TemplateGenerationResponse(
                system_template="Test system",
                prompt_template="Test prompt"
            )
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "response_template" in missing_fields
    
    def test_template_generation_response_empty_templates(self):
        """Test TemplateGenerationResponse with empty templates."""
        response_data = {
            "system_template": "",
            "prompt_template": "",
            "response_template": ""
        }
        response = TemplateGenerationResponse(**response_data)
        assert response.system_template == ""
        assert response.prompt_template == ""
        assert response.response_template == ""
    
    def test_template_generation_response_complex_templates(self):
        """Test TemplateGenerationResponse with complex template structures."""
        response_data = {
            "system_template": """You are a {role} with the following characteristics:
- Goal: {goal}
- Background: {backstory}
- Expertise: {expertise}

Please provide detailed and accurate responses based on your role.""",
            "prompt_template": """Context: {context}
User Query: {user_input}
Additional Parameters: {parameters}

Please analyze the above and provide a comprehensive response.""",
            "response_template": """## Analysis

{analysis}

## Recommendations

{recommendations}

## Next Steps

{next_steps}

---
*Generated by {role} on {timestamp}*"""
        }
        response = TemplateGenerationResponse(**response_data)
        assert "{role}" in response.system_template
        assert "{user_input}" in response.prompt_template
        assert "{analysis}" in response.response_template
    
    def test_template_generation_response_with_placeholders(self):
        """Test TemplateGenerationResponse with various placeholder patterns."""
        response_data = {
            "system_template": "System: {variable1} and {{variable2}} and %variable3%",
            "prompt_template": "Prompt: [input] <context> {data}",
            "response_template": "Response: ${output} #{tag} @mention"
        }
        response = TemplateGenerationResponse(**response_data)
        assert "{variable1}" in response.system_template
        assert "{{variable2}}" in response.system_template
        assert "%variable3%" in response.system_template
        assert "[input]" in response.prompt_template
        assert "<context>" in response.prompt_template
        assert "${output}" in response.response_template
        assert "#{tag}" in response.response_template
        assert "@mention" in response.response_template
    
    def test_template_generation_response_multiline_templates(self):
        """Test TemplateGenerationResponse with multiline templates."""
        system_template = """You are a {role} AI assistant.
Your primary goal is: {goal}
Your background: {backstory}

Guidelines:
1. Be helpful and accurate
2. Provide detailed explanations
3. Ask clarifying questions when needed"""
        
        prompt_template = """User Request: {user_input}

Context Information:
- Session ID: {session_id}
- User Preferences: {preferences}
- Previous Interactions: {history}

Please respond appropriately."""
        
        response_template = """Thank you for your question.

{main_response}

Additional Information:
{additional_info}

Would you like me to elaborate on any specific aspect?"""
        
        response_data = {
            "system_template": system_template,
            "prompt_template": prompt_template,
            "response_template": response_template
        }
        response = TemplateGenerationResponse(**response_data)
        assert "\n" in response.system_template
        assert "\n" in response.prompt_template
        assert "\n" in response.response_template
        assert response.system_template == system_template
    
    def test_template_generation_response_special_characters(self):
        """Test TemplateGenerationResponse with special characters in templates."""
        response_data = {
            "system_template": "System: Special chars !@#$%^&*(){}[]|\\:;\"'<>,.?/~`",
            "prompt_template": "Prompt: Unicode chars αβγδε 中文 🚀 ñáéíóú",
            "response_template": "Response: Math symbols ∑∏∫∞ ≤≥≠ ±×÷"
        }
        response = TemplateGenerationResponse(**response_data)
        assert "!@#$%^&*()" in response.system_template
        assert "αβγδε" in response.prompt_template
        assert "中文" in response.prompt_template
        assert "🚀" in response.prompt_template
        assert "∑∏∫∞" in response.response_template
    
    def test_template_generation_response_realistic_examples(self):
        """Test TemplateGenerationResponse with realistic template examples."""
        examples = [
            {
                "system_template": "You are a Data Analyst specializing in sales analytics. Your goal is to analyze sales data and identify trends. You have 5 years of experience working with Fortune 500 companies.",
                "prompt_template": "Please analyze the following sales data: {sales_data}. Focus on identifying trends for the period {time_period} and provide insights about {focus_area}.",
                "response_template": "Based on my analysis of the sales data for {time_period}, I've identified the following key trends:\n\n{trends}\n\nKey insights regarding {focus_area}:\n{insights}\n\nRecommendations:\n{recommendations}"
            },
            {
                "system_template": "You are a Research Assistant with expertise in academic literature review. Your goal is to conduct comprehensive research and summarize findings. You have a PhD in Computer Science.",
                "prompt_template": "Please conduct a literature review on the topic: {research_topic}. Include papers from {date_range} and focus on {specific_aspects}. Provide citations in {citation_format} format.",
                "response_template": "# Literature Review: {research_topic}\n\n## Summary\n{summary}\n\n## Key Findings\n{key_findings}\n\n## References\n{references}\n\n## Recommendations for Further Research\n{further_research}"
            }
        ]
        
        for example in examples:
            response = TemplateGenerationResponse(**example)
            assert len(response.system_template) > 50
            assert len(response.prompt_template) > 30
            assert len(response.response_template) > 40
            assert "{" in response.prompt_template
            assert "{" in response.response_template


class TestTemplateGenerationIntegration:
    """Integration tests for template generation schema interactions."""
    
    def test_request_response_workflow(self):
        """Test complete request-response workflow."""
        # Create request
        request = TemplateGenerationRequest(
            role="Marketing Analyst",
            goal="Analyze marketing campaign performance and ROI",
            backstory="Digital marketing expert with 7 years of experience in data-driven marketing analytics",
            model="gpt-4"
        )
        
        # Create corresponding response (simulating what API would return)
        response = TemplateGenerationResponse(
            system_template=f"You are a {request.role} AI assistant. Your goal is: {request.goal}. Background: {request.backstory}",
            prompt_template="Please analyze the marketing data: {campaign_data} for the period {period} and calculate ROI for {channels}.",
            response_template="## Campaign Performance Analysis\n\n{performance_summary}\n\n## ROI Analysis\n{roi_analysis}\n\n## Recommendations\n{recommendations}"
        )
        
        # Verify workflow
        assert request.role in response.system_template
        assert request.goal in response.system_template
        assert request.backstory in response.system_template
        assert "{campaign_data}" in response.prompt_template
        assert "{roi_analysis}" in response.response_template
    
    def test_multiple_agent_template_generation(self):
        """Test template generation for multiple agent types."""
        agent_configs = [
            {
                "role": "Financial Advisor",
                "goal": "Provide investment advice and financial planning",
                "backstory": "CFA charterholder with 10 years of experience in wealth management"
            },
            {
                "role": "Technical Writer",
                "goal": "Create clear and comprehensive technical documentation",
                "backstory": "Technical communications specialist with expertise in software documentation"
            },
            {
                "role": "Customer Support Agent",
                "goal": "Resolve customer issues efficiently and maintain satisfaction",
                "backstory": "Customer service professional with experience in SaaS support"
            }
        ]
        
        requests = []
        responses = []
        
        for config in agent_configs:
            # Create request
            request = TemplateGenerationRequest(**config)
            requests.append(request)
            
            # Create response
            response = TemplateGenerationResponse(
                system_template=f"You are a {config['role']} with the goal: {config['goal']}",
                prompt_template=f"As a {config['role']}, please help with: {{user_request}}",
                response_template="Based on my expertise as a {role}, here's my response:\n\n{response_content}"
            )
            responses.append(response)
        
        # Verify all templates were created
        assert len(requests) == 3
        assert len(responses) == 3
        
        # Verify each template contains role-specific content
        for i, (request, response) in enumerate(zip(requests, responses)):
            assert request.role == agent_configs[i]["role"]
            assert request.goal == agent_configs[i]["goal"]
            assert request.backstory == agent_configs[i]["backstory"]
            assert request.role in response.system_template
    
    def test_template_validation_scenarios(self):
        """Test various template validation scenarios."""
        # Valid minimal case
        minimal_request = TemplateGenerationRequest(
            role="Assistant",
            goal="Help users",
            backstory="Helpful AI"
        )
        minimal_response = TemplateGenerationResponse(
            system_template="You are an assistant",
            prompt_template="User: {input}",
            response_template="Response: {output}"
        )
        
        assert minimal_request.model == "databricks-llama-4-maverick"  # Default
        assert len(minimal_response.system_template) > 0
        
        # Valid complex case
        complex_request = TemplateGenerationRequest(
            role="AI Research Scientist",
            goal="Advance the field of artificial intelligence through rigorous research and development",
            backstory="PhD in Machine Learning from Stanford, 15 years of experience at leading AI labs, published 50+ papers in top-tier conferences",
            model="claude-3-opus-20240229"
        )
        
        complex_response = TemplateGenerationResponse(
            system_template="""You are an AI Research Scientist with the following profile:
- Role: {role}
- Goal: {goal}  
- Background: {backstory}
- Expertise: Machine Learning, Deep Learning, Natural Language Processing
- Research Focus: {research_focus}

Provide scientifically rigorous and well-researched responses.""",
            prompt_template="""Research Query: {query}
Domain: {domain}
Methodology Requirements: {methodology}
Timeline: {timeline}

Please provide a comprehensive research-based response.""",
            response_template="""# Research Response

## Executive Summary
{executive_summary}

## Detailed Analysis
{detailed_analysis}

## Methodology
{methodology_used}

## Results and Findings
{results}

## Limitations and Future Work
{limitations}

## References
{references}"""
        )
        
        assert len(complex_request.backstory) > 100
        assert len(complex_response.system_template) > 200
        assert complex_response.response_template.count("{") >= 6  # Multiple placeholders
    
    def test_edge_cases_and_error_handling(self):
        """Test edge cases and error handling."""
        # Test with very long content
        very_long_text = "x" * 10000
        long_request = TemplateGenerationRequest(
            role=very_long_text,
            goal=very_long_text,
            backstory=very_long_text
        )
        assert len(long_request.role) == 10000
        
        # Test with minimal content
        minimal_response = TemplateGenerationResponse(
            system_template="x",
            prompt_template="y", 
            response_template="z"
        )
        assert minimal_response.system_template == "x"
        
        # Test with None values should fail validation
        with pytest.raises(ValidationError):
            TemplateGenerationRequest(
                role=None,
                goal="test",
                backstory="test"
            )
            
        with pytest.raises(ValidationError):
            TemplateGenerationResponse(
                system_template=None,
                prompt_template="test",
                response_template="test"
            )