import pytest
from abc import ABC
from unittest.mock import MagicMock

from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail


class TestBaseGuardrail:
    """Test suite for BaseGuardrail abstract class."""
    
    def test_is_abstract_class(self):
        """Test that BaseGuardrail is an abstract class."""
        assert issubclass(BaseGuardrail, ABC)
    
    def test_cannot_instantiate_directly(self):
        """Test that BaseGuardrail cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class BaseGuardrail"):
            BaseGuardrail({})
    
    def test_concrete_implementation_required(self):
        """Test that concrete implementation must implement validate method."""
        class IncompleteGuardrail(BaseGuardrail):
            pass
        
        with pytest.raises(TypeError, match="Can't instantiate abstract class IncompleteGuardrail"):
            IncompleteGuardrail({})
    
    def test_concrete_implementation_success(self):
        """Test successful concrete implementation."""
        class ConcreteGuardrail(BaseGuardrail):
            def validate(self, output: str):
                return {"valid": True, "feedback": ""}
        
        config = {"test": "config"}
        guardrail = ConcreteGuardrail(config)
        
        assert guardrail.config == config
        assert hasattr(guardrail, 'validate')
        assert callable(guardrail.validate)
    
    def test_init_stores_config(self):
        """Test that initialization stores configuration."""
        class TestGuardrail(BaseGuardrail):
            def validate(self, output: str):
                return {"valid": True, "feedback": ""}
        
        config = {"field": "companies", "min_count": 5}
        guardrail = TestGuardrail(config)
        
        assert guardrail.config == config
    
    def test_init_with_empty_config(self):
        """Test initialization with empty configuration."""
        class TestGuardrail(BaseGuardrail):
            def validate(self, output: str):
                return {"valid": True, "feedback": ""}
        
        config = {}
        guardrail = TestGuardrail(config)
        
        assert guardrail.config == {}
    
    def test_init_with_complex_config(self):
        """Test initialization with complex configuration."""
        class TestGuardrail(BaseGuardrail):
            def validate(self, output: str):
                return {"valid": True, "feedback": ""}
        
        config = {
            "field": "companies",
            "min_count": 5,
            "nested": {
                "validation": True,
                "rules": ["rule1", "rule2"]
            },
            "callbacks": [1, 2, 3]
        }
        guardrail = TestGuardrail(config)
        
        assert guardrail.config == config
        assert guardrail.config["field"] == "companies"
        assert guardrail.config["nested"]["validation"] is True
    
    def test_validate_method_signature(self):
        """Test that validate method has correct signature."""
        class TestGuardrail(BaseGuardrail):
            def validate(self, output: str):
                return {"valid": True, "feedback": "Test feedback"}
        
        guardrail = TestGuardrail({})
        result = guardrail.validate("test output")
        
        assert isinstance(result, dict)
        assert "valid" in result
        assert "feedback" in result
        assert result["valid"] is True
        assert result["feedback"] == "Test feedback"
    
    def test_validate_method_with_different_return_types(self):
        """Test validate method can return different result types."""
        class ValidGuardrail(BaseGuardrail):
            def validate(self, output: str):
                if output == "valid":
                    return {"valid": True, "feedback": ""}
                else:
                    return {"valid": False, "feedback": "Invalid output"}
        
        guardrail = ValidGuardrail({})
        
        # Test valid case
        result1 = guardrail.validate("valid")
        assert result1["valid"] is True
        assert result1["feedback"] == ""
        
        # Test invalid case
        result2 = guardrail.validate("invalid")
        assert result2["valid"] is False
        assert result2["feedback"] == "Invalid output"
    
    def test_multiple_concrete_implementations(self):
        """Test multiple concrete implementations can coexist."""
        class Guardrail1(BaseGuardrail):
            def validate(self, output: str):
                return {"valid": True, "feedback": "Guardrail1"}
        
        class Guardrail2(BaseGuardrail):
            def validate(self, output: str):
                return {"valid": False, "feedback": "Guardrail2"}
        
        config1 = {"type": "guardrail1"}
        config2 = {"type": "guardrail2"}
        
        g1 = Guardrail1(config1)
        g2 = Guardrail2(config2)
        
        assert g1.config["type"] == "guardrail1"
        assert g2.config["type"] == "guardrail2"
        
        result1 = g1.validate("test")
        result2 = g2.validate("test")
        
        assert result1["valid"] is True
        assert result1["feedback"] == "Guardrail1"
        assert result2["valid"] is False
        assert result2["feedback"] == "Guardrail2"
    
    def test_inheritance_chain(self):
        """Test inheritance chain works correctly."""
        class MiddleGuardrail(BaseGuardrail):
            def __init__(self, config):
                super().__init__(config)
                self.middle_attribute = "middle"
        
        class ConcreteGuardrail(MiddleGuardrail):
            def __init__(self, config):
                super().__init__(config)
                self.concrete_attribute = "concrete"
            
            def validate(self, output: str):
                return {"valid": True, "feedback": "Inherited"}
        
        config = {"test": "inheritance"}
        guardrail = ConcreteGuardrail(config)
        
        assert guardrail.config == config
        assert guardrail.middle_attribute == "middle"
        assert guardrail.concrete_attribute == "concrete"
        
        result = guardrail.validate("test")
        assert result["valid"] is True
        assert result["feedback"] == "Inherited"
    
    def test_config_mutability(self):
        """Test that config can be modified after initialization."""
        class TestGuardrail(BaseGuardrail):
            def validate(self, output: str):
                return {"valid": True, "feedback": ""}
        
        config = {"initial": "value"}
        guardrail = TestGuardrail(config)
        
        # Modify config
        guardrail.config["new_key"] = "new_value"
        guardrail.config["initial"] = "modified"
        
        assert guardrail.config["initial"] == "modified"
        assert guardrail.config["new_key"] == "new_value"
    
    def test_abstract_method_enforcement(self):
        """Test that abstract method must be implemented."""
        class AlmostCompleteGuardrail(BaseGuardrail):
            def some_other_method(self):
                pass
            # Missing validate method
        
        with pytest.raises(TypeError):
            AlmostCompleteGuardrail({})
    
    def test_validate_method_can_access_config(self):
        """Test that validate method can access configuration."""
        class ConfigAccessingGuardrail(BaseGuardrail):
            def validate(self, output: str):
                threshold = self.config.get("threshold", 0)
                if len(output) > threshold:
                    return {"valid": True, "feedback": ""}
                else:
                    return {"valid": False, "feedback": f"Output too short, minimum {threshold}"}
        
        config = {"threshold": 10}
        guardrail = ConfigAccessingGuardrail(config)
        
        # Test short output
        result1 = guardrail.validate("short")
        assert result1["valid"] is False
        assert "minimum 10" in result1["feedback"]
        
        # Test long output
        result2 = guardrail.validate("this is a long enough output")
        assert result2["valid"] is True
        assert result2["feedback"] == ""
    
    def test_abstract_method_pass_statement(self):
        """Test that the abstract validate method contains the pass statement."""
        # This test covers line 40 which is the pass statement in the abstract method
        class TestGuardrail(BaseGuardrail):
            def validate(self, output: str):
                # Call the parent method to cover the pass statement
                super().validate(output)
                return {"valid": True, "feedback": ""}
        
        guardrail = TestGuardrail({})
        # The abstract method will be called and execute the pass statement
        result = guardrail.validate("test")
        assert result["valid"] is True