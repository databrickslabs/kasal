"""
Unit tests for src.engines.crewai.security.secret_leak_detector.
"""
import pytest
from src.engines.crewai.security.secret_leak_detector import detect, SecretLeakResult


class TestSecretLeakDetectorCleanInputs:
    def test_empty_string_returns_not_detected(self):
        result = detect("")
        assert result.detected is False
        assert result.secret_types == []

    def test_none_like_empty_returns_not_detected(self):
        # detect() accepts str; empty string is the edge case
        result = detect("")
        assert not result.detected

    def test_clean_text_returns_not_detected(self):
        result = detect("The Q4 revenue was $1.2 million, up 10% year-over-year.")
        assert result.detected is False

    def test_normal_code_no_secrets(self):
        result = detect("def hello():\n    print('world')")
        assert not result.detected


class TestDatabricksPAT:
    def test_detects_databricks_pat(self):
        pat = "dapi" + "a" * 32
        result = detect(f"Token: {pat}")
        assert result.detected
        assert "databricks_pat" in result.secret_types

    def test_detects_databricks_pat_uppercase(self):
        pat = "DAPI" + "b" * 32
        result = detect(pat)
        assert result.detected
        assert "databricks_pat" in result.secret_types

    def test_short_dapi_not_detected(self):
        result = detect("dapi1234")  # too short
        assert "databricks_pat" not in (result.secret_types or [])


class TestDatabricksEnvToken:
    def test_detects_databricks_token_env_var(self):
        result = detect("DATABRICKS_TOKEN=dapi12345678901234567890")
        assert result.detected
        assert "databricks_env_token" in result.secret_types

    def test_detects_databricks_token_with_colon(self):
        result = detect("DATABRICKS_TOKEN: some_long_value_here")
        assert result.detected
        assert "databricks_env_token" in result.secret_types

    def test_short_value_not_detected(self):
        result = detect("DATABRICKS_TOKEN=short")  # < 10 chars
        assert "databricks_env_token" not in (result.secret_types or [])


class TestAWSAccessKey:
    def test_detects_aws_access_key(self):
        result = detect("key=AKIAIOSFODNN7EXAMPLE")  # gitleaks:allow
        assert result.detected
        assert "aws_access_key" in result.secret_types

    def test_lowercase_akia_not_detected(self):
        result = detect("akia1234567890123456")  # lowercase — pattern requires uppercase
        assert "aws_access_key" not in (result.secret_types or [])


class TestSlackToken:
    def test_detects_slack_bot_token(self):
        result = detect("token: xoxb-FAKETESTABC")  # gitleaks:allow
        assert result.detected
        assert "slack_token" in result.secret_types

    def test_detects_slack_app_token(self):
        result = detect("token: xoxa-FAKETESTABC")  # gitleaks:allow
        assert result.detected

    def test_detects_slack_refresh_token(self):
        result = detect("token: xoxr-FAKETESTABC")  # gitleaks:allow
        assert result.detected


class TestPrivateKey:
    def test_detects_rsa_private_key_header(self):
        result = detect("-----BEGIN RSA PRIVATE KEY-----\nMIIE...")  # gitleaks:allow
        assert result.detected
        assert "private_key" in result.secret_types

    def test_detects_generic_private_key_header(self):
        result = detect("-----BEGIN PRIVATE KEY-----\nMIIE...")
        assert result.detected

    def test_detects_openssh_private_key_header(self):
        result = detect("-----BEGIN OPENSSH PRIVATE KEY-----\nb3Bl...")
        assert result.detected

    def test_detects_ec_private_key_header(self):
        result = detect("-----BEGIN EC PRIVATE KEY-----\nMHQC...")
        assert result.detected

    def test_detects_dsa_private_key_header(self):
        result = detect("-----BEGIN DSA PRIVATE KEY-----\nMIIB...")
        assert result.detected

    def test_detects_encrypted_private_key_header(self):
        result = detect("-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIE...")
        assert result.detected


class TestGitHubToken:
    def test_detects_github_personal_access_token(self):
        result = detect("ghp_ABCDEFghijklmnopqrstuvwx")
        assert result.detected
        assert "github_token" in result.secret_types

    def test_detects_github_oauth_token(self):
        result = detect("gho_ABCDEFghijklmnopqrstuvwx")
        assert result.detected
        assert "github_token" in result.secret_types

    def test_detects_github_app_install_token(self):
        result = detect("ghs_ABCDEFghijklmnopqrstuvwx")
        assert result.detected
        assert "github_token" in result.secret_types

    def test_detects_github_fine_grained_pat(self):
        result = detect("github_pat_ABCDEFghijklmnopqrstuvwx")
        assert result.detected
        assert "github_token" in result.secret_types

    def test_short_ghp_not_detected(self):
        result = detect("ghp_short")  # < 20 chars after prefix
        assert "github_token" not in (result.secret_types or [])


class TestGCPServiceAccount:
    def test_detects_gcp_service_account_json(self):
        result = detect('{"type": "service_account", "project_id": "my-project"}')
        assert result.detected
        assert "gcp_service_account" in result.secret_types

    def test_no_false_positive_other_type(self):
        result = detect('{"type": "authorized_user"}')
        assert "gcp_service_account" not in (result.secret_types or [])


class TestAzureConnectionString:
    def test_detects_azure_account_key(self):
        key = "A" * 44 + "=="  # 46 chars, base64-like
        result = detect(f"AccountKey={key}")
        assert result.detected
        assert "azure_connection_string" in result.secret_types

    def test_short_azure_key_not_detected(self):
        result = detect("AccountKey=shortkey")  # < 40 chars
        assert "azure_connection_string" not in (result.secret_types or [])


class TestGenericAPIKey:
    def test_detects_api_key_assignment(self):
        result = detect('api_key = "abcdefghijklmnopqrstuvwxyz1234"')
        assert result.detected
        assert "generic_api_key" in result.secret_types

    def test_detects_secret_key_assignment(self):
        result = detect("secret_key: abcdefghijklmnopqrstuvwxyz1234")
        assert result.detected

    def test_detects_auth_token_assignment(self):
        result = detect('auth_token = "abcdefghijklmnopqrstuvwxyz1234"')
        assert result.detected

    def test_short_value_not_detected(self):
        # Value must be >=24 chars to trigger the tightened generic pattern
        result = detect("api_key=short")
        assert "generic_api_key" not in (result.secret_types or [])

    def test_all_numeric_value_not_detected(self):
        # Tightened pattern requires first char to be alpha
        result = detect("api_key=123456789012345678901234")
        assert "generic_api_key" not in (result.secret_types or [])


class TestMultipleSecrets:
    def test_detects_multiple_secret_types(self):
        text = (
            "dapi" + "a" * 32 + "\n"
            "AKIAIOSFODNN7EXAMPLE\n"  # gitleaks:allow
        )
        result = detect(text)
        assert result.detected
        assert "databricks_pat" in result.secret_types
        assert "aws_access_key" in result.secret_types


class TestReturnType:
    def test_returns_secret_leak_result_instance(self):
        result = detect("hello")
        assert isinstance(result, SecretLeakResult)

    def test_not_detected_has_empty_secret_types(self):
        result = detect("nothing suspicious here")
        assert result.secret_types == []
