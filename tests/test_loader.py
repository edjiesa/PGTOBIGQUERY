import json
import pytest
from app.config import MigrationConfig
from app.loader import BigQueryLoader


def test_loader_diagnostic_invalid_json():
    config = MigrationConfig(
        GCP_SA_KEY_JSON="invalid-json-string"
    )
    loader = BigQueryLoader(config)
    res = loader.test_connection()
    assert res["status"] == "failed"
    assert res["message"] == "Failed Service Account Key Parsing"
    assert res["checklist"][0]["status"] == "failed"


def test_loader_diagnostic_valid_sa_format():
    mock_sa = {
        "type": "service_account",
        "project_id": "test-gcp-project",
        "private_key_id": "keyid123",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC...\n-----END PRIVATE KEY-----\n",
        "client_email": "test-sa@test-gcp-project.iam.gserviceaccount.com",
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    config = MigrationConfig(
        GCP_SA_KEY_JSON=json.dumps(mock_sa)
    )
    loader = BigQueryLoader(config)
    res = loader.test_connection()
    # Step 1 should pass formatting check
    assert res["checklist"][0]["status"] == "success"
    assert "test-sa@test-gcp-project.iam.gserviceaccount.com" in res["checklist"][0]["detail"]
