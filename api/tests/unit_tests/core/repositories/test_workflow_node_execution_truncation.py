"""
Unit tests for WorkflowNodeExecution truncation functionality.

Tests the truncation and offloading logic for large inputs and outputs
in the SQLAlchemyWorkflowNodeExecutionRepository.
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.repositories.sqlalchemy_workflow_node_execution_repository import (
    TRUNCATION_SIZE_THRESHOLD,
    SQLAlchemyWorkflowNodeExecutionRepository,
)
from core.workflow.entities.workflow_node_execution import (
    WorkflowNodeExecution,
    WorkflowNodeExecutionStatus,
)
from core.workflow.nodes.enums import NodeType
from models import Account, CreatorUserRole, WorkflowNodeExecutionTriggeredFrom
from models.workflow import WorkflowNodeExecutionModel, WorkflowNodeExecutionOffload
from services.variable_truncator import VariableTruncator


@dataclass
class TruncationTestCase:
    """Test case data for truncation scenarios."""

    name: str
    inputs: dict[str, Any] | None
    outputs: dict[str, Any] | None
    should_truncate_inputs: bool
    should_truncate_outputs: bool
    description: str


def create_test_cases() -> list[TruncationTestCase]:
    """Create test cases for different truncation scenarios."""
    # Create large data that will definitely exceed the threshold (10KB)
    large_data = {"data": "x" * (TRUNCATION_SIZE_THRESHOLD + 1000)}
    small_data = {"data": "small"}

    return [
        TruncationTestCase(
            name="small_data_no_truncation",
            inputs=small_data,
            outputs=small_data,
            should_truncate_inputs=False,
            should_truncate_outputs=False,
            description="Small data should not be truncated",
        ),
        TruncationTestCase(
            name="large_inputs_truncation",
            inputs=large_data,
            outputs=small_data,
            should_truncate_inputs=True,
            should_truncate_outputs=False,
            description="Large inputs should be truncated",
        ),
        TruncationTestCase(
            name="large_outputs_truncation",
            inputs=small_data,
            outputs=large_data,
            should_truncate_inputs=False,
            should_truncate_outputs=True,
            description="Large outputs should be truncated",
        ),
        TruncationTestCase(
            name="large_both_truncation",
            inputs=large_data,
            outputs=large_data,
            should_truncate_inputs=True,
            should_truncate_outputs=True,
            description="Both large inputs and outputs should be truncated",
        ),
        TruncationTestCase(
            name="none_inputs_outputs",
            inputs=None,
            outputs=None,
            should_truncate_inputs=False,
            should_truncate_outputs=False,
            description="None inputs and outputs should not be truncated",
        ),
    ]


def create_workflow_node_execution(
    execution_id: str = "test-execution-id",
    inputs: dict[str, Any] | None = None,
    outputs: dict[str, Any] | None = None,
) -> WorkflowNodeExecution:
    """Factory function to create a WorkflowNodeExecution for testing."""
    return WorkflowNodeExecution(
        id=execution_id,
        node_execution_id="test-node-execution-id",
        workflow_id="test-workflow-id",
        workflow_execution_id="test-workflow-execution-id",
        index=1,
        node_id="test-node-id",
        node_type=NodeType.LLM,
        title="Test Node",
        inputs=inputs,
        outputs=outputs,
        status=WorkflowNodeExecutionStatus.SUCCEEDED,
        created_at=datetime.now(UTC),
    )


def create_mock_user() -> Account:
    """Create a mock Account user for testing."""
    from unittest.mock import MagicMock

    user = MagicMock(spec=Account)
    user.id = "test-user-id"
    user.current_tenant_id = "test-tenant-id"
    return user


class TestSQLAlchemyWorkflowNodeExecutionRepositoryTruncation:
    """Test class for truncation functionality in SQLAlchemyWorkflowNodeExecutionRepository."""

    def setup_method(self):
        """Set up test environment before each test method."""
        # Create in-memory SQLite database for testing
        self.engine = create_engine("sqlite:///:memory:")
        self.session_maker = sessionmaker(bind=self.engine)

        # Create mock user
        self.user = create_mock_user()

    def create_repository(self) -> SQLAlchemyWorkflowNodeExecutionRepository:
        """Create a repository instance for testing."""
        return SQLAlchemyWorkflowNodeExecutionRepository(
            session_factory=self.session_maker,
            user=self.user,
            app_id="test-app-id",
            triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
        )

    def test_truncator_initialization(self):
        """Test that VariableTruncator is correctly initialized."""
        repo = self.create_repository()

        assert hasattr(repo, "_truncator")
        assert isinstance(repo._truncator, VariableTruncator)

    def test_safe_truncate_integration(self):
        """Test our safe truncation wrapper method."""
        repo = self.create_repository()

        # Test small data that doesn't need truncation
        small_data = {"key": "value"}
        result, was_truncated = repo._safe_truncate_inputs_outputs(small_data)
        assert not was_truncated
        assert result == small_data

        # Test large data that needs truncation
        large_data = {"data": "x" * (TRUNCATION_SIZE_THRESHOLD + 1000)}
        result, was_truncated = repo._safe_truncate_inputs_outputs(large_data)
        assert was_truncated
        assert result != large_data
        assert "__truncated__" in result

    @pytest.mark.parametrize("test_case", create_test_cases())
    @patch("core.repositories.sqlalchemy_workflow_node_execution_repository.FileService")
    def test_to_db_model_truncation(self, mock_file_service_class, test_case: TruncationTestCase):
        """Test the to_db_model method handles truncation correctly."""
        # Setup mock file service
        mock_file_service = MagicMock()
        mock_upload_file = MagicMock()
        mock_upload_file.id = "mock-file-id"
        mock_file_service.upload_file.return_value = mock_upload_file
        mock_file_service_class.return_value = mock_file_service

        repo = self.create_repository()
        execution = create_workflow_node_execution(
            inputs=test_case.inputs,
            outputs=test_case.outputs,
        )

        db_model, offload_record = repo._to_db_model(execution)

        # Check if offload record was created when expected
        if test_case.should_truncate_inputs or test_case.should_truncate_outputs:
            assert offload_record is not None
            if test_case.should_truncate_inputs:
                assert offload_record.inputs_file_id == "mock-file-id"
            else:
                assert offload_record.inputs_file_id is None
            if test_case.should_truncate_outputs:
                assert offload_record.outputs_file_id == "mock-file-id"
            else:
                assert offload_record.outputs_file_id is None
        else:
            assert offload_record is None

        # Check if file service was called when expected
        expected_calls = 0
        if test_case.should_truncate_inputs:
            expected_calls += 1
        if test_case.should_truncate_outputs:
            expected_calls += 1

        assert mock_file_service.upload_file.call_count == expected_calls

    def test_to_db_model_sets_truncated_data(self):
        """Test that to_db_model sets truncated data in the domain model."""
        large_data = {"data": "x" * (TRUNCATION_SIZE_THRESHOLD + 1)}

        with patch(
            "core.repositories.sqlalchemy_workflow_node_execution_repository.FileService"
        ) as mock_file_service_class:
            mock_file_service = MagicMock()
            mock_upload_file = MagicMock()
            mock_upload_file.id = "mock-file-id"
            mock_file_service.upload_file.return_value = mock_upload_file
            mock_file_service_class.return_value = mock_file_service

            repo = self.create_repository()
            execution = create_workflow_node_execution(
                inputs=large_data,
                outputs=large_data,
            )

            db_model, offload_record = repo._to_db_model(execution)

            # Check that truncated data was set in the domain model
            assert execution.get_truncated_inputs() is not None
            assert execution.get_truncated_outputs() is not None

    def test_to_domain_model_with_offload_data(self):
        """Test _to_domain_model correctly handles models with offload data."""
        repo = self.create_repository()

        # Create a mock database model with offload data
        db_model = MagicMock()
        db_model.id = "test-id"
        db_model.node_execution_id = "node-exec-id"
        db_model.workflow_id = "workflow-id"
        db_model.workflow_run_id = "run-id"
        db_model.index = 1
        db_model.predecessor_node_id = None
        db_model.node_id = "node-id"
        db_model.node_type = NodeType.LLM.value
        db_model.title = "Test Node"
        db_model.inputs_dict = {"truncated": True}
        db_model.process_data_dict = None
        db_model.outputs_dict = {"truncated": True}
        db_model.status = WorkflowNodeExecutionStatus.SUCCEEDED.value
        db_model.error = None
        db_model.elapsed_time = 1.0
        db_model.execution_metadata_dict = {}
        db_model.created_at = datetime.now(UTC)
        db_model.finished_at = None

        # Mock offload data
        offload_data = MagicMock()
        offload_data.inputs_file_id = "inputs-file-id"
        offload_data.outputs_file_id = "outputs-file-id"
        db_model.offload_data = offload_data

        domain_model = repo._to_domain_model(db_model)

        # Check that truncated data was set correctly
        assert domain_model.get_truncated_inputs() == {"truncated": True}
        assert domain_model.get_truncated_outputs() == {"truncated": True}

    def test_to_domain_model_without_offload_data(self):
        """Test _to_domain_model correctly handles models without offload data."""
        repo = self.create_repository()

        # Create a mock database model without offload data
        db_model = MagicMock()
        db_model.id = "test-id"
        db_model.node_execution_id = "node-exec-id"
        db_model.workflow_id = "workflow-id"
        db_model.workflow_run_id = "run-id"
        db_model.index = 1
        db_model.predecessor_node_id = None
        db_model.node_id = "node-id"
        db_model.node_type = NodeType.LLM.value
        db_model.title = "Test Node"
        db_model.inputs_dict = {"normal": True}
        db_model.process_data_dict = None
        db_model.outputs_dict = {"normal": True}
        db_model.status = WorkflowNodeExecutionStatus.SUCCEEDED.value
        db_model.error = None
        db_model.elapsed_time = 1.0
        db_model.execution_metadata_dict = {}
        db_model.created_at = datetime.now(UTC)
        db_model.finished_at = None
        db_model.offload_data = None

        domain_model = repo._to_domain_model(db_model)

        # Check that no truncated data was set
        assert domain_model.get_truncated_inputs() is None
        assert domain_model.get_truncated_outputs() is None

    @patch("core.repositories.sqlalchemy_workflow_node_execution_repository.FileService")
    def test_save_with_truncation(self, mock_file_service_class):
        """Test the save method handles truncation and offload record creation."""
        # Setup mock file service
        mock_file_service = MagicMock()
        mock_upload_file = MagicMock()
        mock_upload_file.id = "mock-file-id"
        mock_file_service.upload_file.return_value = mock_upload_file
        mock_file_service_class.return_value = mock_file_service

        large_data = {"data": "x" * (TRUNCATION_SIZE_THRESHOLD + 1)}

        repo = self.create_repository()
        execution = create_workflow_node_execution(
            inputs=large_data,
            outputs=large_data,
        )

        # Mock the session and database operations
        with patch.object(repo, "_session_factory") as mock_session_factory:
            mock_session = MagicMock()
            mock_session_factory.return_value.__enter__.return_value = mock_session

            repo.save(execution)

            # Check that both merge operations were called (db_model and offload_record)
            assert mock_session.merge.call_count == 2
            mock_session.commit.assert_called_once()

    def test_repository_initialization_with_different_user_types(self):
        """Test repository initialization with different user types."""
        # Test with Account
        account_user = create_mock_user()
        repo = SQLAlchemyWorkflowNodeExecutionRepository(
            session_factory=self.session_maker,
            user=account_user,
            app_id="test-app-id",
            triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
        )
        assert repo._creator_user_role == CreatorUserRole.ACCOUNT

        # Test with EndUser
        from models.model import EndUser

        end_user = EndUser()
        end_user.id = "test-end-user-id"
        end_user.tenant_id = "test-tenant-id"

        repo = SQLAlchemyWorkflowNodeExecutionRepository(
            session_factory=self.session_maker,
            user=end_user,
            app_id="test-app-id",
            triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
        )
        assert repo._creator_user_role == CreatorUserRole.END_USER


class TestWorkflowNodeExecutionModelTruncatedProperties:
    """Test the truncated properties on WorkflowNodeExecutionModel."""

    def test_inputs_truncated_with_offload_data(self):
        """Test inputs_truncated property when offload data exists."""
        model = WorkflowNodeExecutionModel()

        # Mock offload data with inputs file
        offload_data = MagicMock()
        offload_data.inputs_file_id = "file-id"
        offload_data.outputs_file_id = None
        model.offload_data = offload_data

        assert model.inputs_truncated is True
        assert model.outputs_truncated is False

    def test_outputs_truncated_with_offload_data(self):
        """Test outputs_truncated property when offload data exists."""
        model = WorkflowNodeExecutionModel()

        # Mock offload data with outputs file
        offload_data = MagicMock()
        offload_data.inputs_file_id = None
        offload_data.outputs_file_id = "file-id"
        model.offload_data = offload_data

        assert model.inputs_truncated is False
        assert model.outputs_truncated is True

    def test_truncated_properties_without_offload_data(self):
        """Test truncated properties when no offload data exists."""
        model = WorkflowNodeExecutionModel()
        model.offload_data = None

        assert model.inputs_truncated is False
        assert model.outputs_truncated is False

    def test_truncated_properties_without_offload_attribute(self):
        """Test truncated properties when offload_data attribute doesn't exist."""
        model = WorkflowNodeExecutionModel()
        # Don't set offload_data attribute at all

        assert model.inputs_truncated is False
        assert model.outputs_truncated is False
