import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import (
    BadRequestError,
    ConflictError,
    ForbiddenError,
    KasalError,
    NotFoundError,
)
from src.models.flow import Flow
from src.repositories.flow_repository import FlowRepository
from src.schemas.flow import FlowCreate, FlowResponse, FlowUpdate

logger = logging.getLogger(__name__)


class FlowService:
    """
    Service for Flow model with business logic.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the service with a database session.

        Args:
            session: Database session for operations
        """
        self.session = session

    async def create_flow_with_group(self, flow_in: FlowCreate, group_context) -> Flow:
        """
        Create a new flow with group isolation.

        Args:
            flow_in: Flow data for creation
            group_context: Group context with group_ids and email

        Returns:
            Created flow with group_id set
        """
        try:
            # Check for duplicate name within the group
            if group_context and group_context.group_ids:
                repository = FlowRepository(self.session)
                existing = await repository.find_by_name_and_group(
                    flow_in.name, group_context.group_ids
                )
                if existing:
                    raise ConflictError(
                        detail=f"A flow with the name '{flow_in.name}' already exists. Please choose a different name."
                    )

            # Log details for debugging
            logger.info(f"Creating flow with group isolation: {flow_in.name}")
            logger.info(
                f"Group context: {group_context.group_ids if group_context else 'None'}"
            )

            # Validate and normalize flow configuration
            flow_config = flow_in.flow_config or {}

            # Generate a new UUID for the flow
            flow_uuid = str(uuid.uuid4())

            # Ensure required fields exist in flow_config
            flow_config = {
                "id": flow_uuid,
                "name": flow_config.get("name", flow_in.name),
                "type": flow_config.get("type", "default"),
                "listeners": flow_config.get("listeners", []),
                "actions": flow_config.get("actions", []),
                "startingPoints": flow_config.get("startingPoints", []),
            }

            # Validate listeners (allow empty for visual editor)
            for listener in flow_config.get("listeners", []):
                if not isinstance(listener, dict):
                    raise ValueError(f"Invalid listener format: {listener}")

            # Validate actions (allow empty for visual editor)
            for action in flow_config.get("actions", []):
                if not isinstance(action, dict):
                    raise ValueError(f"Invalid action format: {action}")

            # Validate crew_id if provided - for multi-crew flows, crew_id may not exist
            # Set to None if the crew doesn't exist to avoid FK constraint violations
            validated_crew_id = None
            if flow_in.crew_id:
                # Crews are CrewService's domain.
                from src.services.catalog.crews import CrewService

                existing_crew = await CrewService(self.session).get(flow_in.crew_id)
                if existing_crew:
                    validated_crew_id = flow_in.crew_id
                    logger.info(f"Validated crew_id: {validated_crew_id}")
                else:
                    logger.warning(
                        f"crew_id {flow_in.crew_id} not found in database, setting to None for multi-crew flow"
                    )

            # Create flow dictionary with validated data and group info
            flow_dict = {
                "name": flow_in.name,
                "crew_id": validated_crew_id,
                "nodes": [node.model_dump() for node in flow_in.nodes],
                "edges": [edge.model_dump() for edge in flow_in.edges],
                "flow_config": flow_config,
                # Add group isolation fields
                "group_id": (
                    group_context.group_ids[0]
                    if group_context and group_context.group_ids
                    else None
                ),
                "created_by_email": (
                    group_context.group_email if group_context else None
                ),
            }

            repository = FlowRepository(self.session)
            flow = await repository.create(flow_dict)

            logger.info(
                f"Successfully created flow with ID: {flow.id}, group: {flow.group_id}"
            )
            return flow

        except ValueError as ve:
            logger.error(f"Validation error while creating flow: {str(ve)}")
            raise BadRequestError(detail=str(ve))
        except KasalError:
            raise
        except Exception as e:
            logger.error(f"Error creating flow: {str(e)}")
            raise KasalError(detail=f"Error creating flow: {str(e)}")

    async def get_flow(self, flow_id: uuid.UUID) -> Flow:
        """
        Get a flow by ID.

        Args:
            flow_id: UUID of the flow to get

        Returns:
            Flow if found, else raises HTTPException
        """
        repository = FlowRepository(self.session)
        flow = await repository.get(flow_id)

        if not flow:
            raise NotFoundError(detail="Flow not found")

        return flow

    async def get_flow_with_group_check(
        self, flow_id: uuid.UUID, group_context
    ) -> Flow:
        """
        Get a flow by ID with group authorization check.

        Args:
            flow_id: UUID of the flow to get
            group_context: Group context with group_ids list

        Returns:
            Flow if found and user has access

        Raises:
            HTTPException: If flow not found or user doesn't have access
        """
        repository = FlowRepository(self.session)
        flow = await repository.get(flow_id)

        if not flow:
            raise NotFoundError(detail="Flow not found")

        # Check if user has access to this flow's group
        if flow.group_id and group_context and group_context.group_ids:
            if flow.group_id not in group_context.group_ids:
                raise ForbiddenError(detail="Access denied to this flow")

        return flow

    async def get_all_flows_for_group(self, group_context) -> List[Flow]:
        """
        Get all flows for the user's groups.

        Args:
            group_context: Group context with group_ids list

        Returns:
            List of flows belonging to user's groups
        """
        if not group_context or not group_context.group_ids:
            return []
        return await FlowRepository(self.session).list_for_groups(
            group_context.group_ids
        )

    async def update_flow_with_group_check(
        self, flow_id: uuid.UUID, flow_in: FlowUpdate, group_context
    ) -> Flow:
        """
        Update a flow with group authorization check and name uniqueness validation.

        Args:
            flow_id: UUID of the flow to update
            flow_in: Flow data for update
            group_context: Group context with group_ids list

        Returns:
            Updated flow

        Raises:
            NotFoundError: If flow not found
            ForbiddenError: If user doesn't have access
            ConflictError: If a flow with the same name already exists in the group
        """
        repository = FlowRepository(self.session)
        flow = await repository.get(flow_id)

        if not flow:
            raise NotFoundError(detail="Flow not found")

        # Check if user has access to this flow's group
        if flow.group_id and group_context and group_context.group_ids:
            if flow.group_id not in group_context.group_ids:
                raise ForbiddenError(detail="Access denied to this flow")

        # Check for duplicate name within the group (if name is being changed)
        if (
            flow_in.name
            and flow_in.name != flow.name
            and group_context
            and group_context.group_ids
        ):
            duplicate = await repository.find_by_name_and_group(
                flow_in.name, group_context.group_ids, exclude_id=flow_id
            )
            if duplicate:
                raise ConflictError(
                    detail=f"A flow with the name '{flow_in.name}' already exists. Please choose a different name."
                )

        # Delegate to the existing update_flow method
        return await self.update_flow(flow_id, flow_in)

    async def delete_all_flows_for_group(self, group_context) -> None:
        """
        Delete all flows for the user's groups.

        Args:
            group_context: Group context with group_ids list
        """
        if not group_context or not group_context.group_ids:
            return

        from sqlalchemy import delete as sql_delete
        from sqlalchemy import or_, select

        # First delete execution history for these flows
        flows = await self.get_all_flows_for_group(group_context)
        for flow in flows:
            try:
                await self.force_delete_flow_with_executions(flow.id)
            except Exception as e:
                logger.error(f"Error deleting flow {flow.id}: {e}")

    async def find_flow(self, flow_id: uuid.UUID) -> Optional[Flow]:
        """One flow by id, or None.

        Distinct from :meth:`get_flow`, which RAISES when missing. The crew/flow
        runner needs the None so it can return a clean error payload to a caller
        that is not an HTTP request — it used to build ``FlowRepository`` for that.
        """
        return await FlowRepository(self.session).get(flow_id)

    async def get_flows_by_ids(
        self, flow_ids: List[Union[uuid.UUID, str]]
    ) -> List[Flow]:
        """Flows for a set of ids — for callers rendering a list of references.

        Flows are this service's domain; `publications` used to build
        ``FlowRepository`` itself to resolve the flows it advertises.
        """
        return await FlowRepository(self.session).find_by_ids(flow_ids)

    async def get_most_recent_flow(self) -> Optional[Flow]:
        """The most recently authored flow, or None.

        A fallback for a run that arrived without a flow id. Unscoped by design —
        it exists for a single-tenant/dev path — so do NOT use it to resolve a
        flow on behalf of a tenant.
        """
        return await FlowRepository(self.session).get_most_recent()

    async def get_flows_by_crew(self, crew_id: Union[uuid.UUID, str]) -> List[Flow]:
        """
        Get all flows for a specific crew.

        Args:
            crew_id: ID of the crew (UUID)

        Returns:
            List of flows for the crew
        """
        # Convert string to UUID if needed
        if isinstance(crew_id, str):
            try:
                crew_id = uuid.UUID(crew_id)
            except ValueError:
                # Return empty list if the UUID is invalid
                return []

        repository = FlowRepository(self.session)
        return await repository.find_by_crew_id(crew_id)

    async def update_flow(self, flow_id: uuid.UUID, flow_in: FlowUpdate) -> Flow:
        """
        Update a flow.

        Args:
            flow_id: UUID of the flow to update
            flow_in: Flow data for update

        Returns:
            Updated flow if found, else raises HTTPException
        """
        try:
            repository = FlowRepository(self.session)
            flow = await repository.get(flow_id)

            if not flow:
                raise NotFoundError(detail="Flow not found")

            # Log the incoming flow data for debugging
            logger.info(f"Updating flow {flow_id} with name: {flow_in.name}")

            # Process flow_config if provided
            if flow_in.flow_config is not None:
                logger.info(f"Flow config provided: {type(flow_in.flow_config)}")

                # Check for actions specifically
                if "actions" not in flow_in.flow_config:
                    logger.info("Adding empty actions array to flow_config")
                    flow_in.flow_config["actions"] = []

            # Create update data
            update_data = {"name": flow_in.name, "updated_at": datetime.now()}

            if flow_in.flow_config is not None:
                update_data["flow_config"] = flow_in.flow_config

            # Update nodes if provided
            if flow_in.nodes is not None:
                logger.info(f"Updating flow nodes: {len(flow_in.nodes)} nodes")
                update_data["nodes"] = [node.model_dump() for node in flow_in.nodes]

            # Update edges if provided
            if flow_in.edges is not None:
                logger.info(f"Updating flow edges: {len(flow_in.edges)} edges")
                update_data["edges"] = [edge.model_dump() for edge in flow_in.edges]

            # Update the flow
            updated_flow = await repository.update(flow_id, update_data)
            return updated_flow
        except KasalError:
            raise
        except Exception as e:
            logger.error(f"Error updating flow: {str(e)}")
            raise KasalError(detail=f"Error updating flow: {str(e)}")

    async def _delete_execution_children(
        self, execution_ids: list, job_ids: list
    ) -> None:
        """Delegates to the repository, which owns the cascade order."""
        await FlowRepository(self.session).delete_execution_children(
            execution_ids, job_ids
        )

    async def force_delete_flow_with_executions(self, flow_id: uuid.UUID) -> bool:
        """
        Force delete a flow by first removing any associated flow executions.
        This handles the foreign key constraint issue.

        Args:
            flow_id: UUID of the flow to delete

        Returns:
            True if deleted, raises HTTPException if not found
        """
        try:
            repository = FlowRepository(self.session)

            if not await repository.exists(flow_id):
                raise NotFoundError(detail="Flow not found")

            logger.info(f"Starting force deletion of flow {flow_id}")

            execution_ids, job_ids = await repository.find_execution_keys(flow_id)

            # Children first, or the executionhistory delete fails with
            # "FOREIGN KEY constraint failed".
            await repository.delete_execution_children(execution_ids, job_ids)

            deleted_count = await repository.delete_executions_of(flow_id)
            if deleted_count > 0:
                logger.info(
                    f"Deleted {deleted_count} flow executions for flow {flow_id}"
                )

            # Off every external surface before the row goes. This variant is
            # what `delete_all_flows_for_group` loops through, so the cleanup has
            # to live here too and not only on the group-checked twin.
            await self._withdraw_publication(flow_id)

            await repository.delete_row(flow_id)

            logger.info(f"Successfully deleted flow {flow_id} with all its executions")
            return True

        except KasalError:
            await self.session.rollback()
            raise
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error force deleting flow with executions: {str(e)}")
            raise KasalError(
                detail=f"Error force deleting flow with executions: {str(e)}"
            )

    async def _withdraw_publication(
        self, flow_id: uuid.UUID, group_context=None
    ) -> None:
        """Unpublish a flow that is about to be deleted.

        A publication outlives the flow it names unless this removes it, and the
        registry is what the MCP tool list, the A2A card and the chat route
        catalogue all read. A dangling row also HOLDS its external name, so a
        deleted flow could permanently block a crew from publishing under it.

        Best-effort: the catalogue drops dangling rows on read anyway, so
        failing the deletion over one would trade a stale row in the publish
        dialog for a flow the user cannot remove.
        """
        from src.services.publications import cleanup

        group_ids = getattr(group_context, "group_ids", None) or None
        try:
            await cleanup.withdraw_entity(self.session, "flow", flow_id, group_ids)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Could not unpublish deleted flow {flow_id}: {exc}")

    async def force_delete_flow_with_executions_with_group_check(
        self, flow_id: uuid.UUID, group_context
    ) -> bool:
        """
        Force delete a flow with group authorization check.
        First verifies the user has access to the flow, then removes associated
        flow executions before deleting the flow itself.

        Args:
            flow_id: UUID of the flow to delete
            group_context: Group context with group_ids list

        Returns:
            True if deleted

        Raises:
            HTTPException: If flow not found or user doesn't have access
        """
        try:
            repository = FlowRepository(self.session)

            exists, flow_group_id = await repository.get_group_id(flow_id)
            if not exists:
                raise NotFoundError(detail="Flow not found")

            # Check if user has access to this flow's group
            if flow_group_id and group_context and group_context.group_ids:
                if flow_group_id not in group_context.group_ids:
                    raise ForbiddenError(detail="Access denied to this flow")

            logger.info(f"Starting force deletion of flow {flow_id} with group check")

            execution_ids, job_ids = await repository.find_execution_keys(flow_id)
            if job_ids:
                logger.info(f"Found {len(job_ids)} flow executions to delete")

            # Children first, or the executionhistory delete fails with
            # "FOREIGN KEY constraint failed".
            await repository.delete_execution_children(execution_ids, job_ids)
            await repository.delete_executions_of(flow_id)

            # Take it off every external surface before the row goes. A
            # publication outlives the flow it names unless something removes
            # it, and the registry is what MCP's tool list, the A2A card and the
            # chat route catalogue all read — one workspace was left advertising
            # eight flows that no longer existed.
            await self._withdraw_publication(flow_id, group_context)

            await repository.delete_row(flow_id)

            logger.info(
                f"Successfully deleted flow {flow_id} with all its executions (group verified)"
            )
            return True

        except KasalError:
            await self.session.rollback()
            raise
        except Exception as e:
            await self.session.rollback()
            logger.error(
                f"Error force deleting flow with executions and group check: {str(e)}"
            )
            raise KasalError(
                detail=f"Error force deleting flow with executions: {str(e)}"
            )

    # Backward compatibility methods (no group isolation)

    async def create_flow(self, flow_in: FlowCreate) -> Flow:
        """
        Create a new flow without group isolation (backward compatibility).

        Args:
            flow_in: Flow data for creation

        Returns:
            Created flow
        """
        try:
            # Validate and normalize flow configuration
            flow_config = flow_in.flow_config or {}

            # Validate listeners format
            for listener in flow_config.get("listeners", []):
                if not isinstance(listener, dict):
                    raise BadRequestError(detail=f"Invalid listener format: {listener}")
                # Check for required fields in listener (only if listener is not empty)
                if listener and not all(key in listener for key in ["name", "crewId"]):
                    raise BadRequestError(
                        detail=f"Missing required fields in listener: {listener}"
                    )

            # Validate actions format
            for action in flow_config.get("actions", []):
                if not isinstance(action, dict):
                    raise BadRequestError(detail=f"Invalid action format: {action}")
                # Check for required fields in action (only if action is not empty)
                if action and not all(key in action for key in ["crewId", "taskId"]):
                    raise BadRequestError(
                        detail=f"Missing required fields in action: {action}"
                    )

            # Generate a new UUID for the flow
            flow_uuid = str(uuid.uuid4())

            # Ensure required fields exist in flow_config
            flow_config = {
                "id": flow_uuid,
                "name": flow_config.get("name", flow_in.name),
                "type": flow_config.get("type", "default"),
                "listeners": flow_config.get("listeners", []),
                "actions": flow_config.get("actions", []),
                "startingPoints": flow_config.get("startingPoints", []),
            }

            # Create flow dictionary
            flow_dict = {
                "name": flow_in.name,
                "crew_id": flow_in.crew_id,
                "nodes": [node.model_dump() for node in flow_in.nodes],
                "edges": [edge.model_dump() for edge in flow_in.edges],
                "flow_config": flow_config,
                "group_id": flow_in.group_id if hasattr(flow_in, "group_id") else None,
                "created_by_email": (
                    flow_in.created_by_email
                    if hasattr(flow_in, "created_by_email")
                    else None
                ),
            }

            repository = FlowRepository(self.session)
            flow = await repository.create(flow_dict)

            logger.info(f"Successfully created flow with ID: {flow.id}")
            return flow

        except ValueError as ve:
            logger.error(f"Validation error while creating flow: {str(ve)}")
            raise BadRequestError(detail=str(ve))
        except KasalError:
            raise
        except Exception as e:
            logger.error(f"Error creating flow: {str(e)}")
            raise KasalError(detail=f"Error creating flow: {str(e)}")

    async def get_all_flows(self) -> List[Flow]:
        """
        Get all flows without group filtering (backward compatibility).

        Returns:
            List of all flows
        """
        repository = FlowRepository(self.session)
        return await repository.find_all()

    async def delete_flow(self, flow_id: uuid.UUID) -> bool:
        """
        Delete a flow if it has no execution records (backward compatibility).

        Args:
            flow_id: ID of the flow to delete

        Returns:
            True if deletion successful

        Raises:
            HTTPException: 404 if flow not found, 400 if has executions
        """
        repository = FlowRepository(self.session)
        flow = await repository.get(flow_id)

        if not flow:
            raise NotFoundError(detail="Flow not found")

        # Check for execution records
        execution_count = await repository.count_executions(flow_id)
        if execution_count > 0:
            raise BadRequestError(
                detail=f"Cannot delete flow with {execution_count} execution records. Use force delete instead."
            )

        await self._withdraw_publication(flow_id)
        await repository.delete(flow_id)
        logger.info(f"Successfully deleted flow {flow_id}")
        return True

    async def delete_all_flows(self) -> None:
        """
        Delete all flows (backward compatibility).
        Use with caution - this does not check for execution records.
        """
        repository = FlowRepository(self.session)
        await repository.delete_all()
        try:
            from src.services.publications import cleanup

            await cleanup.withdraw_all(self.session, "flow")
        except Exception as exc:  # noqa: BLE001 — the flows are already gone
            logger.warning(f"Could not unpublish deleted flows: {exc}")
        logger.info("Deleted all flows")

    async def validate_flow_data(self, flow_in: FlowCreate) -> Dict[str, Any]:
        """
        Validate flow data without creating it.

        Args:
            flow_in: Flow data to validate

        Returns:
            Validation result
        """
        try:
            # Convert to dict to ensure it's valid
            data_dict = flow_in.model_dump()
            logger.info("Flow data validation successful")
            logger.info(f"Flow name: {data_dict['name']}")
            logger.info(f"Crew ID: {data_dict['crew_id']}")
            logger.info(f"Number of nodes: {len(data_dict['nodes'])}")
            logger.info(f"Number of edges: {len(data_dict['edges'])}")

            if data_dict.get("flow_config"):
                logger.info(
                    f"Flow config details: {json.dumps(data_dict['flow_config'], indent=2)}"
                )

            return {
                "status": "success",
                "message": "Data validation successful",
                "data": {
                    "name": data_dict["name"],
                    "crew_id": data_dict["crew_id"],
                    "node_count": len(data_dict["nodes"]),
                    "edge_count": len(data_dict["edges"]),
                    "has_flow_config": data_dict.get("flow_config") is not None,
                },
            }
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            return {"status": "error", "message": f"Validation failed: {str(e)}"}
