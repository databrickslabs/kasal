"""Billing estimates for the selected teamspace; rate changes require admin."""

from typing import Annotated

from fastapi import APIRouter, Depends

from src.dependencies.providers import GroupContextDep, SessionDep
from src.schemas.billing import BillingQuery, BillingSummary, ModelRate
from src.services.billing.service import BillingService

router = APIRouter(prefix="/billing", tags=["billing"])


async def get_billing_service(session: SessionDep):
    return BillingService(session)


BillingServiceDep = Annotated[BillingService, Depends(get_billing_service)]


@router.post("/summary", response_model=BillingSummary)
async def summary(
    query: BillingQuery, group_context: GroupContextDep, service: BillingServiceDep
):
    # Admin, Editor and Operator may monitor their selected teamspace.
    return await service.summary(group_context, query)


@router.put("/rates", response_model=ModelRate)
async def save_rate(
    rate: ModelRate, group_context: GroupContextDep, service: BillingServiceDep
):
    # Only an Admin may set the rates used in everyone’s estimates.
    return await service.save_rate(group_context, rate)
