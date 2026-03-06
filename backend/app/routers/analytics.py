"""Router for analytics endpoints using `interacts` table, safe for all labs."""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import func, case
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord as Item
from app.models.learner import Learner

router = APIRouter(prefix="/analytics", tags=["analytics"])


async def get_lab_item(lab: str, session: AsyncSession) -> Item | None:
    """Find the lab item by case-insensitive match."""
    result = await session.exec(
        select(Item).where(Item.title.ilike(f"%{lab}%"))
    )
    return result.first()


async def get_task_ids(lab_item: Item, session: AsyncSession) -> list[int]:
    """Get IDs of tasks belonging to a lab."""
    tasks = await session.exec(select(Item.id).where(Item.parent_id == lab_item.id))
    return [t for t in tasks.all()]


@router.get("/scores")
async def get_scores(lab: str = Query(...), session: AsyncSession = Depends(get_session)):
    lab_item = await get_lab_item(lab, session)
    if not lab_item:
        return [{"bucket": b, "count": 0} for b in ["0-25", "26-50", "51-75", "76-100"]]

    task_ids = await get_task_ids(lab_item, session)
    if not task_ids:
        return [{"bucket": b, "count": 0} for b in ["0-25", "26-50", "51-75", "76-100"]]

    buckets = [("0-25", 0, 25), ("26-50", 26, 50), ("51-75", 51, 75), ("76-100", 76, 100)]
    case_expr = [
        func.count(case(((InteractionLog.score >= low) & (InteractionLog.score <= high), 1))).label(bucket)
        for bucket, low, high in buckets
    ]

    result = await session.exec(
        select(*case_expr).where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score != None
        )
    )
    counts = result.one()
    return [{"bucket": buckets[i][0], "count": counts[i]} for i in range(len(buckets))]


@router.get("/pass-rates")
async def get_pass_rates(lab: str = Query(...), session: AsyncSession = Depends(get_session)):
    """Per-task pass rates (NULL-safe)."""
    lab_item = await get_lab_item(lab, session)
    if not lab_item:
        return []

    tasks = await session.exec(select(Item).where(Item.parent_id == lab_item.id))
    tasks = tasks.all()
    result = []

    for task in tasks:
        agg = await session.exec(
            select(
                func.round(func.avg(InteractionLog.score), 1),
                func.count(InteractionLog.id)
            ).where(InteractionLog.item_id == task.id)
        )
        row = agg.first()
        if row:
            avg_score, attempts = row
        else:
            avg_score, attempts = 0.0, 0

        result.append({
            "task": task.title,
            "avg_score": avg_score or 0.0,
            "attempts": attempts
        })

    return sorted(result, key=lambda x: x["task"])


@router.get("/timeline")
async def get_timeline(lab: str = Query(...), session: AsyncSession = Depends(get_session)):
    """Submissions per day."""
    lab_item = await get_lab_item(lab, session)
    if not lab_item:
        return []

    task_ids = await get_task_ids(lab_item, session)
    if not task_ids:
        return []

    result = await session.exec(
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count().label("submissions")
        ).where(InteractionLog.item_id.in_(task_ids))
         .group_by(func.date(InteractionLog.created_at))
         .order_by(func.date(InteractionLog.created_at))
    )
    return [{"date": str(r.date), "submissions": r.submissions} for r in result.all()]


@router.get("/groups")
async def get_groups(lab: str = Query(...), session: AsyncSession = Depends(get_session)):
    """Per-group performance (NULL-safe)."""
    lab_item = await get_lab_item(lab, session)
    if not lab_item:
        return []

    task_ids = await get_task_ids(lab_item, session)
    if not task_ids:
        return []

    result = await session.exec(
        select(
            Learner.student_group,
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(func.distinct(InteractionLog.learner_id)).label("students")
        ).join(Learner, Learner.id == InteractionLog.learner_id)
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score != None
        )
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    return [
        {
            "group": r.student_group,
            "avg_score": r.avg_score or 0.0,
            "students": r.students
        }
        for r in result.all()
    ]