from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select
from sqlalchemy import func, case

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()


def _lab_title(lab: str) -> str:
    return lab.replace("-", " ").title()


@router.get("/scores")
async def get_scores(
    lab: str = Query(...),
    session: AsyncSession = Depends(get_session),
):

    title = _lab_title(lab)

    res = await session.exec(
        select(ItemRecord).where(ItemRecord.title.contains(title))
    )
    lab_item = res.first()

    res = await session.exec(
        select(ItemRecord.id).where(ItemRecord.parent_id == lab_item.id)
    )
    task_ids = res.all()

    bucket_case = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    )

    res = await session.exec(
        select(bucket_case, func.count())
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(bucket_case)
    )

    counts = {b: c for b, c in res.all()}
    buckets = ["0-25", "26-50", "51-75", "76-100"]

    return [{"bucket": b, "count": counts.get(b, 0)} for b in buckets]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(...),
    session: AsyncSession = Depends(get_session),
):

    title = _lab_title(lab)

    res = await session.exec(
        select(ItemRecord).where(ItemRecord.title.contains(title))
    )
    lab_item = res.first()

    res = await session.exec(
        select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    )
    tasks = res.all()

    result = []

    for task in tasks:
        stats = await session.exec(
            select(
                func.avg(InteractionLog.score),
                func.count(InteractionLog.id),
            ).where(InteractionLog.item_id == task.id)
        )

        avg_score, attempts = stats.first()

        result.append(
            {
                "task": task.title,
                "avg_score": round(avg_score, 1) if avg_score else 0,
                "attempts": attempts,
            }
        )

    return sorted(result, key=lambda x: x["task"])


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(...),
    session: AsyncSession = Depends(get_session),
):

    title = _lab_title(lab)

    res = await session.exec(
        select(ItemRecord).where(ItemRecord.title.contains(title))
    )
    lab_item = res.first()

    res = await session.exec(
        select(ItemRecord.id).where(ItemRecord.parent_id == lab_item.id)
    )
    task_ids = res.all()

    res = await session.exec(
        select(
            func.date(InteractionLog.created_at),
            func.count()
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    return [
        {"date": str(d), "submissions": c}
        for d, c in res.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(...),
    session: AsyncSession = Depends(get_session),
):

    title = _lab_title(lab)

    res = await session.exec(
        select(ItemRecord).where(ItemRecord.title.contains(title))
    )
    lab_item = res.first()

    res = await session.exec(
        select(ItemRecord.id).where(ItemRecord.parent_id == lab_item.id)
    )
    task_ids = res.all()

    res = await session.exec(
        select(
            Learner.student_group,
            func.avg(InteractionLog.score),
            func.count(func.distinct(InteractionLog.learner_id)),
        )
        .join(Learner, Learner.id == InteractionLog.learner_id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    return [
        {
            "group": g,
            "avg_score": round(avg, 1),
            "students": s,
        }
        for g, avg, s in res.all()
    ]