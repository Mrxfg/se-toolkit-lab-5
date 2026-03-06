from datetime import datetime
from typing import Optional, List, Dict, Tuple

import httpx
from sqlalchemy import select, func
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------

async def fetch_items() -> List[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    url = f"{settings.autochecker_api_url}/api/items"

    async with httpx.AsyncClient(
        auth=(settings.autochecker_email, settings.autochecker_password),
        timeout=30,
    ) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

    # Expecting a JSON array
    if not isinstance(data, list):
        raise ValueError("Unexpected items response format")

    return data


async def fetch_logs(since: Optional[datetime] = None) -> List[dict]:
    """Fetch check results from the autochecker API with pagination."""
    url = f"{settings.autochecker_api_url}/api/logs"

    params = {"limit": 500}
    if since:
        params["since"] = since.isoformat()

    all_logs: List[dict] = []

    async with httpx.AsyncClient(
        auth=(settings.autochecker_email, settings.autochecker_password),
        timeout=60,
    ) as client:
        current_since = params.get("since")

        while True:
            req_params = {"limit": 500}
            if current_since:
                req_params["since"] = current_since

            resp = await client.get(url, params=req_params)
            resp.raise_for_status()

            data = resp.json()

            logs = data.get("logs", [])
            has_more = data.get("has_more", False)

            if not logs:
                break

            all_logs.extend(logs)

            # Pagination: move "since" to the last log timestamp
            last_ts = logs[-1]["submitted_at"]
            current_since = last_ts

            if not has_more:
                break

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------

async def load_items(items: List[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    from app.models.item import ItemRecord

    created = 0
    lab_lookup: Dict[str, ItemRecord] = {}

    # --- Process labs first ---
    for item in items:
        if item["type"] != "lab":
            continue

        lab_title = item["title"]
        lab_short = item["lab"]

        result = await session.execute(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == lab_title,
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            lab_lookup[lab_short] = existing
            continue

        lab_record = ItemRecord(type="lab", title=lab_title)
        session.add(lab_record)
        await session.flush()

        lab_lookup[lab_short] = lab_record
        created += 1

    # --- Process tasks ---
    for item in items:
        if item["type"] != "task":
            continue

        task_title = item["title"]
        lab_short = item["lab"]

        parent_lab = lab_lookup.get(lab_short)
        if not parent_lab:
            continue

        result = await session.execute(
            select(ItemRecord).where(
                ItemRecord.title == task_title,
                ItemRecord.parent_id == parent_lab.id,
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            continue

        task_record = ItemRecord(
            type="task",
            title=task_title,
            parent_id=parent_lab.id,
        )

        session.add(task_record)
        created += 1

    await session.commit()
    return created


async def load_logs(
    logs: List[dict], items_catalog: List[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    from app.models.learner import Learner
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord

    created = 0

    # Build lookup: (lab, task) -> title
    item_lookup: Dict[Tuple[str, Optional[str]], str] = {}

    for item in items_catalog:
        key = (item["lab"], item["task"])
        item_lookup[key] = item["title"]

    for log in logs:

        # --- Learner ---
        result = await session.execute(
            select(Learner).where(Learner.external_id == log["student_id"])
        )
        learner = result.scalar_one_or_none()

        if not learner:
            learner = Learner(
                external_id=log["student_id"],
                student_group=log["group"],
            )
            session.add(learner)
            await session.flush()

        # --- Item ---
        title = item_lookup.get((log["lab"], log["task"]))
        if not title:
            continue

        result = await session.execute(
            select(ItemRecord).where(ItemRecord.title == title)
        )
        item = result.scalars().first()

        if not item:
            continue

        # --- Idempotency check ---
        result = await session.execute(
            select(InteractionLog).where(
                InteractionLog.external_id == log["id"]
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            continue

        created_at = datetime.fromisoformat(
            log["submitted_at"].replace("Z", "+00:00")
        )

        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=created_at,
        )

        session.add(interaction)
        created += 1

    await session.commit()
    return created


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    from app.models.interaction import InteractionLog

    # Step 1: fetch items
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: determine last synced timestamp
    result = await session.execute(
        select(func.max(InteractionLog.created_at))
    )
    last_synced = result.scalar_one_or_none()

    # Step 3: fetch logs since last timestamp
    logs = await fetch_logs(last_synced)

    new_records = await load_logs(logs, items, session)

    # Total records
    result = await session.execute(
        select(func.count()).select_from(InteractionLog)
    )
    total_records = result.scalar_one()

    return {
        "new_records": new_records,
        "total_records": total_records,
    }