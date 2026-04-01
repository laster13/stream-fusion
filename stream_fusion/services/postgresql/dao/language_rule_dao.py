import time
from typing import Optional

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.models.language_rule_model import LanguageRuleModel


class LanguageRuleDAO:
    """DAO for the language_rules table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_all_active(self) -> list[LanguageRuleModel]:
        try:
            result = await self.session.execute(
                select(LanguageRuleModel)
                .where(LanguageRuleModel.is_active == True)
                .order_by(LanguageRuleModel.rule_type, LanguageRuleModel.id)
            )
            return list(result.scalars().all())
        except Exception as e:
            logger.warning(f"LanguageRuleDAO: get_all_active failed ({e})")
            return []

    async def get_all(self) -> list[LanguageRuleModel]:
        try:
            result = await self.session.execute(
                select(LanguageRuleModel)
                .order_by(LanguageRuleModel.rule_type, LanguageRuleModel.id)
            )
            return list(result.scalars().all())
        except Exception as e:
            logger.warning(f"LanguageRuleDAO: get_all failed ({e})")
            return []

    async def get_by_id(self, rule_id: int) -> Optional[LanguageRuleModel]:
        try:
            result = await self.session.execute(
                select(LanguageRuleModel).where(LanguageRuleModel.id == rule_id)
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.warning(f"LanguageRuleDAO: get_by_id failed ({e})")
            return None

    async def count(self) -> int:
        try:
            result = await self.session.execute(select(LanguageRuleModel))
            return len(result.scalars().all())
        except Exception as e:
            logger.warning(f"LanguageRuleDAO: count failed ({e})")
            return 0

    async def create(
        self,
        rule_type: str,
        key: str,
        value: str,
        extra: Optional[dict] = None,
        description: Optional[str] = None,
        is_active: bool = True,
    ) -> LanguageRuleModel:
        rule = LanguageRuleModel(
            rule_type=rule_type,
            key=key,
            value=value,
            extra=extra,
            description=description,
            is_active=is_active,
        )
        self.session.add(rule)
        await self.session.commit()
        await self.session.refresh(rule)
        logger.info(f"LanguageRuleDAO: created rule [{rule_type}] '{key}'")
        return rule

    async def update(
        self,
        rule_id: int,
        rule_type: str,
        key: str,
        value: str,
        extra: Optional[dict] = None,
        description: Optional[str] = None,
        is_active: bool = True,
    ) -> Optional[LanguageRuleModel]:
        result = await self.session.execute(
            select(LanguageRuleModel).where(LanguageRuleModel.id == rule_id)
        )
        rule = result.scalar_one_or_none()
        if not rule:
            return None
        rule.rule_type = rule_type
        rule.key = key
        rule.value = value
        rule.extra = extra
        rule.description = description
        rule.is_active = is_active
        rule.updated_at = int(time.time())
        await self.session.commit()
        await self.session.refresh(rule)
        logger.info(f"LanguageRuleDAO: updated rule {rule_id}")
        return rule

    async def delete(self, rule_id: int) -> bool:
        try:
            result = await self.session.execute(
                delete(LanguageRuleModel).where(LanguageRuleModel.id == rule_id)
            )
            await self.session.commit()
            deleted = result.rowcount > 0
            if deleted:
                logger.info(f"LanguageRuleDAO: deleted rule {rule_id}")
            return deleted
        except Exception as e:
            await self.session.rollback()
            logger.warning(f"LanguageRuleDAO: delete failed ({e})")
            return False

    async def bulk_create(self, rules: list[dict]) -> int:
        """Insert multiple rules at once. Returns the number inserted."""
        created = 0
        for r in rules:
            rule = LanguageRuleModel(**r)
            self.session.add(rule)
            created += 1
        await self.session.commit()
        logger.info(f"LanguageRuleDAO: bulk_create inserted {created} rules")
        return created
