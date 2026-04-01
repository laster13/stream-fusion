import time
from typing import Optional

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.models.title_normalization_rule_model import TitleNormalizationRuleModel


class TitleNormalizationRuleDAO:
    """DAO for the title_normalization_rules table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_all_active(self) -> list[TitleNormalizationRuleModel]:
        try:
            result = await self.session.execute(
                select(TitleNormalizationRuleModel)
                .where(TitleNormalizationRuleModel.is_active == True)
                .order_by(TitleNormalizationRuleModel.rule_type, TitleNormalizationRuleModel.id)
            )
            return list(result.scalars().all())
        except Exception as e:
            logger.warning(f"TitleNormalizationRuleDAO: get_all_active failed ({e})")
            return []

    async def get_all(self) -> list[TitleNormalizationRuleModel]:
        try:
            result = await self.session.execute(
                select(TitleNormalizationRuleModel)
                .order_by(TitleNormalizationRuleModel.rule_type, TitleNormalizationRuleModel.id)
            )
            return list(result.scalars().all())
        except Exception as e:
            logger.warning(f"TitleNormalizationRuleDAO: get_all failed ({e})")
            return []

    async def get_by_id(self, rule_id: int) -> Optional[TitleNormalizationRuleModel]:
        try:
            result = await self.session.execute(
                select(TitleNormalizationRuleModel).where(TitleNormalizationRuleModel.id == rule_id)
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.warning(f"TitleNormalizationRuleDAO: get_by_id failed ({e})")
            return None

    async def count(self) -> int:
        try:
            result = await self.session.execute(
                select(TitleNormalizationRuleModel)
            )
            return len(result.scalars().all())
        except Exception as e:
            logger.warning(f"TitleNormalizationRuleDAO: count failed ({e})")
            return 0

    async def create(
        self,
        rule_type: str,
        pattern: str,
        replacement: str = "",
        description: Optional[str] = None,
        is_active: bool = True,
    ) -> TitleNormalizationRuleModel:
        rule = TitleNormalizationRuleModel(
            rule_type=rule_type,
            pattern=pattern,
            replacement=replacement,
            description=description,
            is_active=is_active,
        )
        self.session.add(rule)
        await self.session.commit()
        await self.session.refresh(rule)
        logger.info(f"TitleNormalizationRuleDAO: created rule [{rule_type}] '{pattern}'")
        return rule

    async def update(
        self,
        rule_id: int,
        rule_type: str,
        pattern: str,
        replacement: str = "",
        description: Optional[str] = None,
        is_active: bool = True,
    ) -> Optional[TitleNormalizationRuleModel]:
        result = await self.session.execute(
            select(TitleNormalizationRuleModel).where(TitleNormalizationRuleModel.id == rule_id)
        )
        rule = result.scalar_one_or_none()
        if not rule:
            return None
        rule.rule_type = rule_type
        rule.pattern = pattern
        rule.replacement = replacement
        rule.description = description
        rule.is_active = is_active
        rule.updated_at = int(time.time())
        await self.session.commit()
        await self.session.refresh(rule)
        logger.info(f"TitleNormalizationRuleDAO: updated rule {rule_id}")
        return rule

    async def delete(self, rule_id: int) -> bool:
        try:
            result = await self.session.execute(
                delete(TitleNormalizationRuleModel).where(TitleNormalizationRuleModel.id == rule_id)
            )
            await self.session.commit()
            deleted = result.rowcount > 0
            if deleted:
                logger.info(f"TitleNormalizationRuleDAO: deleted rule {rule_id}")
            return deleted
        except Exception as e:
            await self.session.rollback()
            logger.warning(f"TitleNormalizationRuleDAO: delete failed ({e})")
            return False

    async def bulk_create(self, rules: list[dict]) -> int:
        """Insert multiple rules at once. Returns the number inserted."""
        created = 0
        for r in rules:
            rule = TitleNormalizationRuleModel(**r)
            self.session.add(rule)
            created += 1
        await self.session.commit()
        logger.info(f"TitleNormalizationRuleDAO: bulk_create inserted {created} rules")
        return created
