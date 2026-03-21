from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres


class DSQLDialect(Postgres):
    class Generator(Postgres.Generator):
        IDENTIFY = False
        TYPE_MAPPING = {
            **Postgres.Generator.TYPE_MAPPING,
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.VARCHAR: "STRING",
        }
        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,
            exp.Rand: lambda self, expression: self.func("RAND"),
        }

        def identifier_sql(self, expression: exp.Identifier) -> str:
            text = expression.name
            lower = text.lower()
            text = lower if self.normalize and not expression.quoted else text
            text = text.replace(self._identifier_end, self._escaped_identifier_end)

            if self.identify and (
                self.dialect.can_identify(text, self.identify)
                or lower in self.RESERVED_KEYWORDS
                or (
                    not self.dialect.IDENTIFIERS_CAN_START_WITH_DIGIT
                    and text[:1].isdigit()
                )
            ):
                text = f"{self._identifier_start}{text}{self._identifier_end}"

            return text
