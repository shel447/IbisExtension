from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres


class DSQLDialect(Postgres):
    class Generator(Postgres.Generator):
        IDENTIFY = False
        SINGLE_STRING_INTERVAL = False
        TYPE_MAPPING = {
            **Postgres.Generator.TYPE_MAPPING,
            exp.DataType.Type.FLOAT: "FLOAT",
            exp.DataType.Type.DOUBLE: "DOUBLE",
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.VARCHAR: "STRING",
        }
        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,
            exp.Rand: lambda self, expression: self.func("RAND"),
            exp.StrPosition: lambda self, expression: self.func(
                "INSTR", expression.this, expression.args["substr"]
            ),
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

        def not_sql(self, expression: exp.Not) -> str:
            target = expression.this

            if isinstance(target, exp.Paren):
                target = target.this

            if isinstance(target, exp.In):
                return self.in_sql(target).replace(" IN ", " NOT IN ", 1)

            if isinstance(target, exp.Like):
                return (
                    f"{self.sql(target, 'this')} NOT LIKE "
                    f"{self.sql(target, 'expression')}"
                )

            if isinstance(target, exp.Is) and isinstance(target.expression, exp.Null):
                return f"{self.sql(target, 'this')} IS NOT NULL"

            return super().not_sql(expression)
