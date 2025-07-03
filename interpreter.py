# Interpreter evalautes the expressions, currently used for where clause

from typing import Any
from symbols import *
from visitor import Visitor

class Interpreter(Visitor):
    def __init__(self):
        pass

    def evaluate(self, expr: Expr) -> Any:
        pass

    def evaluate_comparison(self, expr: Comparison) -> Any:
        pass

    def evaluate_term(self, expr: Term) -> Any:
        pass

    def evaluate_factor(self, expr: Factor) -> Any:
        pass

    def evaluate_primary(self, expr: Primary) -> Any:
        pass

    def visit_expr(self, expr: Expr) -> Any:
        pass

    def visit_comparison(self, expr: Comparison) -> Any:
        pass

    def visit_term(self, expr: Term) -> Any:
        pass

    def visit_or_clause(self, expr: OrClause) -> Any:
        pass

    def visit_and_clause(self, expr: AndClause) -> Any:
        pass

    def visit_not_clause(self, expr: NotClause) -> Any:
        pass

    def visit_predicate(self, expr: Predicate) -> Any:
        pass

    def visit_column_name(self, expr: ColumnName) -> Any:
        pass

    def visit_literal(self, expr: Literal) -> Any:
        pass

    def visit_identifier(self, expr: Identifier) -> Any:
        pass

    def visit_primary(self, expr: Primary) -> Any:
        pass