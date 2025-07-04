# Interpreter evalautes the expressions, currently used for where clause

from typing import Any
from record import Record
from symbols import *
from visitor import Visitor

class Interpreter(Visitor):

    def __init__(self, record: Record = None):
        self.record = record

    def set_record(self, record: Record):
        self.record = record

    def evaluate(self, expr: Expr) -> Any:
        return expr.accept(self)

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
        left = self.evaluate(expr.left)
        right = self.evaluate(expr.right)
        if expr.operator == ArithmeticOp.EQ:
            return left == right
        elif expr.operator == ArithmeticOp.NE:
            return left != right
        elif expr.operator == ArithmeticOp.GT:
            return left > right

    def visit_term(self, expr: Term) -> Any:
        pass

    def visit_or_clause(self, expr: OrClause) -> Any:
        print("interpreter or clause", expr)
        for and_clause in expr.and_clauses:
            if self.evaluate(and_clause):
                return True
        return False

    def visit_and_clause(self, expr: AndClause) -> Any:
        for predicate in expr.predicates:
            if not self.evaluate(predicate):
                return False
        return True

    def visit_not_clause(self, expr: NotClause) -> Any:
        pass

    def visit_predicate(self, expr: Predicate) -> Any:
        pass

    def visit_column_name(self, expr: ColumnName) -> Any:
        return self.record.values[expr.name]

    def visit_literal(self, expr: Literal) -> Any:
        return expr.value

    def visit_identifier(self, expr: Identifier) -> Any:
        pass

    def visit_primary(self, expr: Primary) -> Any:
        pass