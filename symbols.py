from dataclasses import dataclass
from typing import Any, List, Optional
from lark import ast_utils

from visitor import Visitor


class Symbol(ast_utils.Ast):
    def accept(self, visitor: Visitor):
        return visitor.visit(self)


@dataclass
class SelectClause(Symbol):
    selectables: List[Any]

@dataclass
class FromSource(Symbol):
    source: Any

@dataclass
class FromClause(Symbol):
    source: FromSource


@dataclass
class SelectStmt(Symbol):
    select_clause: SelectClause
    from_clause: FromClause

@dataclass
class Selectable(Symbol):
    value: Any

@dataclass
class Program(Symbol):
    statements: list

@dataclass
class Literal(Symbol):
    value: Any


@dataclass
class Expr(Symbol):
    value: Any

@dataclass
class Condition(Symbol):
    value: Any

@dataclass
class OrClause(Symbol):
    left: Any
    right: Any

@dataclass
class AndClause(Symbol):
    left: Any
    right: Any

@dataclass
class NotClause(Symbol):
    operand: Any

@dataclass
class Comparison(Symbol):
    left: Any
    operator: str
    right: Any

@dataclass
class Predicate(Symbol):
    value: Any

@dataclass
class Term(Symbol):
    left: Any
    operator: str
    right: Any

@dataclass
class Factor(Symbol):
    left: Any
    operator: str
    right: Any

@dataclass
class UnaryOp(Symbol):
    operator: str
    operand: Any

@dataclass
class BinaryOp(Symbol):
    operator: str
    left: Any
    right: Any

@dataclass
class Primary(Symbol):
    value: Any

@dataclass
class Identifier(Symbol):
    name: str

@dataclass
class ColumnName(Symbol):
    name: str

@dataclass
class WhereClause(Symbol):
    condition: Any

@dataclass
class GroupByClause(Symbol):
    columns: List[Any]

@dataclass
class HavingClause(Symbol):
    condition: Any

@dataclass
class OrderByClause(Symbol):
    columns: List[Any]

@dataclass
class LimitClause(Symbol):
    limit: Any
    offset: Optional[Any] = None


@dataclass
class SingleSource(Symbol):
    table_name: str
    alias: Optional[str] = None


@dataclass
class Source(Symbol):
    single_source: List[SingleSource]


@dataclass
class Joining(Symbol):
    value: Any

@dataclass
class ConditionedJoin(Symbol):
    source: Any
    single_source: Any
    condition: Any
    join_modifier: Optional[str] = None

@dataclass
class UnconditionedJoin(Symbol):
    source: Any
    single_source: Any

@dataclass
class OrderedColumn(Symbol):
    column: Any
    direction: Optional[str] = None

@dataclass
class ColumnDef(Symbol):
    column_name: str
    datatype: str
    primary_key: bool = False
    not_null: bool = False
@dataclass
class CreateStmt(Symbol):
    table_name: str
    column_defs: List[ColumnDef]

@dataclass
class DropStmt(Symbol):
    table_name: str

@dataclass
class InsertStmt(Symbol):
    table_name: str
    columns: List[str]
    values: List[Any]

@dataclass
class DeleteStmt(Symbol):
    table_name: str
    where_clause: Optional[Any] = None

@dataclass
class UpdateStmt(Symbol):
    table_name: str
    column: str
    value: Any
    where_clause: Optional[Any] = None

@dataclass
class TruncateStmt(Symbol):
    table_name: str

@dataclass
class FuncCall(Symbol):
    func_name: str
    args: List[Any]

@dataclass
class Nested(Symbol):
    value: Any