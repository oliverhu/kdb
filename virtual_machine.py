from pager import Table
from schema.basic_schema import BasicSchema
from visitor import Visitor
from symbols import *

class VirtualMachine(Visitor):
    def __init__(self, table: Table):
        self.table = table
        self.stack = []
        self.heap = {}
        self.pc = 0
        self.running = True

    def run(self,program):
        self.execute(program)

    def execute(self, stmt: Symbol):
        stmt.accept(self)

    def visit_select_stmt(self, stmt: SelectStmt):
        pass

    def visit_from_clause(self, stmt: FromClause):
        pass

    def visit_select_clause(self, stmt: SelectClause):
        pass

    def visit_selectable(self, stmt: Selectable):
        pass

    def visit_program(self, stmt: Program):
        for stmt in stmt.statements:
            self.execute(stmt)

    def visit_literal(self, stmt: Literal):
        pass

    def visit_expr(self, stmt: Expr):
        pass

    def visit_condition(self, stmt: Condition):
        pass

    def visit_or_clause(self, stmt: OrClause):
        pass

    def visit_and_clause(self, stmt: AndClause):
        pass

    def visit_not_clause(self, stmt: NotClause):
        pass

    def visit_comparison(self, stmt: Comparison):
        pass

    def visit_predicate(self, stmt: Predicate):
        pass

    def visit_term(self, stmt: Term):
        pass

    def visit_factor(self, stmt: Factor):
        pass

    def visit_unary_op(self, stmt: UnaryOp):
        pass

    def visit_binary_op(self, stmt: BinaryOp):
        pass

    def visit_primary(self, stmt: Primary):
        pass

    def visit_identifier(self, stmt: Identifier):
        pass

    def visit_column_name(self, stmt: ColumnName):
        pass

    def visit_where_clause(self, stmt: WhereClause):
        pass

    def visit_group_by_clause(self, stmt: GroupByClause):
        pass

    def visit_having_clause(self, stmt: HavingClause):
        pass

    def visit_order_by_clause(self, stmt: OrderByClause):
        pass

    def visit_limit_clause(self, stmt: LimitClause):
        pass

    def visit_source(self, stmt: Source):
        pass

    def visit_single_source(self, stmt: SingleSource):
        pass

    def visit_joining(self, stmt: Joining):
        pass

    def visit_conditioned_join(self, stmt: ConditionedJoin):
        pass

    def visit_unconditioned_join(self, stmt: UnconditionedJoin):
        pass

    def visit_ordered_column(self, stmt: OrderedColumn):
        pass

    def visit_create_stmt(self, stmt: CreateStmt):
        schema = BasicSchema(stmt.table_name,
                             [ColumnDef(col.column_name, col.datatype, col.primary_key, col.not_null) for col in stmt.column_defs])
        table_name = stmt.table_name


    def visit_column_def(self, stmt: ColumnDef):
        pass

    def visit_drop_stmt(self, stmt: DropStmt):
        pass

    def visit_insert_stmt(self, stmt: InsertStmt):
        pass

    def visit_delete_stmt(self, stmt: DeleteStmt):
        pass

    def visit_update_stmt(self, stmt: UpdateStmt):
        pass

    def visit_truncate_stmt(self, stmt: TruncateStmt):
        pass

    def visit_func_call(self, stmt: FuncCall):
        pass

    def visit_nested(self, stmt: Nested):
        pass
