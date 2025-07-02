from pager import PageHeader, Table
from record import Record
from schema.basic_schema import BasicSchema, Column
from schema.datatypes import Integer, Text, Boolean
from state_manager import StateManager
from visitor import Visitor
from symbols import *

class VirtualMachine(Visitor):
    def __init__(self, file_path: str):
        self.stack = []
        self.heap = {}
        self.pc = 0
        self.running = True
        self.state_manager = StateManager(file_path)

    def run(self,program):
        self.execute(program)

    def execute(self, stmt: Symbol):
        stmt.accept(self)

    def visit_select_stmt(self, stmt: SelectStmt):
        from_clause = stmt.from_clause
        self.materialize(from_clause.source.source)

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
        # Convert ColumnDef objects to Column objects
        columns = []
        for col_def in stmt.column_defs:
            # Convert datatype string to actual datatype object
            if col_def.datatype.lower() == "integer":
                datatype = Integer()
            elif col_def.datatype.lower() == "text":
                datatype = Text()
            elif col_def.datatype.lower() == "boolean":
                datatype = Boolean()
            else:
                raise ValueError(f"Unsupported datatype: {col_def.datatype}")

            column = Column(col_def.column_name, datatype, col_def.primary_key)
            columns.append(column)

        schema = BasicSchema(stmt.table_name, columns)
        table_name = stmt.table_name
        # TODO: table/schema is only stored in memory for now :/
        self.state_manager.register_table(table_name, schema)


    def visit_column_def(self, stmt: ColumnDef):
        pass

    def visit_drop_stmt(self, stmt: DropStmt):
        pass

    def visit_insert_stmt(self, stmt: InsertStmt):
        table_name = stmt.table_name
        schema = self.state_manager.schemas[table_name]
        # Extract string names from ColumnName objects
        column_names = [col.name if hasattr(col, 'name') else str(col) for col in stmt.columns]
        # Extract raw values from Literal objects
        values = [v.value if hasattr(v, 'value') else v for v in stmt.values]
        row_dict = dict(zip(column_names, values))
        record = Record(values=row_dict, schema=schema)
        # For now, insert into page 1
        self.state_manager.insert(table_name, record)

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

    def filter_records(self, where_clause: WhereClause, source: Source):
        pass

    def materialize(self, source: SingleSource):
        table_name = source.table_name
        schema = self.state_manager.schemas[table_name]
        assert schema in self.state_manager.schemas, f"Table {table_name} not found"
        pager_num = self.state_manager.table_pages[table_name]
        page = self.state_manager.pager.get_page(pager_num)
        page_header = PageHeader.from_header(page)
        records = []
        for cell_num in page_header.cell_pointers:
            cell = self.state_manager.pager.get_page(cell_num)
            record = Record.from_bytes(cell)
            records.append(record)
        return records