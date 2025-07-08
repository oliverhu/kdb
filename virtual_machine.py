from abc import ABC, abstractmethod
from btree import LeafNodeHeader
from cursor import Cursor
from generator import ColumnNameGenerator, LiteralGenerator
from interpreter import Interpreter
from record import Record, deserialize
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
        self.interpreter = Interpreter()

    def run(self,program):
        self.execute(program)

    def execute(self, stmt: Symbol):
        stmt.accept(self)

    def visit_select_stmt(self, stmt: SelectStmt):
        from_clause = stmt.from_clause
        records = self.materialize(from_clause.source.source)
        where_clause = from_clause.where_clause
        generators = self.generate_from_selectables(stmt.select_clause.selectables)
        if where_clause:
            records = self.filter_records(where_clause, records)
        values = []
        for record in records:
            value_list = []
            for generator in generators:
                value_list.append(generator.get_value(record))
            values.append(value_list)
        print(values)

    def visit_select_clause(self, stmt: SelectClause):
        pass

    def visit_selectable(self, stmt: Selectable):
        print("selectable", stmt)

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

    def materialize(self, source):
        # Unwrap until we get an object with 'table_name'
        while not hasattr(source, 'table_name'):
            if isinstance(source, list):
                source = source[0]
            elif hasattr(source, 'source'):
                source = source.source
            elif hasattr(source, 'single_source'):
                source = source.single_source
            else:
                raise ValueError(f"Cannot find table_name in source: {source}")
        table_name = source.table_name
        schema = self.state_manager.schemas[table_name]
        assert table_name in self.state_manager.schemas, f"Table {table_name} not found"

        # Get or create the B-tree for this table
        if table_name not in self.state_manager.trees:
            page_num = self.state_manager.table_pages[table_name]
            from btree import BTree
            tree = BTree(self.state_manager.pager, page_num)
            self.state_manager.trees[table_name] = tree

        tree = self.state_manager.trees[table_name]

                # Use cursor for proper B-tree traversal
        records = []
        cursor = Cursor(self.state_manager.pager, tree)
        cursor.navigate_to_first_leaf_node()

        while not cursor.end_of_table:
            try:
                cell = cursor.get_cell()
                record = deserialize(cell, schema)
                records.append(record)
            except Exception as e:
                print(f"Error deserializing record: {e}")
                continue

            cursor.advance()

        return records

    def filter_records(self, where_clause: WhereClause, records: List[Record]):
        ret_records = []
        for record in records:
            self.interpreter.set_record(record)
            if self.interpreter.evaluate(where_clause.condition):
                ret_records.append(record)
        return ret_records

    def generate_from_selectables(self, selectables: List[Selectable]):
        generators = []
        for selectable in selectables:
            selectable = selectable.value
            if isinstance(selectable, ColumnName):
                generators.append(ColumnNameGenerator(selectable.name))
            elif isinstance(selectable, Literal):
                generators.append(LiteralGenerator(selectable.value))
            else:
                raise ValueError(f"Unsupported selectable: {selectable}")
        return generators
