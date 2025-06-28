import re
from lark.ast_utils import camel_to_snake

class Visitor:
    def visit(self, symbol):
        suffix = camel_to_snake(symbol.__class__.__name__)
        handler = f"visit_{suffix}"
        print(f"-> Visiting {symbol} with handler {handler}")
        if hasattr(self, handler):
            return getattr(self, handler)(symbol)
        return self.visit_default(symbol)

    def visit_default(self, symbol):
        raise NotImplementedError(f"No handler for {symbol.__class__.__name__}")

    def visit_program(self, symbol):
        return self.visit_default(symbol)