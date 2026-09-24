"""Small expression language for SQL file arguments; never executes Python code."""
import ast
import operator
from datetime import date, datetime, timedelta


def evaluate_expression(source, arguments):
    if len(source) > 10000:
        raise ValueError('Unsupported expression size')
    tree = ast.parse(source, mode='eval')
    if sum(1 for _ in ast.walk(tree)) > 256:
        raise ValueError('Unsupported expression size')
    constructors = {'date': date, 'datetime': datetime, 'timedelta': timedelta}
    binary = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul,
              ast.Div: operator.truediv, ast.FloorDiv: operator.floordiv, ast.Mod: operator.mod}

    def visit(node):
        value = evaluate(node)
        if isinstance(value, (str, list, tuple, dict)) and len(value) > 10000:
            raise ValueError('Unsupported expression result size')
        if isinstance(value, int) and value.bit_length() > 4096:
            raise ValueError('Unsupported integer size')
        return value

    def evaluate(node):
        if isinstance(node, ast.Constant) and isinstance(node.value, (str, int, float, bool, type(None))):
            return node.value
        if isinstance(node, ast.Name):
            if node.id not in arguments:
                raise NameError(f"Unknown argument: {node.id}")
            return arguments[node.id]
        if isinstance(node, (ast.List, ast.Tuple)):
            values = [visit(item) for item in node.elts]
            return tuple(values) if isinstance(node, ast.Tuple) else values
        if isinstance(node, ast.Dict) and all(key is not None for key in node.keys):
            return {visit(key): visit(value) for key, value in zip(node.keys, node.values)}
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            value = visit(node.operand)
            return +value if isinstance(node.op, ast.UAdd) else -value
        if isinstance(node, ast.BinOp) and type(node.op) in binary:
            left, right = visit(node.left), visit(node.right)
            if isinstance(node.op, (ast.Mult, ast.Div, ast.FloorDiv, ast.Mod)) and not all(
                    isinstance(v, (int, float)) for v in (left, right)):
                raise ValueError('Unsupported non-numeric arithmetic')
            return binary[type(node.op)](left, right)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in constructors:
            if any(keyword.arg is None for keyword in node.keywords):
                raise ValueError('Unsupported keyword expansion')
            return constructors[node.func.id](*[visit(arg) for arg in node.args],
                                              **{kw.arg: visit(kw.value) for kw in node.keywords})
        raise ValueError(f'Unsupported expression: {type(node).__name__}')

    return visit(tree.body)
