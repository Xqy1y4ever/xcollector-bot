"""静态自检：**用了但没定义的名字**（改名漏改调用处、except 写了个没导入的异常类……）。

    .\\venv\\Scripts\\python.exe -m tests.check_static

⚠️ 这份文件与 xcollector-client/tests/check_static.py 是**同一份东西**（逐字）。
它是今天两个 bug 的直接产物：

1. client：uild_system_prompt 改名成 
ender_system_prompt，_call_model 里的
   调用处没跟着改 → 每次模型调用都 NameError，被上层吞成"降级为规则抽取"；
2. **本仓库**：_fan_out 里 except BackendRejected，但文件顶部只导入了
   BackendClient, BackendError → 那条分支一旦被走到就是 NameError，
   真实原因（某个用户的 4xx）被顶掉。

共同点：**只有真的执行到那一行才会炸**，所以"把常用路径跑一遍"的测试抓不到；
而它又是纯语法层面的确定性问题 —— 用 ast 扫一遍就够，不需要装 pyflakes。
改一边记得改另一边。

## 判据（宁可漏报，不误报）

一个名字在函数里被**读**，但既不是：局部变量/参数、外层函数（闭包）里的变量、
模块级定义过的名字、内置名字 —— 就报出来。命中一律人工看一眼。
"""

from __future__ import annotations

import ast
import builtins
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

BUILTINS = set(dir(builtins)) | {
    "__file__", "__name__", "__doc__", "__spec__", "__package__", "__debug__",
}

# 这些名字由 Python/框架按约定提供，不算"没定义"
ALLOW = {"self", "cls"}


class Scope:
    def __init__(self, kind: str, parent: "Scope | None"):
        self.kind = kind
        self.parent = parent
        self.defined: set[str] = set()
        self.declared: set[str] = set()   # global / nonlocal


def module_level_names(tree: ast.Module) -> set[str]:
    """模块级**定义过**的名字：def/class/import，以及顶层的赋值与 for/with/except 目标。"""
    names: set[str] = set()
    for node in tree.body:
        for sub in ast.walk(node):
            if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.add(sub.name)
            elif isinstance(sub, (ast.Import, ast.ImportFrom)):
                for alias in sub.names:
                    names.add((alias.asname or alias.name).split(".")[0])
            elif isinstance(sub, ast.Name) and isinstance(sub.ctx, (ast.Store, ast.Del)):
                names.add(sub.id)
            elif isinstance(sub, ast.arg):
                names.add(sub.arg)
            elif isinstance(sub, ast.ExceptHandler) and sub.name:
                names.add(sub.name)
            elif isinstance(sub, (ast.Global, ast.Nonlocal)):
                names.update(sub.names)
            elif isinstance(sub, ast.MatchAs) and sub.name:
                names.add(sub.name)
            elif isinstance(sub, ast.MatchStar) and sub.name:
                names.add(sub.name)
            elif isinstance(sub, ast.MatchMapping) and sub.rest:
                names.add(sub.rest)
    return names


def scan_source(source: str, label: str) -> list[str]:
    """扫一段源码，返回可疑位置。`label` 只用于打印。"""
    try:
        tree = ast.parse(source, filename=label)
    except SyntaxError as exc:
        return [f"{label}:{exc.lineno}: 语法错误：{exc.msg}"]

    module_names = module_level_names(tree)
    problems: list[str] = []
    scopes: list[Scope] = []

    def bind(target: ast.AST, scope: Scope) -> None:
        for node in ast.walk(target):
            if isinstance(node, ast.Name):
                scope.defined.add(node.id)

    def visit(node: ast.AST) -> None:  # noqa: C901 - 就是一棵语法树的分发
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            scope = Scope("function", scopes[-1] if scopes else None)
            args = node.args
            for arg in (
                list(args.posonlyargs) + list(args.args) + list(args.kwonlyargs)
                + [a for a in (args.vararg, args.kwarg) if a]
            ):
                scope.defined.add(arg.arg)
            for deco in node.decorator_list:
                visit(deco)
            for default in list(args.defaults) + [d for d in args.kw_defaults if d]:
                visit(default)
            for ret in [node.returns]:
                if ret is not None:
                    visit(ret)
            scopes.append(scope)
            for child in node.body:
                visit(child)
            scopes.pop()
            return
        if isinstance(node, ast.Lambda):
            scope = Scope("lambda", scopes[-1] if scopes else None)
            for arg in list(node.args.posonlyargs) + list(node.args.args) + list(node.args.kwonlyargs):
                scope.defined.add(arg.arg)
            scopes.append(scope)
            visit(node.body)
            scopes.pop()
            return
        if isinstance(node, ast.ClassDef):
            for deco in node.decorator_list:
                visit(deco)
            scopes.append(Scope("class", scopes[-1] if scopes else None))
            for child in node.body:
                visit(child)
            scopes.pop()
            return
        if isinstance(node, (ast.ListComp, ast.SetComp, ast.GeneratorExp, ast.DictComp)):
            scope = Scope("comprehension", scopes[-1] if scopes else None)
            scopes.append(scope)
            for gen in node.generators:
                visit(gen.iter)
                bind(gen.target, scope)
                for cond in gen.ifs:
                    visit(cond)
            if isinstance(node, ast.DictComp):
                visit(node.key)
                visit(node.value)
            else:
                visit(node.elt)
            scopes.pop()
            return
        if isinstance(node, (ast.Global, ast.Nonlocal)) and scopes:
            scopes[-1].declared.update(node.names)
            return
        if isinstance(node, ast.Name):
            if isinstance(node.ctx, (ast.Store, ast.Del)):
                if scopes:
                    scopes[-1].defined.add(node.id)
                return
            name = node.id
            if name in ALLOW or name in BUILTINS or name in module_names:
                return
            scope = scopes[-1] if scopes else None
            while scope is not None:
                if name in scope.defined or name in scope.declared:
                    return
                scope = scope.parent
            problems.append(
                f"{label}:{node.lineno}: 用了但没定义的名字 `{name}`"
            )
            return
        for child in ast.iter_child_nodes(node):
            visit(child)

    for node in tree.body:
        visit(node)
    return problems


def scan(path: Path) -> list[str]:
    """扫一个文件。utf-8-sig：有的文件带 BOM（PowerShell 的 Set-Content 会加），
    Python import 时本来就是按 utf-8-sig 处理的，这里也要跟上。"""
    source = path.read_text(encoding="utf-8-sig")
    return scan_source(source, str(path.relative_to(ROOT)))


# ---------------------------------------------------------------------------
# 自检：**先证明这个扫描器还能抓东西**，否则"0 命中"可能只是它坏了
# ---------------------------------------------------------------------------

BAD = [
    ("def f():\n    return nope()\n", "函数里调了一个从没出现过的名字"),
    ("def f(x):\n    return x + MISSING\n", "模块级常量没定义"),
    ("try:\n    pass\nexcept NotImported as e:\n    pass\n", "except 了个没导入的异常类"),
]

GOOD = [
    ("import os\n\ndef f():\n    return os.sep\n", "import 过的模块"),
    ("def outer():\n    v = 1\n    def inner():\n        return v\n    return inner\n", "闭包变量"),
    ("def f(xs):\n    return [y * 2 for y in xs]\n", "推导式的目标变量"),
    ("class A:\n    def f(self):\n        return self.x\n", "方法与 self"),
    ("def f():\n    try:\n        pass\n    except ValueError as e:\n        return e\n", "except ... as"),
    ("def f(d):\n    if (n := d.get('n')):\n        return n\n", "海象运算符绑定的名字"),
    ("def f():\n    global G\n    G = 1\n", "global 声明"),
    ("def f(x):\n    match x:\n        case {'a': v}:\n            return v\n        case _:\n            return None\n", "match-case 绑定的名字"),
    ("def f():\n    return len([1, 2])\n", "内置函数"),
    ("def f():\n    return sum(i for i in range(3))\n", "生成器表达式"),
]


def self_test() -> list[str]:
    """扫描器自身的断言。返回失败说明。"""
    bad_failures = [
        f"扫描器漏报了：{why}（{code!r}）" for code, why in BAD if not scan_source(code, "selftest")
    ]
    good_failures = [
        f"扫描器误报了：{why} → {scan_source(code, 'selftest')}"
        for code, why in GOOD
        if scan_source(code, "selftest")
    ]
    return bad_failures + good_failures


def main() -> int:
    print("--- 0. 先证明扫描器本身还能用（否则 0 命中说明不了任何事）---")
    self_problems = self_test()
    for line in self_problems:
        print(f"FAIL  {line}")
    if not self_problems:
        print(f"ok    {len(BAD)} 个「该报的」用例都报了，{len(GOOD)} 个「不该报的」用例都没报")

    files = [
        p
        for base in ("app", "tests")
        for p in sorted((ROOT / base).rglob("*.py"))
        if "__pycache__" not in p.parts
    ]
    problems: list[str] = []
    for file in files:
        problems += scan(file)
    for line in problems:
        print(f"FAIL  {line}")
    print(f"\n扫了 {len(files)} 个文件，可疑 {len(problems)} 处")
    if problems or self_problems:
        print("❌ 有「用了但没定义的名字」—— 这些位置一旦执行到就是 NameError（而测试通常跑不到）")
        return 1
    print("✅ 没有用了但没定义的名字")
    return 0


if __name__ == "__main__":
    sys.exit(main())
