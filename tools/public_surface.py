#!/usr/bin/env python3
"""Enumerate the symbols each internal module exports.

`async_postgres.nim` re-exports a reviewed whitelist, but a user can also
`import async_postgres/pg_client/core` and reach everything marked `*` there.
This prints that surface so it stays a decision, not an accident; CI diffs it
against tools/public_surface.txt.

The rules below are an approximation of Nim's grammar, so the last pass is the
one that matters: every `*` that no rule claimed is reported as an error rather
than dropped. A pattern that stops matching is then a failed run instead of a
silently shrinking baseline -- which is the whole point of having a baseline.
"""
import re
import sys
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent

# A name is an identifier or a backquoted operator (`==`, `[]`, `$`).
NAME = r'(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)'

# Indent is not fixed: a `when hasChronos:` branch indents whatever it guards,
# and those declarations are just as reachable. Anchor on declaration syntax
# instead. `type` is in the keyword list because a one-line `type Foo* = ...`
# at column 0 is neither a routine nor a section item.
DECL = re.compile(
    rf'^\s*(?:proc|func|method|template|macro|iterator|converter|const|let|var|type)'
    rf'\s+({NAME})\*'
)
# Section items: `Foo* = ...`, `Foo*[T] = ...`, `Foo* {.pragma.} = ...`.
TYPE = re.compile(rf'^\s*({NAME})\*\s*(?:\[[^\]]*\]\s*)?(?:=|\{{)')
# Object/tuple fields: `foo*: T`, `foo* {.pragma.}: T`, and a variant object's
# discriminator, `case kind*: Kind` -- exported like any other field.
FIELD = re.compile(rf'^\s*(?:case\s+)?({NAME})\*\s*(?:\{{[^}}]*\}}\s*)?:')

RULES = (DECL, FIELD, TYPE)

# Any `<name>*` that starts a declaration: the `*` sits against the name and is
# followed by what can follow an export marker. `a * b` never matches (the star
# is not against the name) and neither does `a*b` (a letter cannot follow).
EXPORT_MARK = re.compile(rf'(?<![A-Za-z0-9_`]){NAME}\*(?=\s*(?:[:={{(\[,*]|$))')

# Per-line approximation: does not track multiline `"""` / `r"""` strings
# or `#[ ]#` block comments across lines. A `*`-like pattern inside such
# a block would be a false positive (fail-closed via `problems`) rather
# than a silent baseline omission, which preserves the safety property.
STRINGS = re.compile(r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'')


def strip_noise(line: str) -> str:
    """Blank out string literals and comments so their text cannot look like code."""
    line = STRINGS.sub('""', line)
    hash_at = line.find('#')
    return line if hash_at < 0 else line[:hash_at]


def unquote(name: str) -> str:
    return name[1:-1] if name.startswith('`') else name


def scan(path: pathlib.Path):
    names, unclaimed = set(), []
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = strip_noise(raw)
        for rx in RULES:
            m = rx.match(line)
            if m:
                names.add(unquote(m.group(1)))
                break
        else:
            # `quote do:` bodies interpolate `\`sym\`` names that are not
            # declarations; the export marker check ignores them because the
            # backquoted name is followed by `(` only after a closing quote.
            if EXPORT_MARK.search(line):
                unclaimed.append((lineno, raw.strip()))
    return names, unclaimed


def main() -> int:
    paths = [ROOT / 'async_postgres.nim'] + sorted(ROOT.glob('async_postgres/**/*.nim'))
    out, problems = [], []
    for path in paths:
        rel = path.relative_to(ROOT).as_posix()
        names, unclaimed = scan(path)
        for lineno, text in unclaimed:
            problems.append(f'{rel}:{lineno}: unclassified export: {text}')
        for n in sorted(names):
            out.append(f'{rel}\t{n}')
    if problems:
        sys.stderr.write(
            'public_surface: these lines export something the rules do not\n'
            'recognise, so the baseline would silently omit them. Teach the\n'
            'rules about them (or fix the declaration):\n\n'
        )
        sys.stderr.write('\n'.join(problems) + '\n')
        return 1
    print('\n'.join(out))
    return 0


if __name__ == '__main__':
    sys.exit(main())
