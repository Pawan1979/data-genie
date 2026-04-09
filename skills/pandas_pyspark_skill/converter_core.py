"""Core converter classes extracted from pandas-to-pyspark converter.

This module contains the fundamental conversion logic:
- PandasDiscovery: Finds pandas operations in code using AST
- PandasToSparkConverter: Converts pandas calls to PySpark equivalents
- _ASTTransformer: AST-based code transformation
- SecureCodeValidator: Validates code security before execution
- FolderConverter: Batch processes hierarchical folder structures
"""

import ast
import re
import os
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any, Callable
from dataclasses import dataclass
import logging
import json

logger = logging.getLogger(__name__)

# ============================================================================
# Constants
# ============================================================================

MAGIC_PATTERNS = [
    r'^\s*!',           # Shell commands: !pip, !ls, etc.
    r'^\s*%',           # Magic commands: %pip, %run, %md, etc.
    r'^\s*# MAGIC',     # Databricks MAGIC comments
    r'^\s*# COMMAND',   # Databricks COMMAND separator
]

SUPPORTED_WRITE_OPS = {'to_csv', 'to_parquet', 'to_json', 'to_excel'}


# ============================================================================
# PandasDiscovery: AST-based pandas operation discovery
# ============================================================================

class PandasDiscovery:
    """Discovers pandas read/write operations and mount points using AST."""

    def __init__(self):
        self.variable_paths: Dict[str, Dict] = {}

    def discover_source(self, source_code: str, file_path: str = '<string>') -> List[Dict]:
        """Discover pandas operations from source code string."""
        findings: List[Dict] = []
        self.variable_paths = {}

        try:
            processed_code, _ = self._preprocess_magic_commands(source_code)
            tree = ast.parse(processed_code)

            # Pass 1: collect variable → path assignments
            for node in ast.walk(tree):
                if (isinstance(node, ast.Assign)
                    and isinstance(node.value, ast.Constant)
                    and isinstance(node.value.value, str)):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            self.variable_paths[target.id] = {
                                'path': node.value.value,
                                'line': node.lineno,
                            }

            # Pass 2: find pandas read/write operations
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    func = node.func

                    # pd.read_*()
                    if (isinstance(func.value, ast.Name)
                        and func.value.id == 'pd'
                        and func.attr.startswith('read_')):
                        path, var = self._extract_path_with_variable(node)
                        findings.append(self._make_finding(
                            file_path, node, f'pandas_{func.attr}', path, var))

                    # df.to_*()
                    elif func.attr.startswith('to_'):
                        path, var = self._extract_path_with_variable(node)
                        if path:
                            findings.append(self._make_finding(
                                file_path, node, f'pandas_{func.attr}', path, var))

                # Mount point references
                if (isinstance(node, ast.Constant)
                    and isinstance(node.value, str)
                    and '/mnt/' in node.value):
                    findings.append({
                        'file_path': file_path,
                        'line_number': node.lineno,
                        'operation_type': 'mount_point_reference',
                        'code_content': f"'{node.value}'",
                        'path_used': node.value,
                        'variable_name': None,
                        'is_mount_point': True,
                    })
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")

        return findings

    @staticmethod
    def _make_finding(file_path, node, op_type, path, variable_name):
        """Create a finding dictionary."""
        return {
            'file_path': file_path,
            'line_number': node.lineno,
            'operation_type': op_type,
            'code_content': ast.unparse(node),
            'path_used': path,
            'variable_name': variable_name,
            'is_mount_point': bool(path and (path.startswith('/mnt/') or path.startswith('/dbfs/mnt/'))),
        }

    @staticmethod
    def _preprocess_magic_commands(source_code: str) -> Tuple[str, Dict[int, str]]:
        """Comment out Databricks magic commands for AST parsing."""
        lines = source_code.split('\n')
        processed, magic_map = [], {}
        for idx, line in enumerate(lines, start=1):
            if any(re.match(p, line) for p in MAGIC_PATTERNS):
                magic_map[idx] = line
                processed.append(f'# {line}' if not line.strip().startswith('#') else line)
            else:
                processed.append(line)
        return '\n'.join(processed), magic_map

    def _extract_path_with_variable(self, call_node: ast.Call) -> Tuple[str, str]:
        """Extract file path from function call, resolving variables."""
        # Check positional arg
        if call_node.args:
            arg = call_node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                return arg.value, None
            if isinstance(arg, ast.Name):
                var = arg.id
                info = self.variable_paths.get(var)
                return (info['path'], var) if info else ('', var)
            if isinstance(arg, ast.BinOp):
                resolved = self._resolve_binop(arg)
                if resolved:
                    return resolved, 'expression'

        # Check keyword args
        for kw in call_node.keywords:
            if kw.arg in ('path', 'filepath_or_buffer', 'path_or_buf'):
                if isinstance(kw.value, ast.Constant):
                    return kw.value.value, None
                if isinstance(kw.value, ast.Name):
                    var = kw.value.id
                    info = self.variable_paths.get(var)
                    return (info['path'], var) if info else ('', var)

        return '', None

    def _resolve_binop(self, node: ast.BinOp) -> str:
        """Resolve binary operations (path concatenation)."""
        try:
            if isinstance(node.op, ast.Add):
                left = self._resolve_value(node.left)
                right = self._resolve_value(node.right)
                if left and right:
                    return left + right
        except Exception:
            pass
        return ''

    def _resolve_value(self, node) -> str:
        """Resolve a value node."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name) and node.id in self.variable_paths:
            return self.variable_paths[node.id]['path']
        return ''


# ============================================================================
# PandasToSparkConverter: Core conversion engine
# ============================================================================

class PandasToSparkConverter:
    """Converts pandas read/write calls to PySpark equivalents.

    All conversion logic, option mapping, and path translation happens here.
    """

    # Read option mapping: pandas → Spark
    PANDAS_TO_SPARK_READ_OPTIONS = {
        'sep': 'sep', 'delimiter': 'sep', 'header': 'header',
        'encoding': 'encoding', 'quotechar': 'quote', 'escapechar': 'escape',
        'comment': 'comment', 'na_values': 'nullValue', 'compression': 'compression',
        'sheet_name': 'sheetName',
        'lines': 'multiLine',
        # Unsupported (silently skipped)
        'names': None, 'index_col': None, 'usecols': None, 'dtype': None,
        'skiprows': None, 'nrows': None, 'orient': None, 'engine': None,
        'columns': None,
    }

    # Write option mapping: pandas → Spark
    PANDAS_TO_SPARK_WRITE_OPTIONS = {
        'sep': 'sep', 'delimiter': 'sep', 'header': 'header',
        'encoding': 'encoding', 'quotechar': 'quote', 'escapechar': 'escape',
        'na_rep': 'nullValue', 'compression': 'compression',
        'lineterminator': 'lineSep',
        'sheet_name': 'sheetName',
        # Unsupported (silently skipped)
        'index': None, 'index_label': None, 'mode': None,
        'columns': None, 'float_format': None, 'decimal': None,
        'date_format': None, 'doublequote': None, 'quoting': None,
        'chunksize': None, 'storage_options': None, 'orient': None,
        'lines': None, 'engine': None,
    }

    PANDAS_TO_SPARK_OPTIONS = PANDAS_TO_SPARK_READ_OPTIONS  # backward-compatible alias

    def __init__(self, path_mapping: Dict[str, str] = None):
        self.spark_var = 'spark'
        self.path_mapping = path_mapping or {}

    def convert(self, source_code: str) -> str:
        """Convert source code: try AST first, fall back to line-by-line."""
        try:
            return self._convert_with_ast(source_code)
        except Exception:
            return self._convert_line_by_line(source_code)

    def apply_path_mapping(self, path_str: str) -> str:
        """Apply path mapping transformations."""
        for old, new in self.path_mapping.items():
            if old in path_str:
                path_str = path_str.replace(old, new)
        return path_str

    def map_read_options(self, pandas_kwargs: Dict[str, str], operation: str) -> Dict[str, str]:
        """Map pandas read kwargs to Spark options."""
        spark_opts: Dict[str, str] = {}
        for key, raw_value in pandas_kwargs.items():
            spark_key = self.PANDAS_TO_SPARK_READ_OPTIONS.get(key)
            if spark_key is None:
                continue
            value = raw_value.strip("'\"")
            if key == 'header':
                spark_opts['header'] = 'true' if raw_value in ('0', 'True', 'true') else 'false'
            elif key in ('sep', 'delimiter'):
                spark_opts['sep'] = value
            elif key == 'lines' and operation == 'read_json':
                spark_opts['multiLine'] = 'false' if raw_value.lower() == 'true' else 'true'
            else:
                spark_opts[spark_key] = value
        return spark_opts

    map_options = map_read_options  # backward-compatible alias

    def map_write_options(self, pandas_kwargs: Dict[str, str], operation: str) -> Dict[str, str]:
        """Map pandas write kwargs to Spark options."""
        spark_opts: Dict[str, str] = {}
        for key, raw_value in pandas_kwargs.items():
            spark_key = self.PANDAS_TO_SPARK_WRITE_OPTIONS.get(key)
            if spark_key is None:
                continue
            value = raw_value.strip("'\"")
            if key == 'header':
                spark_opts['header'] = 'true' if raw_value.lower() in ('true', '1') else 'false'
            elif key in ('sep', 'delimiter'):
                spark_opts['sep'] = value
            else:
                spark_opts[spark_key] = value
        return spark_opts

    def convert_read(self, node: ast.Call, operation: str) -> str:
        """Build spark.read... statement from AST Call node."""
        path = self.apply_path_mapping(ast.unparse(node.args[0])) if node.args else ''
        kwargs = {kw.arg: ast.unparse(kw.value) for kw in node.keywords if kw.arg}
        return self._build_read_statement(operation, path, self.map_read_options(kwargs, operation))

    def convert_write(self, node: ast.Call, operation: str) -> str:
        """Build spark.createDataFrame(...).write... statement from AST Call node."""
        df_name = (node.func.value.id if isinstance(node.func.value, ast.Name)
                   else ast.unparse(node.func.value))
        path = self.apply_path_mapping(ast.unparse(node.args[0])) if node.args else ''
        kwargs = {kw.arg: ast.unparse(kw.value) for kw in node.keywords if kw.arg}
        spark_opts = self.map_write_options(kwargs, operation)
        write_mode = self._detect_write_mode(kwargs)
        return self._build_write_statement(operation, df_name, path, spark_opts, write_mode)

    def _build_read_statement(self, operation: str, path: str, opts: Dict[str, str]) -> str:
        """Build read statement for each format."""
        fmt = self._format_options
        if operation == 'read_csv':
            opts.setdefault('header', 'true')
            opts.setdefault('inferSchema', 'true')
            return f"{self.spark_var}.read{fmt(opts)}.csv({path}).toPandas()"
        if operation == 'read_parquet':
            return f"{self.spark_var}.read{fmt(opts)}.parquet({path}).toPandas()"
        if operation == 'read_json':
            opts.setdefault('multiLine', 'true')
            return f"{self.spark_var}.read{fmt(opts)}.json({path}).toPandas()"
        if operation == 'read_excel':
            opts.setdefault('header', 'true')
            return f"{self.spark_var}.read.format('excel'){fmt(opts)}.load({path}).toPandas()"
        return f"{self.spark_var}.read.load({path}).toPandas()"

    def _build_write_statement(self, operation: str, df_name: str, path: str,
                               opts: Dict[str, str], mode: str = 'overwrite') -> str:
        """Build write statement for each format."""
        sv = self.spark_var
        fmt = self._format_options
        base = f"{sv}.createDataFrame({df_name}).write.mode('{mode}')"

        if operation == 'to_csv':
            opts.setdefault('header', 'true')
            return f"{base}{fmt(opts)}.csv({path})"
        if operation == 'to_parquet':
            return f"{base}{fmt(opts)}.parquet({path})"
        if operation == 'to_json':
            return f"{base}{fmt(opts)}.json({path})"
        if operation == 'to_excel':
            opts.setdefault('header', 'true')
            return f"{base}.format('excel'){fmt(opts)}.save({path})"
        return f"{base}{fmt(opts)}.save({path})"

    @staticmethod
    def _detect_write_mode(pandas_kwargs: Dict[str, str]) -> str:
        """Detect write mode from pandas mode kwarg."""
        raw = pandas_kwargs.get('mode', "'w'").strip("'\"")
        return 'append' if raw == 'a' else 'overwrite'

    @staticmethod
    def _format_options(options: Dict[str, str]) -> str:
        """Format options as .option() chain."""
        return ''.join(f".option('{k}', '{v}')" for k, v in options.items())

    def _convert_with_ast(self, source_code: str) -> str:
        """Convert using AST (preserves formatting and comments)."""
        processed, _ = PandasDiscovery._preprocess_magic_commands(source_code)
        tree = ast.parse(processed)

        replacements = []
        for node in ast.walk(tree):
            call_node, is_assign = None, False

            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                call_node, is_assign = node.value, True
            elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                call_node = node.value

            if not call_node or not isinstance(getattr(call_node, 'func', None), ast.Attribute):
                continue

            func = call_node.func
            converted = None

            # pd.read_*()
            if (isinstance(func.value, ast.Name) and func.value.id == 'pd'
                    and func.attr.startswith('read_')):
                try:
                    expr = self.convert_read(call_node, func.attr)
                    converted = (f'{", ".join(ast.unparse(t) for t in node.targets)} = {expr}'
                                 if is_assign else expr)
                except Exception:
                    continue

            # df.to_*()
            elif func.attr in SUPPORTED_WRITE_OPS:
                try:
                    converted = self.convert_write(call_node, func.attr)
                except Exception:
                    continue

            if converted is None:
                continue

            # Validate syntax
            try:
                ast.parse(converted)
            except SyntaxError:
                continue

            replacements.append((node.lineno, node.end_lineno, converted))

        if not replacements:
            return source_code

        # Apply replacements in reverse order
        lines = source_code.split('\n')
        for start, end, code in sorted(replacements, key=lambda r: r[0], reverse=True):
            orig = lines[start - 1]
            indent = orig[:len(orig) - len(orig.lstrip())]
            lines[start - 1:end] = [indent + code]

        return '\n'.join(lines)

    def _convert_line_by_line(self, source_code: str) -> str:
        """Fallback: line-by-line conversion."""
        return '\n'.join(self._transform_line(line) for line in source_code.split('\n'))

    def _transform_line(self, line: str) -> str:
        """Transform a single line."""
        stripped = line.strip()
        if (not stripped or stripped.startswith('#')
            or any(re.match(p, line) for p in MAGIC_PATTERNS)):
            return line

        code_part, comment = self._split_code_and_comment(line)
        indent = line[:len(line) - len(line.lstrip())]
        transformed = self._transform_single_statement(code_part.strip())

        return (f'{indent}{transformed}  {comment}' if comment
                else f'{indent}{transformed}')

    def _transform_single_statement(self, code: str) -> str:
        """Parse and convert a single statement."""
        try:
            tree = ast.parse(code, mode='exec')
            if len(tree.body) != 1:
                return code
            stmt = tree.body[0]

            call_node = None
            if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Call):
                call_node = stmt.value
            elif isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
                call_node = stmt.value

            if call_node and isinstance(call_node.func, ast.Attribute):
                func = call_node.func
                converted = None

                if (isinstance(func.value, ast.Name) and func.value.id == 'pd'
                        and func.attr.startswith('read_')):
                    converted = self.convert_read(call_node, func.attr)
                elif func.attr in SUPPORTED_WRITE_OPS:
                    converted = self.convert_write(call_node, func.attr)

                if converted is not None:
                    if isinstance(stmt, ast.Assign):
                        targets = ', '.join(ast.unparse(t) for t in stmt.targets)
                        return f'{targets} = {converted}'
                    return converted
        except Exception:
            pass

        return code

    @staticmethod
    def _split_code_and_comment(line: str) -> Tuple[str, str]:
        """Split line into code and comment parts."""
        in_str, char, esc = False, None, False
        for i, c in enumerate(line):
            if esc:
                esc = False
                continue
            if c == '\\':
                esc = True
                continue
            if c in ('"', "'") and not in_str:
                in_str, char = True, c
            elif c == char and in_str:
                in_str, char = False, None
            elif c == '#' and not in_str:
                return line[:i].rstrip(), line[i:]
        return line, ''


# ============================================================================
# _ASTTransformer: AST-based transformation (thin wrapper)
# ============================================================================

class _ASTTransformer(ast.NodeTransformer):
    """AST transformer that delegates to PandasToSparkConverter."""

    def __init__(self, converter: PandasToSparkConverter):
        self._conv = converter

    def visit_Call(self, node: ast.Call):
        if isinstance(node.func, ast.Attribute):
            func = node.func

            # pd.read_*()
            if (isinstance(func.value, ast.Name) and func.value.id == 'pd'
                    and func.attr.startswith('read_')):
                try:
                    return ast.parse(self._conv.convert_read(node, func.attr), mode='eval').body
                except Exception:
                    pass

            # df.to_*()
            elif func.attr in SUPPORTED_WRITE_OPS:
                try:
                    return ast.parse(self._conv.convert_write(node, func.attr), mode='eval').body
                except Exception:
                    pass

        return self.generic_visit(node)


# ============================================================================
# SecureCodeValidator: Pre-execution security checks
# ============================================================================

class SecureCodeValidator:
    """Validates code via AST before execution to block unsafe operations."""

    _ALLOWED_MODULES = {'pd', 'spark', 'np'}
    _DANGEROUS_FUNCS = {'eval', 'exec', 'compile', '__import__', 'open'}
    _DANGEROUS_ATTRS = {'system', 'popen', 'remove', 'rmdir', 'unlink'}

    @staticmethod
    def is_safe_to_execute(code: str) -> Tuple[bool, str]:
        """Check if code is safe to execute."""
        try:
            tree = ast.parse(code, mode='eval')
        except SyntaxError:
            return False, 'Invalid Python syntax'
        except Exception as e:
            return False, f'Validation error: {e}'

        if not isinstance(tree.body, ast.Call):
            return False, 'Code must be a single function call'

        call = tree.body
        if not isinstance(call.func, ast.Attribute):
            return False, 'Not a valid pandas/spark read operation'
        if not isinstance(call.func.value, ast.Name):
            return False, 'Not a valid pandas/spark read operation'

        module = call.func.value.id
        operation = call.func.attr

        if module not in SecureCodeValidator._ALLOWED_MODULES:
            return False, f"Module '{module}' not allowed"
        if not operation.startswith('read_'):
            return False, f"Operation '{operation}' not allowed (only read operations)"

        # Check for dangerous constructs
        for node in ast.walk(call):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id in SecureCodeValidator._DANGEROUS_FUNCS:
                    return False, f"Dangerous function '{node.func.id}' detected"
                if isinstance(node.func, ast.Attribute) and node.func.attr in SecureCodeValidator._DANGEROUS_ATTRS:
                    return False, f"Dangerous operation '{node.func.attr}' detected"
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                return False, 'Import statements not allowed'

        return True, 'Safe'


# ============================================================================
# FolderConverter: Hierarchical batch processing
# ============================================================================

class FolderConverter:
    """Process entire folder hierarchies, converting all Python files."""

    def __init__(self, path_mapping: Dict[str, str] = None,
                 progress_callback: Optional[Callable] = None,
                 auto_consolidate: bool = True):
        """Initialize folder converter.

        Args:
            path_mapping: Optional path mappings
            progress_callback: Optional callback for progress
            auto_consolidate: If True, create consolidated folder
        """
        self.path_mapping = path_mapping or {}
        self.callback = progress_callback or (lambda m: None)
        self.auto_consolidate = auto_consolidate
        self.converter = PandasToSparkConverter(self.path_mapping)
        self.discovery = PandasDiscovery()

    def convert_folder(self, input_folder: str, output_folder: str,
                       recursive: bool = True) -> Dict:
        """Convert all Python files in folder hierarchy.

        Args:
            input_folder: Root folder to process
            output_folder: Root output folder
            recursive: If True, process subdirectories

        Returns:
            Dict with results and statistics
        """
        input_path = Path(input_folder)
        output_path = Path(output_folder)

        if not input_path.exists():
            raise FileNotFoundError(f"Input folder not found: {input_folder}")

        if not input_path.is_dir():
            raise NotADirectoryError(f"Not a directory: {input_folder}")

        self.callback(f"📁 Processing folder: {input_folder}")
        output_path.mkdir(parents=True, exist_ok=True)

        # Find all Python files
        if recursive:
            py_files = sorted(input_path.rglob('*.py'))
        else:
            py_files = sorted(input_path.glob('*.py'))

        self.callback(f"📊 Found {len(py_files)} Python files")

        if not py_files:
            self.callback("⚠️ No Python files found")
            return {
                'total_files': 0,
                'successful': 0,
                'failed': 0,
                'skipped': 0,
                'results': [],
                'summary': 'No Python files found',
            }

        # Convert each file
        results = []
        successful = 0
        failed = 0
        skipped = 0

        for idx, py_file in enumerate(py_files, 1):
            try:
                relative_path = py_file.relative_to(input_path)
                self.callback(f"[{idx}/{len(py_files)}] Converting {relative_path}...")

                # Create output directory structure
                output_file_dir = output_path / relative_path.parent
                output_file_dir.mkdir(parents=True, exist_ok=True)

                output_file = output_file_dir / py_file.name

                # Read source
                source_code = py_file.read_text(encoding='utf-8')

                # Convert
                converted_code = self.converter.convert(source_code)

                # Discover operations
                discoveries = self.discovery.discover_source(
                    source_code, str(relative_path))

                # Write converted file
                output_file.write_text(converted_code, encoding='utf-8')

                # Save report
                report_file = output_file.with_suffix('.conversion_report.json')
                report = {
                    'source_file': str(relative_path),
                    'output_file': str(output_file.relative_to(output_path)),
                    'operations_found': len(discoveries),
                    'changes_made': len([
                        l1 != l2
                        for l1, l2 in zip(source_code.split('\n'),
                                         converted_code.split('\n'))
                        if l1.strip() and l2.strip()
                    ]),
                }
                report_file.write_text(json.dumps(report, indent=2))

                results.append({
                    'file': str(relative_path),
                    'status': 'success',
                    'output': str(output_file.relative_to(output_path)),
                })
                successful += 1
                self.callback(f"  ✅ Converted")

            except Exception as e:
                failed += 1
                results.append({
                    'file': str(relative_path),
                    'status': 'error',
                    'error': str(e),
                })
                self.callback(f"  ❌ Error: {e}")

        # Save batch report
        batch_report = {
            'timestamp': __import__('datetime').datetime.now().isoformat(),
            'input_folder': str(input_path),
            'output_folder': str(output_path),
            'total_files': len(py_files),
            'successful': successful,
            'failed': failed,
            'skipped': skipped,
            'file_results': results,
        }

        report_file = output_path / '_batch_conversion_report.json'
        report_file.write_text(json.dumps(batch_report, indent=2))

        # Create consolidated folder with only converted PySpark files (if enabled)
        consolidated_count = 0
        consolidated_folder = None

        if self.auto_consolidate:
            self.callback("[*] Creating consolidated PySpark files folder...")
            consolidated_folder = output_path / 'converted_pyspark_files'
            consolidated_folder.mkdir(parents=True, exist_ok=True)

            import shutil
            for result in results:
                if result['status'] == 'success':
                    source_file = Path(result['output'])
                    source_full_path = output_path / source_file.parent / source_file.name
                    dest_full_path = consolidated_folder / source_file.name

                    if source_full_path.exists():
                        shutil.copy2(source_full_path, dest_full_path)
                        consolidated_count += 1
                        self.callback(f"  [OK] Copied {source_file.name}")

        summary = f"""# Folder Conversion Report

**Total Files:** {len(py_files)}
- [OK] Successful: {successful}
- [FAIL] Failed: {failed}
- [SKIP] Skipped: {skipped}

**Batch Report:** _batch_conversion_report.json

## Output Structure
- **Individual folders:** Each file has its own folder with conversion reports
  - test_basic_read_write/ - Contains converted file + reports
  - test_advanced_operations/ - Contains converted file + reports
  - etc.

- **Consolidated folder:** converted_pyspark_files/
  - Contains {consolidated_count} converted PySpark files only (no reports)
  - Ready to use directly or copy to your project

All converted files are in: {str(output_path)}
Consolidated PySpark files: {str(consolidated_folder)}
Each individual file has a corresponding .conversion_report.json
"""

        self.callback("\n" + "="*70)
        self.callback(f"Conversion complete: {successful}/{len(py_files)} successful")
        self.callback(f"[*] Consolidated PySpark files: {str(consolidated_folder)}")
        self.callback("="*70)

        return {
            'total_files': len(py_files),
            'successful': successful,
            'failed': failed,
            'skipped': skipped,
            'results': results,
            'summary': summary,
            'consolidated_folder': str(consolidated_folder),
            'consolidated_file_count': consolidated_count,
        }
