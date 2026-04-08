# Data Genie - Code Issues Detailed Report
**Scan Date:** 2026-04-08

---

## 🔴 CRITICAL ISSUES

### 1. **EXPOSED API KEY in `.env` file**
**Severity:** CRITICAL  
**File:** `.env` (Line 10)  
**Issue:**
```
OPENAI_API_KEY=sk-proj-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```
**Impact:** Public API key is visible in code repo  
**Fix:** 
- Replace with placeholder: `OPENAI_API_KEY=your_openai_key_here`
- Revoke the exposed key in OpenAI dashboard
- Add `.env` to `.gitignore`

---

### 2. **Missing `.gitignore` entry for `.env`**
**Severity:** CRITICAL  
**File:** `.gitignore` (not found)  
**Issue:** `.env` file containing secrets may be committed to git  
**Fix:** Create/update `.gitignore`:
```
.env
.env.local
*.pyc
__pycache__/
.DS_Store
```

---

## 🟠 HIGH PRIORITY ISSUES

### 3. **Missing Error Handling in `llm_client.py`**
**Severity:** HIGH  
**File:** `core/llm_client.py` (Lines 75-94)  
**Issue:** 
- No validation if liteLLM is properly initialized
- Azure credentials validation doesn't check if api_key format is valid
- No timeout handling for API calls
**Fix:**
```python
# Add validation
if not self.api_key or len(self.api_key) < 10:
    raise ValueError("Invalid API key format")
```

---

### 4. **Azure OpenAI API Version Hardcoded**
**Severity:** HIGH  
**File:** `core/llm_client.py` (Line 93)  
**Issue:**
```python
completion_params["api_version"] = "2024-02-15-preview"  # Hardcoded
```
**Impact:** May break if Azure updates API  
**Fix:** Move to `.env`:
```
AZURE_API_VERSION=2024-02-15-preview
```

---

### 5. **No Timeout/Retry Logic for Skill Execution**
**Severity:** HIGH  
**File:** `core/skill_executor.py`  
**Issue:** Skill execution might hang indefinitely  
**Fix:** Add timeout wrapper:
```python
from timeout_decorator import timeout

@timeout(300)  # 5 minute timeout
def execute_skill(skill_meta, params, callback):
    # ... execution code
```

---

### 6. **Missing Validation for Input/Output Paths**
**Severity:** HIGH  
**File:** `app.py` (Lines 136-142)  
**Issue:** Only checks if paths exist, doesn't validate:
- Write permissions on output folder
- Disk space availability
- Path traversal attacks (e.g., `../../etc/passwd`)

**Fix:**
```python
import os
from pathlib import Path

def validate_path(path_str: str, must_exist: bool = False) -> Path:
    path = Path(path_str).resolve()
    
    # Security: prevent path traversal
    if ".." in path.parts or "~" in str(path):
        raise ValueError("Invalid path: traversal attempts not allowed")
    
    if must_exist and not path.exists():
        raise FileNotFoundError(f"Path not found: {path}")
    
    return path
```

---

## 🟡 MEDIUM PRIORITY ISSUES

### 7. **Skills Have Incomplete Implementation (TODOs)**
**Severity:** MEDIUM  
**Files:**
- `data_copy_skill/tools/data_copy_tool.py` - 2 TODOs
- `discovery_skill/tools/data_discovery_tool.py` - 5 TODOs
- `pandas_pyspark_skill/tools/pandas_pyspark_tool.py` - 2 TODOs
- `pyspark_migrator_skill/tools/pyspark_migrator_tool.py` - 2 TODOs
- `validation_skill/tools/validation_tool.py` - 5 TODOs

**Example:** `data_copy_tool.py`
```python
# TODO: Implement incremental/chunked copy for large files
# TODO: SHA256 or other hash algorithm
```

**Impact:** Large file handling may fail, weak hash algorithms  
**Fix:** Complete the TODO implementations

---

### 8. **No Input Validation in Skill Parameters**
**Severity:** MEDIUM  
**File:** `core/skill_executor.py`  
**Issue:** Skill parameters not validated against schema before execution  
**Fix:** Add schema validation:
```python
from jsonschema import validate, ValidationError

def execute_skill(skill_meta, params, callback):
    schema = skill_meta.get("schema", {})
    try:
        validate(instance=params, schema=schema)
    except ValidationError as e:
        raise ValueError(f"Invalid parameters: {e.message}")
```

---

### 9. **Missing Logging**
**Severity:** MEDIUM  
**Files:** All Python files  
**Issue:** No logging for debugging, only print statements  
**Impact:** Hard to troubleshoot in production  
**Fix:** Add logging:
```python
import logging

logger = logging.getLogger(__name__)
logger.info("Executing skill: %s", skill_name)
logger.error("Skill failed: %s", str(error))
```

---

### 10. **Unused Imports**
**Severity:** MEDIUM  
**File:** `app.py`  
**Issue:**
```python
import time  # Imported but never used
from datetime import datetime  # Imported but never used
```

---

## 🔵 LOW PRIORITY ISSUES

### 11. **Hardcoded Max Iterations**
**Severity:** LOW  
**File:** `agent/graph.py` (Line 197)  
**Issue:**
```python
if state.get("feedback") and state.get("iteration", 0) < 3:
    # Max iterations hardcoded as 3
```
**Fix:** Move to `.env`:
```
MAX_ITERATIONS=3
```

---

### 12. **No Type Hints in Some Functions**
**Severity:** LOW  
**Files:** Multiple skill files  
**Issue:** Missing return type hints  
**Example:** `data_discovery_tool.py`
```python
def run(params):  # Should be: def run(params: Dict) -> Dict:
    pass
```

---

### 13. **Inconsistent Error Messages**
**Severity:** LOW  
**Files:** Various  
**Issue:** Error messages don't follow consistent format  
**Example:**
- `"LLM not configured"` vs `"LLM error: ..."`

---

### 14. **Magic Numbers in Code**
**Severity:** LOW  
**File:** `core/skill_selector.py`  
**Issue:**
```python
top_k=3  # Magic number - what does this mean?
match_score=0.7  # Threshold not documented
```

---

## 📋 SUMMARY TABLE

| Issue | Severity | Status | Fix Time |
|-------|----------|--------|----------|
| Exposed API Key | CRITICAL | ❌ NOT FIXED | 5 min |
| Missing .gitignore | CRITICAL | ❌ NOT FIXED | 5 min |
| LLM Error Handling | HIGH | ⚠️ PARTIAL | 20 min |
| Azure API Hardcoded | HIGH | ❌ NOT FIXED | 10 min |
| Skill Timeout Missing | HIGH | ❌ NOT FIXED | 15 min |
| Path Validation | HIGH | ❌ NOT FIXED | 20 min |
| Incomplete Skills (TODOs) | MEDIUM | ❌ NOT FIXED | 2-3 hours |
| Parameter Validation | MEDIUM | ❌ NOT FIXED | 15 min |
| Missing Logging | MEDIUM | ❌ NOT FIXED | 1 hour |
| Unused Imports | MEDIUM | ✅ EASY | 5 min |

---

## 🎯 RECOMMENDED FIX ORDER

1. **FIRST:** Fix exposed API key + `.gitignore`
2. **SECOND:** Add path validation + parameter validation
3. **THIRD:** Add logging + error handling
4. **FOURTH:** Complete TODO implementations
5. **FIFTH:** Add timeouts + type hints

---

## 🚀 Next Steps

1. Run security scan: `pip install bandit && bandit -r .`
2. Run linter: `pip install pylint && pylint *.py`
3. Run type checker: `pip install mypy && mypy .`
