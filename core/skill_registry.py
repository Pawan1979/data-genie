"""Skill registry — scans skills/ directory and builds SKILL.md index."""

import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime


class SkillRegistry:
    """Manages skill discovery and metadata."""

    def __init__(self, skills_dir: Path = None):
        """Initialize with skills directory path."""
        if skills_dir is None:
            skills_dir = Path(__file__).parent.parent / "skills"
        self.skills_dir = Path(skills_dir)
        self.registry_file = self.skills_dir.parent / "skill_registry.json"
        self.registry: Dict = {}

    def build_registry(self) -> Dict:
        """Scan skills/ and build registry from SKILL.md files."""
        self.registry = {}

        # Find all skill directories (direct subdirs of skills/)
        if not self.skills_dir.exists():
            return self.registry

        for skill_dir in self.skills_dir.iterdir():
            if not skill_dir.is_dir() or skill_dir.name.startswith("_"):
                continue

            skill_md = skill_dir / "SKILL.md"
            schema_json = skill_dir / "schema.json"

            if not skill_md.exists():
                continue

            # Parse SKILL.md
            skill_meta = self._parse_skill_md(skill_md)
            if skill_meta:
                # Add paths
                skill_meta["md_path"] = str(skill_md)
                skill_meta["schema_path"] = str(schema_json) if schema_json.exists() else None
                skill_meta["last_modified"] = datetime.fromtimestamp(
                    skill_md.stat().st_mtime
                ).isoformat()
                skill_meta["skill_dir"] = str(skill_dir)

                # Determine entry point module path from entry_point string
                # entry_point format: "filename.py :: run(...)"
                entry_point_str = skill_meta.get("entry_point", "")
                if entry_point_str:
                    wrapper_filename = entry_point_str.split("::")[0].strip()
                    wrapper_py = skill_dir / wrapper_filename
                    if wrapper_py.exists():
                        skill_meta["entry_module"] = str(wrapper_py)

                self.registry[skill_meta["name"]] = skill_meta

        # Write registry file
        self._write_registry()
        return self.registry

    def load_registry(self) -> Dict:
        """Load registry from disk, rebuilding if needed."""
        # Check if registry exists and is up-to-date
        if self.registry_file.exists():
            self.registry = json.loads(self.registry_file.read_text())
            registry_mtime = self.registry_file.stat().st_mtime

            # Check if any SKILL.md is newer than registry
            needs_rebuild = False
            for skill_dir in self.skills_dir.iterdir():
                if skill_dir.is_dir() and not skill_dir.name.startswith("_"):
                    skill_md = skill_dir / "SKILL.md"
                    if skill_md.exists() and skill_md.stat().st_mtime > registry_mtime:
                        needs_rebuild = True
                        break

            if not needs_rebuild:
                return self.registry

        # Rebuild if not present or out of date
        return self.build_registry()

    def _parse_skill_md(self, path: Path) -> Optional[Dict]:
        """Parse SKILL.md into a metadata dict."""
        content = path.read_text()
        lines = content.split("\n")

        skill_meta = {
            "name": path.parent.name,  # default to dir name
            "description": "",
            "intent_keywords": [],
            "when_to_use": "",
            "entry_point": "",
            "inputs": {},
            "outputs": {},
        }

        current_section = None

        for line in lines:
            line = line.rstrip()

            # Section headers
            if line.startswith("# "):
                skill_meta["name"] = line[2:].strip()
            elif line.startswith("## "):
                current_section = line[3:].strip().lower()
            elif line.startswith("###"):
                # Skip subsections for now
                pass
            elif current_section and line.strip():
                # Content lines
                if current_section == "description":
                    skill_meta["description"] += line + " "
                elif current_section == "intent_keywords":
                    # Parse comma-separated keywords
                    if line.startswith("-"):
                        line = line[1:].strip()
                    keywords = [kw.strip() for kw in line.split(",")]
                    skill_meta["intent_keywords"].extend(keywords)
                elif current_section == "entry_point":
                    skill_meta["entry_point"] = line.strip()
                elif current_section == "when_to_use":
                    skill_meta["when_to_use"] += line + " "

        # Clean up
        skill_meta["description"] = skill_meta["description"].strip()
        skill_meta["when_to_use"] = skill_meta["when_to_use"].strip()
        skill_meta["intent_keywords"] = [
            kw.strip() for kw in skill_meta["intent_keywords"] if kw.strip()
        ]

        return skill_meta

    def _write_registry(self):
        """Write registry to skill_registry.json."""
        self.registry_file.parent.mkdir(parents=True, exist_ok=True)
        self.registry_file.write_text(json.dumps(self.registry, indent=2))

    def get_skill(self, skill_name: str) -> Optional[Dict]:
        """Get a single skill's metadata."""
        return self.registry.get(skill_name)

    def list_skills(self) -> List[str]:
        """List all skill names."""
        return list(self.registry.keys())


def build_registry() -> Dict:
    """Top-level function to build and save registry."""
    registry = SkillRegistry()
    return registry.build_registry()


def load_registry() -> Dict:
    """Top-level function to load registry."""
    registry = SkillRegistry()
    return registry.load_registry()


if __name__ == "__main__":
    # CLI entry point: rebuild registry
    reg = build_registry()
    print(f"[OK] Built registry with {len(reg)} skills")
    for name in sorted(reg.keys()):
        print(f"  - {name}")
