"""Automatic dependency management for pandas-to-pyspark converter.

Detects missing libraries and installs them automatically.
Provides graceful fallback when dependencies are unavailable.
"""

import subprocess
import sys
import logging
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


class DependencyManager:
    """Manages automatic installation of optional dependencies."""

    # Core dependencies (built-in to Python 3.9+)
    CORE_DEPS = {
        'ast': None,
        're': None,
        'os': None,
        'pathlib': None,
        'typing': None,
        'json': None,
        'datetime': None,
        'logging': None,
    }

    # Optional dependencies with pip names
    OPTIONAL_DEPS = {
        'astor': 'astor',
        'pandas': 'pandas',
    }

    # Spark-related (requires special handling)
    SPARK_DEPS = {
        'pyspark': 'pyspark',
        'databricks': 'databricks-sdk',
    }

    def __init__(self, auto_install: bool = True, progress_callback=None):
        """Initialize dependency manager.

        Args:
            auto_install: If True, automatically install missing optional deps
            progress_callback: Optional callback for progress messages
        """
        self.auto_install = auto_install
        self.callback = progress_callback or (lambda m: None)
        self.installed_deps = {}
        self.missing_deps = {}
        self.available_spark = False

    def check_and_install(self, require_spark: bool = False) -> Tuple[bool, str]:
        """Check and install dependencies.

        Args:
            require_spark: If True, check for PySpark/Databricks availability

        Returns:
            (success: bool, message: str)
        """
        self.callback("📦 Checking dependencies...")

        # Check core dependencies (should all be available)
        missing_core = self._check_deps(self.CORE_DEPS)
        if missing_core:
            return False, f"Missing core dependencies: {missing_core}. Upgrade Python 3.9+"

        # Check optional dependencies
        missing_optional = self._check_deps(self.OPTIONAL_DEPS)
        if missing_optional:
            if self.auto_install:
                self.callback(f"📥 Installing missing packages: {missing_optional}...")
                try:
                    self._install_deps(missing_optional)
                    self.callback(f"✅ Installed: {missing_optional}")
                except Exception as e:
                    self.callback(f"⚠️ Could not auto-install (will continue without): {e}")
                    self.missing_deps.update(missing_optional)
            else:
                self.missing_deps.update(missing_optional)

        # Check Spark/Databricks (optional, but provide feedback)
        if require_spark or True:  # Always check
            self.available_spark = self._check_spark()
            if self.available_spark:
                self.callback("✅ PySpark available - side-by-side testing enabled")
            else:
                self.callback("⚠️ PySpark not available - side-by-side testing disabled")

        return True, "Dependencies ready"

    def _check_deps(self, deps: Dict[str, Optional[str]]) -> List[str]:
        """Check if dependencies are available.

        Returns:
            List of missing dependency names
        """
        missing = []
        for dep_name, pip_name in deps.items():
            try:
                __import__(dep_name)
                self.installed_deps[dep_name] = True
            except ImportError:
                missing.append(pip_name or dep_name)
                self.installed_deps[dep_name] = False

        return missing

    def _check_spark(self) -> bool:
        """Check if PySpark is available.

        Returns:
            True if Spark session can be created
        """
        try:
            from pyspark.sql import SparkSession
            # Try to create a minimal session
            spark = SparkSession.builder \
                .master("local[1]") \
                .appName("dependency_check") \
                .config("spark.driver.memory", "512m") \
                .config("spark.sql.shuffle.partitions", "4") \
                .getOrCreate()
            spark.stop()
            return True
        except Exception as e:
            logger.debug(f"Spark not available: {e}")
            return False

    def _install_deps(self, deps: List[str]):
        """Install dependencies using pip.

        Args:
            deps: List of package names to install

        Raises:
            RuntimeError: If installation fails
        """
        for dep in deps:
            try:
                subprocess.check_call(
                    [sys.executable, "-m", "pip", "install", "--quiet", dep],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                logger.info(f"Installed {dep}")
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"Failed to install {dep}: {e}")

    def has_pandas(self) -> bool:
        """Check if pandas is available."""
        return self.installed_deps.get('pandas', False)

    def has_spark(self) -> bool:
        """Check if PySpark is available."""
        return self.available_spark

    def get_report(self) -> Dict:
        """Get dependency status report.

        Returns:
            Dict with installed and missing dependencies
        """
        return {
            'installed': self.installed_deps,
            'missing': self.missing_deps,
            'spark_available': self.available_spark,
            'auto_install_enabled': self.auto_install,
        }


class SparkSessionManager:
    """Manages PySpark session creation with fallback."""

    def __init__(self, progress_callback=None):
        """Initialize Spark session manager.

        Args:
            progress_callback: Optional callback for progress messages
        """
        self.callback = progress_callback or (lambda m: None)
        self.spark = None
        self.available = False

    def create_session(self, app_name: str = "PandasToSparkConverter",
                       use_databricks: bool = False) -> bool:
        """Create a PySpark session.

        Args:
            app_name: Name for Spark application
            use_databricks: Try to use Databricks Spark Connect

        Returns:
            True if session created successfully, False otherwise
        """
        try:
            from pyspark.sql import SparkSession

            if use_databricks:
                # Try Databricks Spark Connect
                try:
                    self.callback("🔗 Attempting Databricks Spark Connect...")
                    self.spark = self._create_databricks_session(app_name)
                    self.available = True
                    self.callback("✅ Connected to Databricks")
                    return True
                except Exception as e:
                    self.callback(f"⚠️ Databricks Connect failed: {e}")
                    self.callback("🔄 Falling back to local Spark...")

            # Local Spark session
            self.callback("🚀 Creating local Spark session...")
            self.spark = SparkSession.builder \
                .master("local[*]") \
                .appName(app_name) \
                .config("spark.driver.memory", "2g") \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.executor.memory", "1g") \
                .getOrCreate()

            self.available = True
            self.callback("✅ Local Spark session created")
            return True

        except Exception as e:
            self.callback(f"❌ Failed to create Spark session: {e}")
            self.available = False
            return False

    def _create_databricks_session(self, app_name: str):
        """Create Databricks Spark Connect session.

        Args:
            app_name: Application name

        Returns:
            SparkSession connected to Databricks

        Raises:
            Exception: If Databricks credentials not configured
        """
        from pyspark.sql import SparkSession
        import os

        # Check for Databricks config
        workspace_url = os.getenv('DATABRICKS_HOST')
        token = os.getenv('DATABRICKS_TOKEN')

        if not workspace_url or not token:
            # Try to read from ~/.databrickscfg
            import configparser
            config = configparser.ConfigParser()
            config_file = os.path.expanduser('~/.databrickscfg')

            if os.path.exists(config_file):
                config.read(config_file)
                if 'DEFAULT' in config:
                    workspace_url = config['DEFAULT'].get('host')
                    token = config['DEFAULT'].get('token')

        if not workspace_url or not token:
            raise RuntimeError(
                "Databricks credentials not found. "
                "Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables "
                "or configure ~/.databrickscfg"
            )

        # Create Spark Connect session
        remote_url = f"sc://{workspace_url.replace('https://', '').replace('http://', '')}:443/;token={token}"

        return SparkSession.builder \
            .remote(remote_url) \
            .appName(app_name) \
            .getOrCreate()

    def close(self):
        """Close Spark session."""
        if self.spark:
            try:
                self.spark.stop()
                self.callback("✅ Spark session closed")
            except Exception as e:
                self.callback(f"⚠️ Error closing Spark: {e}")

    def is_available(self) -> bool:
        """Check if session is available."""
        return self.available

    def get_session(self):
        """Get Spark session.

        Returns:
            SparkSession or None
        """
        return self.spark if self.available else None
