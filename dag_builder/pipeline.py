"""Data pipeline runners for DLT pipelines without Airflow."""

import dlt
import os
import sys
import subprocess
import json
from typing import Optional, Dict, Any
from .config import PipelineConfig, DbtSchema
from .fetcher import RestApiFetcher
from .logger import DagBuilderLogger
from .target import DuckDBTarget
from pydantic import ValidationError


logger = DagBuilderLogger.get_logger(__name__)


class DataPipeline:
    """Simple data pipeline runner for DLT pipelines."""

    def __init__(self, config_path: str):
        """
        Initialize the pipeline with configuration.
        
        Args:
            config_path: Path to the YAML configuration file
        """
        self.config_path = config_path
        self.cfg = PipelineConfig(config_path)
        
    def run_rest_api_to_duckdb(self, working_dir: str = None):
        """
        Run REST API to DuckDB pipeline.
        
        Args:
            working_dir: Directory to change to before running (for DLT config)
        """
        logger.info("Starting REST API to DuckDB pipeline; config_path=%s", self.config_path)

        # Store original working directory
        original_cwd = os.getcwd()
        print(f"Original working directory: {original_cwd}")
        
        try:
            # Change working directory if specified
            if working_dir:
                os.chdir(working_dir)
                print(f"Changed working directory to: {working_dir}")

            # 1. Setup fetcher
            fetcher = RestApiFetcher(
                url=str(self.cfg.get('api_url')),
                token=self.cfg.api_token,
                params=self.cfg.get('api_params', {}),
                headers=self.cfg.get('api_headers', {}),
                pagination_type=self.cfg.get('pagination_type', 'offset')
            )

            # 2. Setup target
            target_config = self.cfg.get('duckdb_config', {})
            if 'destination_name' in target_config:
                target = DuckDBTarget(
                    # destination_name=target_config.get('destination_name', self.cfg.get('pipeline_name') + '.duckdb'),
                    read_only=target_config.get('read_only', False)
                )
            else:
                # Default to in-memory for DuckDB
                target = DuckDBTarget(memory=True)

            # 3. DLT Pipeline Initialization
            logger.info("Initializing dlt pipeline for dag_id=%s", self.cfg.get('dag_id'))
            
            # Ensure the DuckDB data directory exists if specified
            if hasattr(target, 'destination_name') and target.destination_name:
                duckdb_data_dir = os.path.dirname(target.destination_name)
                os.makedirs(duckdb_data_dir, exist_ok=True)
            
            pipeline = dlt.pipeline(
                pipeline_name=self.cfg.get('pipeline_name'),
                # destination="duckdb",
                destination=dlt.destinations.duckdb(
                    destination_name=target_config.get('destination_name', self.cfg.get('pipeline_name') + '.duckdb')
                ),


                # database=target_config.get('destination_name', self.cfg.get('pipeline_name') + '.duckdb'),
                dataset_name=target_config.get('dataset_name', self.cfg.get('pipeline_name'))
            )

            # 4. Resource Definition with Incremental Loading
            resource = dlt.resource(
                fetcher.fetch_records(
                    dlt.sources.incremental(self.cfg.get('incremental_cursor'))
                ),
                name=self.cfg.get('table_name'),
                write_disposition="merge",
                primary_key="id"
            )

            # 5. Run the pipeline
            logger.info("Running data pipeline...")
            load_info = pipeline.run(resource)
            
            logger.info("Load complete: %s", load_info)
            
            # Return information about where data was stored
            result = {
                'pipeline_name': self.cfg.get('pipeline_name'),
                'load_info': load_info,
                'destination_name': getattr(target, 'destination_name', None),
                'table_name': self.cfg.get('table_name')
            }
            
            return result

        finally:
            # Restore original working directory
            os.chdir(original_cwd)


class DbtTransformer:
    """dbt transformation runner with configurable project location."""

    def __init__(self, dbt_config: Optional[Dict[str, Any]] = None):
        """
        Initialize the dbt transformer with configuration.
        
        Args:
            dbt_config: Dictionary with dbt configuration (project_dir, profiles_dir, etc.)
                       If None, uses current working directory as default project_dir
        """
        self.dbt_config = dbt_config or {}
        
        # Validate configuration with Pydantic
        try:
            self.config = DbtSchema(**self.dbt_config)
        except ValidationError as e:
            logger.error("Invalid dbt configuration: %s", e)
            raise ValueError(f"Invalid dbt configuration:\n{e}") from e
        
        # Set default project_dir to current working directory if not specified
        if not self.config.project_dir:
            self.config.project_dir = os.getcwd()
            logger.debug("Using current working directory as dbt project_dir: %s", 
                        self.config.project_dir)
        
        logger.info("Initialized DbtTransformer with project_dir=%s", self.config.project_dir)

    def run(self, command: str = "run", working_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute dbt command in the configured project.
        
        Args:
            command: dbt command to run (e.g. "run", "test", "build", "compile")
            working_dir: Optional directory to change to before running. 
                        Defaults to None (uses project_dir from config)
        
        Returns:
            Dictionary with execution results:
                - 'success': bool indicating if command succeeded
                - 'return_code': int exit code
                - 'stdout': command output
                - 'stderr': error output (if any)
                - 'dbt_project_dir': the project directory used
        
        Raises:
            FileNotFoundError: If dbt project directory doesn't exist
            RuntimeError: If dbt command fails
        """
        project_dir = self.config.project_dir
        original_cwd = os.getcwd()
        
        # Validate project directory exists
        if not os.path.isdir(project_dir):
            logger.error("dbt project directory not found: %s", project_dir)
            raise FileNotFoundError(f"dbt project directory not found: {project_dir}")
        
        try:
            # Change to working directory if specified, otherwise use project_dir
            target_dir = working_dir or project_dir
            if target_dir != original_cwd:
                os.chdir(target_dir)
                logger.debug("Changed working directory to: %s", target_dir)
            
            # Build dbt command
            dbt_cmd = self._build_dbt_command(command)
            
            logger.info("Running dbt command: %s in %s", " ".join(dbt_cmd), os.getcwd())
            
            # Execute dbt command using subprocess
            result = subprocess.run(
                dbt_cmd,
                cwd=project_dir,
                capture_output=True,
                text=True,
                check=False  # Don't raise exception, we handle return code
            )
            
            success = result.returncode == 0
            
            # Parse dbt JSON output if available
            dbt_metadata = self._parse_dbt_output(result.stdout)
            
            output_dict = {
                'success': success,
                'return_code': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'dbt_project_dir': project_dir,
                'command': command,
                'dbt_metadata': dbt_metadata
            }
            
            if success:
                logger.info("dbt command succeeded: %s", command)
            else:
                logger.error("dbt command failed with return code %d: %s", 
                           result.returncode, result.stderr)
                raise RuntimeError(f"dbt {command} failed with return code {result.returncode}\n{result.stderr}")
            
            return output_dict
            
        finally:
            # Restore original working directory
            os.chdir(original_cwd)

    def run_compile(self, working_dir: Optional[str] = None) -> Dict[str, Any]:
        """Run dbt compile."""
        return self.run("compile", working_dir)

    def run_test(self, working_dir: Optional[str] = None) -> Dict[str, Any]:
        """Run dbt test."""
        return self.run("test", working_dir)

    def run_build(self, working_dir: Optional[str] = None) -> Dict[str, Any]:
        """Run dbt build (compile + run + test)."""
        return self.run("build", working_dir)

    def run_snapshot(self, working_dir: Optional[str] = None) -> Dict[str, Any]:
        """Run dbt snapshot."""
        return self.run("snapshot", working_dir)

    def _build_dbt_command(self, command: str) -> list:
        """
        Build the dbt command with all configured options.
        
        Args:
            command: Base dbt command (run, test, build, etc.)
        
        Returns:
            List representing the full command to be executed
        """
        cmd = ["dbt", command]
        
        # Add profiles directory if specified
        if self.config.profiles_dir:
            cmd.extend(["--profiles-dir", self.config.profiles_dir])
        
        # Add model selection
        if self.config.select:
            cmd.extend(["--select", self.config.select])
        
        # Add exclusion
        if self.config.exclude:
            cmd.extend(["--exclude", self.config.exclude])
        
        # Add target
        if self.config.target:
            cmd.extend(["--target", self.config.target])
        
        # Add threads
        if self.config.threads and self.config.threads > 1:
            cmd.extend(["--threads", str(self.config.threads)])
        
        # Add variables (convert dict to dbt var format)
        if self.config.vars:
            vars_json = json.dumps(self.config.vars)
            cmd.extend(["--vars", vars_json])
        
        # Add debug flag
        if self.config.debug:
            cmd.append("--debug")
        
        # Add JSON output for structured results
        cmd.append("--write-json")
        
        return cmd

    def _parse_dbt_output(self, stdout: str) -> Dict[str, Any]:
        """
        Parse dbt JSON output from stdout.
        
        Args:
            stdout: Raw stdout from dbt command
        
        Returns:
            Dictionary with parsed dbt metadata, or empty dict if no JSON output
        """
        try:
            # dbt outputs JSON on separate lines at the end
            lines = stdout.strip().split('\n')
            for line in reversed(lines):
                try:
                    return json.loads(line)
                except json.JSONDecodeError:
                    continue
            return {}
        except Exception as e:
            logger.debug("Could not parse dbt JSON output: %s", e)
            return {}

