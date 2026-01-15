import sys
import time
from pathlib import Path
import argparse

# project imports
sys.path.append(str(Path(__file__).parent.parent))
from config.settings import settings

import structlog
import mlflow
from mlflow.tracking import MlflowClient

# logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer()
    ]
)
logger = structlog.get_logger(__name__)


def find_latest_run_id(client: MlflowClient, experiment_name: str):
    exp = client.get_experiment_by_name(experiment_name)
    if exp is None:
        logger.error("experiment_not_found", experiment_name=experiment_name)
        return None
    runs = client.search_runs([exp.experiment_id], order_by=["attributes.start_time DESC"], max_results=1)
    if not runs:
        logger.error("no_runs_found", experiment_id=exp.experiment_id, experiment_name=experiment_name)
        return None
    return runs[0].info.run_id


def main():
    parser = argparse.ArgumentParser(description="Register model from run and transition model version stage")
    parser.add_argument("--run-id", type=str, default=None, help="MLflow run id to register model from")
    parser.add_argument("--experiment-name", type=str, default="NetworkAnomalyDetector_experiment", help="Experiment name to search for latest run when run-id omitted")
    parser.add_argument("--model-name", type=str, default=getattr(settings, "MLFLOW_MODEL_NAME", "NetworkAnomalyDetector"), help="Registered model name to create or update")
    parser.add_argument("--artifact-path", type=str, default="autoencoder", help="artifact path inside run to register (e.g. 'autoencoder')")
    parser.add_argument("--stage", type=str, default="Production", help="Stage to transition the new model version to (Staging|Production|None)")
    parser.add_argument("--wait", type=int, default=10, help="Seconds to wait for registration to appear (poll interval)")
    args = parser.parse_args()

    mlflow.set_tracking_uri(getattr(settings, "MLFLOW_TRACKING_URI", "http://localhost:5000"))
    client = MlflowClient(tracking_uri=mlflow.get_tracking_uri())

    run_id = args.run_id
    if not run_id:
        logger.info("lookup_latest_run", experiment_name=args.experiment_name)
        run_id = find_latest_run_id(client, args.experiment_name)
        if not run_id:
            logger.error("no_run_to_register")
            return 1

    model_uri = f"runs:/{run_id}/{args.artifact_path}"
    logger.info("registering_model", model_uri=model_uri, model_name=args.model_name)

    try:
        # use high-level helper which returns ModelVersion object
        mv = mlflow.register_model(model_uri, args.model_name)
    except Exception as e:
        logger.error("register_model_failed", error=str(e), model_uri=model_uri, model_name=args.model_name)
        return 2

    # wait until model version is READY
    timeout = args.wait
    deadline = time.time() + timeout
    while time.time() < deadline:
        info = client.get_model_version(args.model_name, mv.version)
        if info.status.lower() in ("ready", "stage_transitioned", "available"):
            logger.info("model_version_ready", name=args.model_name, version=mv.version, status=info.status)
            break
        logger.info("waiting_for_model_version", name=args.model_name, version=mv.version, status=info.status)
        time.sleep(1)

    # transition stage (archive existing versions)
    try:
        client.transition_model_version_stage(
            name=args.model_name,
            version=mv.version,
            stage=args.stage,
            archive_existing_versions=True
        )
        logger.info("model_version_transitioned", name=args.model_name, version=mv.version, stage=args.stage)
    except Exception as e:
        logger.error("transition_failed", error=str(e), name=args.model_name, version=mv.version, target_stage=args.stage)
        return 3

    logger.info("done", model_name=args.model_name, version=mv.version, stage=args.stage)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())