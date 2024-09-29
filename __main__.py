import argparse
import logging
import os
import uuid
import asyncio
import shutil
from job_manager.manager import JobManager
from job_manager.artifact_manager import ArtifactManager
from job_manager.utils import CustomFormatter

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Argument parser
parser = argparse.ArgumentParser(description="Executes media processing pipeline")
parser.add_argument("job", type=str, help="job file path")
parser.add_argument("video", type=str, help="video file path")
parser.add_argument("--storage_dir", type=str, help="artifact storage directory path", default="temp/")
parser.add_argument("--modules_dir", type=str, help="module search directory path", default="modules/")

async def run():
    args = parser.parse_args()

    default_handler = logging.StreamHandler()
    default_handler.setLevel(logging.DEBUG)
    default_handler.setFormatter(CustomFormatter())

    logger.addHandler(default_handler)

    logging.info("Starting job manager...")

    # Get full paths fom arguments
    job_file_path = os.path.abspath(args.job)
    video_file_path = os.path.abspath(args.video)
    storage_dir_path = os.path.abspath(args.storage_dir)
    modules_dir_path = os.path.abspath(args.modules_dir)

    if not os.path.exists(job_file_path):
        logging.error(f"Job file not found: {job_file_path}")
        exit(1)

    if not os.path.exists(video_file_path):
        logging.error(f"Video file not found: {video_file_path}")
        exit(1)

    if not os.path.exists(storage_dir_path):
        logging.error(f"Storage directory not found: {storage_dir_path}")
        exit(1)
    
    if not os.path.exists(modules_dir_path):
        logging.error(f"Modules directory not found: {modules_dir_path}")
        exit(1)

    logging.info(f"Job file: {job_file_path}")
    logging.info(f"Video file: {video_file_path}")
    logging.info(f"Storage directory: {storage_dir_path}")
    logging.info(f"Modules directory: {modules_dir_path}")

    try:
        # Initialize job manager
        artifacts = ArtifactManager(storage_dir_path)
        manager = JobManager(modules_dir_path, artifacts)

        # Create job
        job_id = uuid.uuid4().hex
        job = manager.load_job(job_file_path) # Job ID needs to be sanitized before
                                              # using it for manager operations

        # Set input artifact
        input_artifact_path = artifacts.get_artifact_path(job.input_artifact, job_id)
        shutil.copy(video_file_path, input_artifact_path)
        artifacts.set_job_input_artifact(job_id, job.input_artifact)

        # Run job
        await manager.run_job(job, job_id)
    except Exception as e:
        logging.error(f"Failed to run job: {e}")
        logging.exception(e)

if __name__ == "__main__":
    asyncio.run(run())