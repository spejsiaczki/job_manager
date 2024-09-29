from pathlib import Path
import os
import shutil
from dataclasses import dataclass

@dataclass
class Artifact:
    file_type: str = None
    param_name: str = None
    name: str = None
    isPublic: bool = False

# Manages file storage for jobs
class ArtifactManager:
    ARTIFACT_STORAGE_FOLDER = 'temp'

    # Files are placed either in public or temp folder, depending on the public flag
    ARTIFACT_PUBLIC_FOLDER = 'public'
    ARTIFACT_TEMP_FOLDER = 'temp'
    jobs: list[str]
    job_input_artifact: dict[str, Artifact]

    def __init__(self, storage_path: Path):
        self.jobs = []
        self.job_input_artifact = {}
        self.storage_folder = os.path.join(storage_path, self.ARTIFACT_STORAGE_FOLDER)
        
        # Create artifact storage folder
        if not os.path.exists(self.storage_folder):
            os.makedirs(self.storage_folder)

        # Get existing job folders
        dirs = os.listdir(self.storage_folder)
        self.jobs = [d for d in dirs if os.path.isdir(os.path.join(self.storage_folder, d))]

        # Cleanup unfinished/broken jobs
        for job_id in self.jobs:
            self.cleanup_job(job_id)

    def set_job_input_artifact(self, job_id: str, artifact: Artifact):
        self.job_input_artifact[job_id] = artifact

    def does_job_exist(self, job_id: str):
        return job_id in self.jobs

    def get_artifact_path(self, artifact: Artifact, job_id: str):
        if artifact.isPublic:
            subfolder = self.ARTIFACT_PUBLIC_FOLDER
        else:
            subfolder = self.ARTIFACT_TEMP_FOLDER

        base_public_folder = os.path.join(self.storage_folder, job_id, subfolder)
        if not os.path.exists(base_public_folder):
            os.makedirs(base_public_folder)

        return os.path.join(base_public_folder, f'{artifact.name}')
    
    def cleanup_job(self, job_id: str):
        job_folder = os.path.join(self.storage_folder, job_id, self.ARTIFACT_TEMP_FOLDER)
        if os.path.exists(job_folder):
            shutil.rmtree(job_folder)
