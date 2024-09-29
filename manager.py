import yaml
import os
import logging
import asyncio
from pathlib import Path
from dataclasses import dataclass, field
from job_manager.artifact_manager import ArtifactManager, Artifact

# Stores module definition and allows for abstracted execution in the pipeline
class Module:
    name: str
    entrypoint: str
    description: str
    inputs: list[Artifact]
    outputs: list[Artifact]

    def __init__(self, manifest_file) -> None:
        self.inputs = []
        self.outputs = []

        with open(manifest_file, 'r') as f:
            self.manifest = yaml.load(f, Loader=yaml.SafeLoader)

        self._load_manifest()
        
        # Get full path for entrypoint
        self.entrypoint = os.path.join(os.path.dirname(manifest_file), self.entrypoint)

    def _load_manifest(self):
        if not hasattr(self, 'manifest'):
            raise Exception('Manifest not loaded')
        
        if 'name' not in self.manifest:
            raise Exception('Manifest missing name field')
        self.name = self.manifest['name']
        
        if 'entrypoint' not in self.manifest:
            raise Exception('Manifest missing entrypoint field')
        self.entrypoint = self.manifest['entrypoint']
        
        if 'description' not in self.manifest:
            raise Exception('Manifest missing description field')
        self.description = self.manifest['description']
        
        if 'inputs' not in self.manifest:
            raise Exception('Manifest missing inputs field')
        
        if self.manifest['inputs'] is not None:
            for entry in self.manifest['inputs']:
                if 'param_name' not in entry:
                    raise Exception('Manifest missing input name field')
                if 'type' not in entry:
                    raise Exception('Manifest missing input type field')
                
                self.inputs.append(Artifact(file_type=entry['type'], param_name=entry['param_name']))

        if 'outputs' not in self.manifest:
            raise Exception('Manifest missing outputs field')
        
        if self.manifest['outputs'] is not None:
            for entry in self.manifest['outputs']:
                if 'param_name' not in entry:
                    raise Exception('Manifest missing output name field')
                if 'type' not in entry:
                    raise Exception('Manifest missing output type field')

                self.outputs.append(Artifact(file_type=entry['type'], param_name=entry['param_name']))

    # Runs the module with provided input and output artifacts
    async def run(self, input: list[Artifact], output: list[Artifact], job_id: str, artifact_manager: ArtifactManager):
        # Check if all input artifacts were provided
        for i in self.inputs:
            if not any(i.param_name == a.param_name for a in input):
                raise Exception(f'Missing input artifact: {i.param_name}')
            
        for i in input:
            if not any(i.param_name == a.param_name for a in self.inputs):
                raise Exception(f'Unexpected input artifact: {i.param_name}/{i.file_type}')
        
        # Check if all output artifacts were provided
        for o in self.outputs:
            if not any(o.param_name == a.param_name for a in output):
                raise Exception(f'Missing output artifact: {o.param_name}')
        
        for o in output:
            if not any(o.param_name == a.param_name for a in self.outputs):
                raise Exception(f'Unexpected output artifact: {o.param_name}/{o.file_type}')

        # Create subprocess args based on provided artifacts
        args = []
        for i in input:
            artifact_path = artifact_manager.get_artifact_path(i, job_id)
            if not os.path.exists(artifact_path):
                raise Exception(f'Input artifact not found: {artifact_path}')

            args.append(f'--{i.param_name}')
            args.append(artifact_path)

        for o in output:
            args.append(f'--{o.param_name}')
            args.append(artifact_manager.get_artifact_path(o, job_id))

        async def _read_stream(stream, cb):  
            while True:
                line = await stream.readline()
                if line:
                    cb(line)
                else:
                    break
        
        proc = await asyncio.create_subprocess_exec(
            "python",
            self.entrypoint,
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        
        logger = logging.getLogger(f'job_{job_id[:8]}')

        await asyncio.gather(
            _read_stream(proc.stdout, lambda line: logger.info(f'{self.name}: {line.decode().strip()}')),
            _read_stream(proc.stderr, lambda line: logger.error(f'{self.name}: {line.decode().strip()}'))
        )

        return await proc.wait()

@dataclass
class StepModule:
    module: str
    inputs: list[Artifact] = field(default_factory=list)
    outputs: list[Artifact] = field(default_factory=list)

@dataclass
class Step:
    name: str
    modules: list[StepModule] = field(default_factory=list)

class Job:
    name: str
    version: int
    steps: list[Step]
    input_artifact: Artifact

    def __init__(self, manifest_file) -> None:
        self.steps = []
        with open(manifest_file, 'r') as f:
            manifest = yaml.load(f, Loader=yaml.SafeLoader)
            self._load_manifest(manifest)

    def _load_manifest(self, manifest):
        if 'name' not in manifest:
            raise Exception('Job manifest missing name field')
        self.name = manifest['name']

        if 'version' not in manifest:
            raise Exception('Job manifest missing version field')
        self.version = manifest['version']

        if 'input_artifact' not in manifest:
            raise Exception('Job manifest missing inputs field')
        
        if 'name' not in manifest['input_artifact']:
            raise Exception('Job manifest missing input name field')
        
        if 'type' not in manifest['input_artifact']:
            raise Exception('Job manifest missing input type field')
        
        self.input_artifact = Artifact(file_type=manifest['input_artifact']['type'], name=manifest['input_artifact']['name'])

        if 'steps' not in manifest:
            raise Exception('Job manifest missing steps field')
        
        for step in manifest['steps']:
            if 'name' not in step:
                raise Exception('Step missing name field')
            
            entries = []
            
            for entry in step['entries']:
                if 'module' not in entry:
                    raise Exception('Step module missing module field')
                if 'inputs' not in entry:
                    raise Exception('Step module missing inputs field')
                if 'outputs' not in entry:
                    raise Exception('Step module missing outputs field')
                
                inputs = []
                if entry['inputs'] is not None:
                    for i in entry['inputs']:
                        if 'name' not in i:
                            raise Exception('Step module input missing name field')
                        if 'type' not in i:
                            raise Exception('Step module input missing type field')
                        if 'param_name' not in i:
                            raise Exception('Step module input missing param_name field')
                        
                        public = False
                        if 'public' in i:
                            public = i['public']
                        
                        inputs.append(Artifact(file_type=i['type'], param_name=i['param_name'], name=i['name'], isPublic=public))
                
                outputs = []
                if entry['outputs'] is not None:
                    for o in entry['outputs']:
                        if 'name' not in o:
                            raise Exception('Step module output missing name field')
                        if 'type' not in o:
                            raise Exception('Step module output missing type field')
                        if 'param_name' not in o:
                            raise Exception('Step module output missing param_name field')
                        
                        public = False
                        if 'public' in o:
                            public = o['public']
                    
                        outputs.append(Artifact(file_type=o['type'], param_name=o['param_name'], name=o['name'], isPublic=public))
                
                entries.append(StepModule(module=entry['module'], inputs=inputs, outputs=outputs))
            
            self.steps.append(Step(name=step['name'], modules=entries))

@dataclass
class JobManagerProgressUpdate:
    job_id: str
    current_step: int
    total_steps: int
    step_name: str
class JobManager:
    modules: list[Module]
    artifact_manager: ArtifactManager

    def __init__(self, modules_dir: Path, artifact_manager: ArtifactManager) -> None:
        self.modules = []
        self.modules_dir = modules_dir
        self.artifact_manager = artifact_manager

        # Scan subfolders under modules dir for module manifest files
        files = os.listdir(modules_dir)
        for f in files:
            if os.path.isdir(os.path.join(modules_dir, f)):
                manifest_file = os.path.join(modules_dir, f, 'manifest.yaml')

                # Skip folders without manifest file
                if not os.path.exists(manifest_file):
                    continue

                # Load available modules
                try:
                    module = Module(manifest_file)
                    self.modules.append(module)
                    logging.info(f'Loaded module {f}')
                except Exception as e:
                    logging.error(f'Failed to load module {f}: {e}')


    async def run_job(self, job: Job, job_id: str, progress_cb=None):        
        logging.info(f'Running job {job.name} v{job.version}, id: {job_id}')

        logger = logging.getLogger(f'job_{job_id[:8]}')

        for step in job.steps:
            logger.info(f'Running step {step.name}')

            # Send progress update
            if progress_cb is not None:
                update = JobManagerProgressUpdate(
                    job_id,
                    job.steps.index(step) + 1,
                    len(job.steps),
                    step.name
                )
                try:
                    progress_cb(update)
                except Exception as e:
                    logger.error(f'Failed to send progress update: {e}')

            await asyncio.sleep(1)

            # Gather tasks to be run during current step
            tasks = []
            for entry in step.modules:
                module = next((m for m in self.modules if m.name == entry.module), None)
                if module is None:
                    logger.error(f'Module not found: {entry.module}')
                    continue
                
                logger.info(f'Running module {module.name}')
                
                coroutine = module.run(entry.inputs, entry.outputs, job_id, self.artifact_manager)
                tasks.append(asyncio.create_task(coroutine, name=module.name))
            
            # Run step tasks concurrently
            await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)

            # Check for failed tasks
            for task in tasks:
                if task.exception() is not None or task.result() != 0:
                    reason = task.exception() if task.exception() is not None else f'Exit code {task.result()}'
                    logger.error(f'Failed to run module {task.get_name()}: {reason}')

        # Cleanup temp files
        self.artifact_manager.cleanup_job(job_id)

        logging.info(f'Job {job.name} v{job.version} completed, id: {job_id}')

    def load_job(self, job_file: Path) -> Job:
        return Job(job_file)
