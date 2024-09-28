# job_manager

Scheduler and pipeline supervisor. Runs user requested jobs.

## Modules

Each segment of the pipeline is called a module. Module consists of one self-contained analyzer or data processor that accepts input of any kind (source video, audio, preprocessed data) and outputs result or an intermediate format.

Each module needs to be placed inside a folder and include an entrypoint - a Python script and a manifest.yaml file.

Example file structure:

```
example_module/
  manifest.yaml
  main.py
  helper.py
```

Modules are loaded dynamically by the job manager. Loader looks for all folders following above structure under the provided path.

```

### modules.yaml specification

```yaml
name: example_module    # Name of the module
description: Doing some stuff with the video # User-friendly description of the module
entrypoint: main.py     # Entry point of the module, python script placed in the same folder

# Accepted input files
inputs:
  - param_name: input 
    type: video

# Produced output files
outputs:
  - param_name: output
    type: audio
```

### Inputs and outputs

Each module can accept multiple input files and produce multiple output files. Each input and output file is called artifact and described by a dictionary with the following fields:

```yaml
- param_name: input # Name of the CLI parameter
  type: video       # Type of the parameter, sanity check only
```

Paths are pased to the module as command line arguments in the following format:
```
--{param_name} {path_generated_internally}
```

## Job

Job describes entire pipeline that needs to be run. It consists of a list of steps run sequentially. Each step consists of entries/programs ran in parallel.

### job.yaml specification

```yaml
name: example_job   # Name of the job
version: 1.0        # Version of the job
input_artifacts:    # Main input file for the job, follows module's specification
  - param_name: input
    type: video
steps:
  - name: step1   # Name of the step
    entries:      # List of entries to run in parallel
      - module: example_module  # Name of the module to run
        input_artifacts:        # Input artifacts for the module
          - name: video.mp4
            param_name: input
            type: video
        output_artifacts:       # Output artifacts for the module
          - name: audio.wav     # Name of the file
            param_name: output  # Name of the CLI parameter
            type: audio         # Type of the artifact, sanity check only
            public: true        # Whether the artifact should not be removed after the job is done
```
