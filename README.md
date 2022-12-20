# Introduction
This project create a batch and streaming pipelien forhandling route information


# Getting Started
For a high level overview of the solution visit the page on confluence : <br>
For a general overview / background information how this pyspark project is structured see : https://dev.azure.com/raboweb/AIDA-DevOps/_git/databricks-edl-example-pyspark-project<br>

# Building

```bash
python -m venv ./venv
source ./venv/bin/activate
```

install build:
```bash
pip install build
```

build:
```bash
python -m build
```

the `.whl` and `.tar.gz` are located in `dist`:
```bash
dist
├── assesment_route_pipeline-*-py3-none-any.whl
└── assesment_route_pipeline-*.tar.gz

0 directories, 2 files
```

# Setting up your development environment


## OSX
- clone the git repo:
```bash
git clone https://github.com/Deefvandervliet/route_pipeline.git
```
- setup and activate a new python virtual env:
```bash
python3 -m venv ./venv
source ./venv/bin/activate
```
- install requirements:
```bash
pip install -e '.[dev]'
```

# Latest releases
link to devops release.<br>

# API references
We are not using api libraries

# Build and Test

# Contact information
Code owner: Team X