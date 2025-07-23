# Project structure

The project structure follows a classical Python project structure:
- `.github`: everything related to GitHub Actions
  - `worflows`: contains the CI/CD workflow files used by GitHub Actions
  - `scripts`: contains Python scripts that deploy assets (code, DLT pipeline, etc.) to the environments. Those scripts are used by the GitHub Actions workflows.
  - `CODEOWNERS`: file specifying which users and groups are responsible for which parts of the code. This will be taken into account when opening pull requests as users/groups will be automatically added as reviewers
- `ci_cd_helpers`: helper code used by the CI/CD Python scripts. It's essentially a wrapper around the Databricks REST API.
- `conf`: contains the template configuration files for the DLT pipeline, the different Databricks workflows, and the cluster for each environment
- `dlt_pipelines`: contains the different DLT pipelines, including the actual reference pipeline
  - Notebook `install_requirements` installs all required libraries with their pinned versions specified in file `requirements.txt`
  - Notebook `reference_pipeline_main` contains the actual DLT reference pipeline code
- `docs`: folder containing all documentation around the project, e.g. development standards, CI/CD
- `integration_tests`: folder containing data and scripts to be used when running integration tests
- `reference_pipeline_library`: custom Python library containing business functions. Those are the functions we want to unit test
- `tests`: folder containing all unit tests. Unit tests are created and run with pytest
- `.gitignore`: file specifying everything that should not be tracked by git
- `README.md`: classical README file
- `requirements.txt`: file specifying all Python dependencies with their pinned versions
- `sonar-project.properties`: file containing the configuration for our SonarQube project
