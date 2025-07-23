# Unit testing for the reference pipeline

## Tools

We use [pytest](https://docs.pytest.org/en/7.2.x/) as our unit testing framework, [pytest-cov](https://pypi.org/project/pytest-cov/) as our code coverage tool, and [chispa](https://github.com/MrPowers/chispa) to test Spark DataFrames.

## Code structure

To properly unit test our code, we separated the code into 3 distinct parts:
- the *dlt_pipelines* folder that contains the actual DLT pipeline code
- the *reference_pipeline_library* folder that contains the business functions
- the *tests* folder that contains the unit tests.

The DLT pipeline notebook will then import modules and functions from the `reference_pipeline_library` library. See section [Installing libraries when working with DLT](./installing_libraries.md) to learn more about the installation and import process between DLT, our library containing the business functions, and other Python libraries.

The tests folder follows the same structure as the library’s structure for better maintainability. For example, if there is a file named `business_functions.py` in our library containing functions to be tested, then we create a "matching" file named `test_business_functions.py` in the `tests` folder. If our library contains sub-folders (aka Python modules), then we create matching sub-folders (with the same name this time, no need to prefix it by "test_") at the same level under the tests folder, etc.

## Running unit tests

To run all unit tests, compute code coverage, and save the coverage results and the unit tests results in xml files (to reuse them later with SonarQube for instance), run the following command in a terminal inside the project repository root folder:
```text
pytest \
--cov=./reference_pipeline_library \
--cov-branch \
--cov-report term-missing \
--cov-report xml:./logs/coverage.xml \
--junit-xml=./logs/pytest.xml \
-o junit_suite_example=pipeline
```

All `cov` parameters are pytest-cov parameters used for code coverage. The other parameters are pytest parameters. You can get more information on all those parameters by running pytest --help.
Simply note that by running this command, we tell pytest, among other things, to:
- Only consider folder reference_pipeline_library when computing code coverage
- Save the code coverage report in file coverage.xml under (newly created) folder logs
- Save the unit tests report in file pytest.xml under (newly created) folder logs

## Requirements

NB: to be able to use pytest, pytest-cov, and chispa, we of course need to install those libraries before running the pytest command. All those dependencies are stored in file requirements.txt. To install those dependencies, run the following command in a terminal inside the project repository root folder:
```text
pip install –r requirements.txt
```

NB: this is not the cleanest as those dependencies are used for testing only and will also be installed in production. There are of course cleaner ways of preventing this, but for now this is not a blocker or an issue we should be thinking about.

## CI/CD

Unit tests are run automatically every time a commit is pushed to any branch. The command running unit tests is stored in reusable workflow [base-tests.yaml](../../.github/workflows/base-tests.yaml). This manual workflow is called by 3 workflows:
- [on-push-basic-branches.yaml](../../.github/workflows/on-push-basic-branches.yaml): this workflow is triggered every time a commit is pushed to any branches except `main` and `release-v*` branches
- [deploy-to-prod.yaml](../../.github/workflows/deploy-to-prod.yaml): this workflow is triggered every time a commit is pushed to the `main` branch
- [deploy-to-pre-prod.yaml](../../.github/workflows/deploy-to-pre-prod.yaml): this workflow is triggered every time a commit is pushed to a `release-v*` branch

## Disable unit tests in the CI/CD

The pytest command running unit tests creates a report xml file for those unit tests and a code coverage xml file, both used by SonarQube. If you want to disable unit tests in the CI/CD, you will therefore need to update SonarQube's configuration too.

To disable unit tests in the CI/CD, comment out the following two steps from [base-tests.yaml](../../.github/workflows/base-tests.yaml):
```yaml
- name: Run unit tests and compute code coverage with pytest 
  run: |
    pytest \
    --cov=./reference_pipeline_library \
    --cov-branch \
    --cov-report term-missing \
    --cov-report xml:./logs/coverage.xml \
    --junit-xml=./logs/pytest.xml \
    -o junit_suite_name=pipeline

# Necessary so that SonarQube can pick up the coverage file created by pytest
# see https://dev.to/remast/go-for-sonarcloud-with-github-actions-3pmn
# see also docs/cicd/sonarqube.md
- name: update coverage file
  run: |
    sed -i "s+$GITHUB_WORKSPACE+/github/workspace+g" $GITHUB_WORKSPACE/logs/coverage.xml
```

The first step is the actual pytest command running the unit tests. This command also computes the code coverage report file. The second command transforms that code coverage report file, hence the need to comment it out too.

Finally, update SonarQube's configuration file [sonar-project.properties](../../sonar-project.properties) and remove the two lines specifying the locations of the two generated report files:
```
sonar.python.coverage.reportPaths=/github/workspace/logs/coverage.xml
sonar.python.xunit.reportPath=/github/workspace/logs/pytest.xml
```
