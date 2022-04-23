import os
from typing import List, Mapping


def _dict_filter_none_values(d: Mapping[str, str]) -> Mapping[str, str]:
    return {k: v for k, v in d.items() if v is not None}


def get_github_env_variables() -> Mapping[str, str]:
    """
    Returns a dict with key-values representing details/environment variables for a task
    running as a Github action. All Github environment variables are prefixed with
    "pipeline.github.".

    Eg., the key "pipeline.github.actor" contains the Github username who triggered
    the task run.

    If no github-environment variables are set, this function returns an empty
    dictionary.

    For documentation and explanation of variables, see:
    https://docs.github.com/en/actions/learn-github-actions/environment-variables
    """
    gh_env_vars: List[str] = [
        #
        # ===== repository level metadata =====
        #
        "GITHUB_REPOSITORY",
        # eg. "pynb-dag-runner/mnist-digits-demo-pipeline"
        #
        #
        # ===== gha job level metadata =====
        #
        "GITHUB_WORKFLOW",
        # human readable description of gha job
        #
        "GITHUB_ACTION_REF",
        # (?) not documented in above web page but seen with "printenv". empty?
        #
        "RUNNER_NAME",
        # where was task run. eg "Hosted Agent"
        #
        "GITHUB_RUN_ID",
        # eg "nnnnnnnnnnnn". This can be used to link back to job run page on Github
        #
        "GITHUB_ACTOR",
        # user name triggering task
        #
        "GITHUB_JOB",
        # name of gha job
        #
        #
        # ===== Metadata for runs triggered by a pull request =====
        #
        "GITHUB_BASE_REF",
        # target branch for pull request, eg., "development"
        #
        "GITHUB_HEAD_REF",
        # eg. feature branch name for pull request
        #
        #
        # ===== git metadata =====
        #
        "GITHUB_SHA",
        # sha of commit being run
        #
        "GITHUB_REF",
        # see above link for formats, eg., "refs/pull/nn/merge"
        #
        "GITHUB_REF_TYPE",
        # possible values "branch" or "tag"
        #
        "GITHUB_REF_NAME",
        # eg "40/merge"
        #
        "GITHUB_EVENT_NAME",
        # eg "pull_request" or "workflow_dispatch"
        #
    ]

    return _dict_filter_none_values(
        {
            "pipeline.github." + var.lower().replace("github_", ""): os.getenv(var)
            for var in gh_env_vars
        }
    )
