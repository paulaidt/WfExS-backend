#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2020-2022 Barcelona Supercomputing Center (BSC), Spain
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

import copy
import datetime
import os
import re
import json
import time
import sys
import subprocess
import logging
import shutil

import stat
import tempfile
import venv

import functools
import itertools
import platform
import tempfile
import yaml

from typing import (
    cast,
    TYPE_CHECKING,
)

from .common import (
    ContainerType,
    ContentKind,
    EngineMode,
    LocalWorkflow,
    MaterializedContent,
    MaterializedInput,
    MaterializedWorkflowEngine,
    StagedExecution,
    WorkflowType,

)


if TYPE_CHECKING:
    from typing import (
        Any,
        IO,
        Mapping,
        MutableMapping,
        MutableSequence,
        Optional,
        Pattern,
        Sequence,
        Set,
        Tuple,
        Union,
    )

    from typing_extensions import (
        TypeAlias,
        Final,
    )
    from .common import (
        AbsPath,
        AnyPath,
        ContainerTaggedName,
        EngineLocalConfig,
        EngineMode,
        EnginePath,
        EngineVersion,
        ExitVal,
        ExpectedOutput,
        Fingerprint,
        MaterializedOutput,
        MaterializedWorkflowEngine, 
        RelPath,
        SymbolicParamName,
        URIType,
        WorkflowEngineVersionStr,
    )


from .engine import WorkflowEngine, WorkflowEngineException
from .engine import (
    WORKDIR_STATS_RELDIR,
    WORKDIR_STDOUT_FILE,
    WORKDIR_STDERR_FILE,
    STATS_DAG_DOT_FILE,
)   

import snakemake.workflow as smk_wf
from .utils.smk import(
    get_min_version
    )  

class SnakemakeWorkflowEngine(WorkflowEngine):
    SNAKEMAKE_REPO = ""
    SNAKEMAKE_PACKAGE = "snakemake"
    DEFAULT_SNAKEMAKE_VERSION = cast("EngineVersion", "7.20.0")
    DEFAULT_SNAKEMAKE_DOCKER_IMAGE = "snakemake/snakemake"

    DEFAULT_SNAKEMAKE_ENTRYPOINT = "Snakefile"
    SNAKEMAKE_CONFIG_FILENAME = "config.yaml"

    SNAKEMAKE_IO = cast("URIType", "https://snakemake.github.io/")

    DEFAULT_MAX_RETRIES = 5
    DEFAULT_MAX_CPUS = 4

    ENGINE_NAME = "snakemake"

    SUPPORTED_CONTAINER_TYPES = {
        ContainerType.NoContainer,
        ContainerType.Singularity,
        ContainerType.Docker,
        #  ContainerType.Podman,  # to be supported in the future by Snakemake
    }

    SUPPORTED_SECURE_EXEC_CONTAINER_TYPES = {
        ContainerType.NoContainer,
        ContainerType.Singularity,
        #  ContainerType.Podman,
    }

    def __init__(
        self,
        cacheDir: "Optional[AnyPath]" = None,
        workflow_config: "Optional[Mapping[str, Any]]" = None,
        local_config: "Optional[EngineLocalConfig]" = None,
        engineTweaksDir: "Optional[AnyPath]" = None,
        cacheWorkflowDir: "Optional[AnyPath]" = None,
        cacheWorkflowInputsDir: "Optional[AnyPath]" = None,
        workDir: "Optional[AnyPath]" = None,
        outputsDir: "Optional[AnyPath]" = None,
        outputMetaDir: "Optional[AnyPath]" = None,
        intermediateDir: "Optional[AnyPath]" = None,
        tempDir: "Optional[AnyPath]" = None,
        stagedContainersDir: "Optional[AnyPath]" = None,
        secure_exec: "bool" = False,
        allowOther: "bool" = False,
        config_directory: "Optional[AnyPath]" = None,
    ):
        super().__init__(
            cacheDir=cacheDir,
            workflow_config=workflow_config,
            local_config=local_config,
            engineTweaksDir=engineTweaksDir,
            cacheWorkflowDir=cacheWorkflowDir,
            cacheWorkflowInputsDir=cacheWorkflowInputsDir,
            workDir=workDir,
            outputsDir=outputsDir,
            intermediateDir=intermediateDir,
            tempDir=tempDir,
            stagedContainersDir=stagedContainersDir,
            outputMetaDir=outputMetaDir,
            secure_exec=secure_exec,
            allowOther=allowOther,
            config_directory=config_directory,
        )

        toolsSect = local_config.get("tools", {}) if local_config else {}
        engineConf = copy.deepcopy(toolsSect.get(self.ENGINE_NAME, {}))
        workflowEngineConf = (
            workflow_config.get(self.ENGINE_NAME, {}) if workflow_config else {}
        )
        engineConf.update(workflowEngineConf)

        self.smk_image = engineConf.get(
            "dockerImage", self.DEFAULT_SNAKEMAKE_DOCKER_IMAGE
        )
        smk_version = engineConf.get("version")
        if smk_version is None:
            default_snakemake_version = self.DEFAULT_SNAKEMAKE_VERSION
            smk_version = engineConf.get("version", default_snakemake_version)
        self.smk_version = smk_version
        self.max_retries = engineConf.get("maxRetries", self.DEFAULT_MAX_RETRIES)
        self.max_cpus = engineConf.get("maxProcesses", self.DEFAULT_MAX_CPUS)

        # Setting up packed directory ???

    @classmethod
    def MyWorkflowType(cls) -> "WorkflowType":
        # As of https://about.workflowhub.eu/Workflow-RO-Crate/ ,
        # the rocrate_programming_language should be next
        return WorkflowType(
            engineName=cls.ENGINE_NAME,
            shortname="snakemake",
            name="Snakemake",
            clazz=cls,
            uriMatch=[cls.SNAKEMAKE_IO],
            uriTemplate=cls.SNAKEMAKE_IO,
            url=cls.SNAKEMAKE_IO,
            trs_descriptor="SMK",
            rocrate_programming_language="https://w3id.org/workflowhub/workflow-ro-crate#snakemake",
        )
    
    @classmethod
    def SupportedContainerTypes(cls) -> "Set[ContainerType]":
        return cls.SUPPORTED_CONTAINER_TYPES

    @classmethod
    def SupportedSecureExecContainerTypes(cls) -> "Set[ContainerType]":
        return cls.SUPPORTED_SECURE_EXEC_CONTAINER_TYPES
    
    @property
    def engine_url(self) -> "URIType":
        return self.SNAKEMAKE_IO
    
    def identifyWorkflow(
        self, localWf: "LocalWorkflow", engineVer: "Optional[EngineVersion]" = None
    ) -> "Union[Tuple[EngineVersion, LocalWorkflow], Tuple[None, None]]":
        """
        This method should return the effective engine version needed
        to run it when this workflow engine recognizes the workflow type
        """
        # dir: path to the directory where the checkout was applied (wf root dir?)
        # relPath: Inside the checkout, the relative path to the workflow definition (snakefile?)
        # default snakefile path: dir/workflow/Snakefile
        # default config path: dir/config/config.yaml

        smkPath = localWf.dir
        if localWf.relPath is not None:
            smkPath = cast("AbsPath", os.path.join(smkPath, localWf.relPath))

        smkDir: "AbsPath"
        # If it is a directory, we have to assume there should be a Snakefile and config.yaml
        firstPath = None
        if os.path.isdir(smkPath):
            smkDir = smkPath
        elif os.path.isfile(smkPath):
            # Does it exist?
            smkDir = cast("AbsPath", os.path.dirname(smkPath))
            # We don't know yet which is
            firstPath = smkPath
        else:
            # Giving up
            raise WorkflowEngineException(
                f"Could not find {smkPath} in Snakemake workflow directory"
            )

        # Trying with the defaults (workflow/ and root/)
        if firstPath is None:
            firstPath = cast(
                "AbsPath", os.path.join(smkDir, "workflow", self.DEFAULT_SNAKEMAKE_ENTRYPOINT)
            )
            # Does it exist?
            if not os.path.isfile(firstPath):
                firstPath = cast(
                    "AbsPath", os.path.join(smkDir, self.DEFAULT_SNAKEMAKE_ENTRYPOINT)
                )
                if not os.path.isfile(firstPath):
                    # Giving up
                    raise WorkflowEngineException(
                        f"Could not find {self.DEFAULT_SNAKEMAKE_ENTRYPOINT} in Snakemake workflow directory {smkDir}."
                    )

        # Guessing what we got here is a Snakefile 
        candidateSmk: "Optional[RelPath]" = None

        if firstPath is not None:
            candidateSmk = firstPath

        # Parsing candidate Snakefile
        smkFile = smk_wf.infer_source_file(candidateSmk)
        wf = smk_wf.Workflow(smkFile)
        try: 
            smk_compilation, linemap, rulecount = smk_wf.parse(smkFile, wf)
            # Execute
            # compile(smk_compilation, smkFile.get_path_or_uri(), "exec")
            # exec: requires definition of configuration file or files required by the workflow

        except Exception as e:
                errstr = f"Failed to parse initial file {os.path.relpath(candidateSmk, smkDir)} with snakemake parser."
                self.logger.exception(errstr)
                raise WorkflowEngineException(errstr) from e

        # Select minimal engine version
        minimalEngineVer = None
        engineVer = None

        if smk_compilation is not None and rulecount>0:
            minimalEngineVer = get_min_version(smk_compilation)
            if minimalEngineVer is None:
                self.logger.debug(f"Failed to find min_version defined in file {os.path.relpath(candidateSmk, smkDir)}.")
        
        elif smk_compilation is not None and rulecount==0:
            self.logger.debug(f"Invalid Snakefile {os.path.relpath(candidateSmk, smkDir)}.")

        # Setting a default engineVer
        if (minimalEngineVer is not None) and (
            (engineVer is None) or engineVer < minimalEngineVer
        ):
            engineVer = minimalEngineVer

        if engineVer is None:
            engineVer = self.smk_version
                         
        # Subworkflow / module include detection

        # Wrappers include detection

        candidateSmk = cast("RelPath", os.path.relpath(candidateSmk, smkDir))

        return engineVer, LocalWorkflow(
            dir=smkDir,
            relPath= candidateSmk,
            effectiveCheckout=localWf.effectiveCheckout,
            langVersion=engineVer,
            relPathFiles=[cast("RelPath", localWf.relPath)],
        )
        
        
    def materializeEngineVersion(
        self, engineVersion: "EngineVersion"
    ) -> "Tuple[EngineVersion, EnginePath, Fingerprint]":
        """
        Method to ensure the required engine version is materialized
        It should raise an exception when the exact version is unavailable,
        and no replacement could be fetched
        """

        # Version directory is needed for installation
        snakemake_install_dir = cast(
            "EnginePath", os.path.join(self.weCacheDir, engineVersion)
        )

        # Virtual environment to separate Snakemake code
        # from workflow execution backend
        do_install = True
        if not os.path.isdir(snakemake_install_dir):
            venv.create(snakemake_install_dir, with_pip=True)
        else:
            # Check the installation is up and running
            installed_engineVersion_str = self.__get_engine_version_str_local(
                MaterializedWorkflowEngine(
                    instance=self,
                    workflow=LocalWorkflow(
                        dir=cast("AbsPath", "/"),
                        relPath=None,
                        effectiveCheckout=None,
                    ),
                    version=cast("EngineVersion", ""),
                    fingerprint="",
                    engine_path=snakemake_install_dir,
                )
            )

            r_sp = installed_engineVersion_str.rfind(" ")
            if r_sp != -1:
                installed_engineVersion = installed_engineVersion_str[r_sp + 1 :]
            else:
                installed_engineVersion = installed_engineVersion_str

            do_install = engineVersion != installed_engineVersion
            if do_install:
                self.logger.debug(
                    f"Snakemake mismatch {engineVersion} vs {installed_engineVersion}"
                )

        if do_install:
            instEnv = dict(os.environ)

            with tempfile.NamedTemporaryFile() as snakemake_install_stdout:
                with tempfile.NamedTemporaryFile() as snakemake_install_stderr:
                    retVal = subprocess.Popen(
                        [
                            f"{snakemake_install_dir}/bin/pip",
                            "install",
                            "--upgrade",
                            "pip",
                            "wheel",
                        ],
                        stdout=snakemake_install_stdout,
                        stderr=snakemake_install_stderr,
                        cwd=snakemake_install_dir,
                        env=instEnv,
                    ).wait()

                    # Error handling
                    if retVal != 0:
                        # Reading the output and error for the report
                        with open(snakemake_install_stdout.name, "r") as std_file:
                            snakemake_install_stdout_v = std_file.read()
                        with open(snakemake_install_stderr.name, "r") as std_file:
                            snakemake_install_stderr_v = std_file.read()

                        errstr = "Could not upgrade pip. Retval {}\n======\nSTDOUT\n======\n{}\n======\nSTDERR\n======\n{}".format(
                            retVal,
                            snakemake_install_stdout_v,
                            snakemake_install_stderr_v,
                        )
                        raise WorkflowEngineException(errstr)

                    retVal = subprocess.Popen(
                        [
                            f"{snakemake_install_dir}/bin/pip",
                            "install", self.SNAKEMAKE_PACKAGE + "==" + engineVersion,
                        ],
                        stdout=snakemake_install_stdout,
                        stderr=snakemake_install_stderr,
                        cwd=snakemake_install_dir,
                        env=instEnv,
                    ).wait()

                    # Error handling
                    if retVal != 0:
                        # Reading the output and error for the report
                        with open(snakemake_install_stdout.name, "r") as std_file:
                            snakemake_install_stdout_v = std_file.read()
                        with open(snakemake_install_stderr.name, "r") as std_file:
                            snakemake_install_stderr_v = std_file.read()

                        errstr = "Could not install Snakemake {} . Retval {}\n======\nSTDOUT\n======\n{}\n======\nSTDERR\n======\n{}".format(
                            engineVersion,
                            retVal,
                            snakemake_install_stdout_v,
                            snakemake_install_stderr_v,
                        )
                        raise WorkflowEngineException(errstr)
        return (
            engineVersion,
            snakemake_install_dir,
            cast("Fingerprint", engineVersion),
        )
            
        
    # def runSnakemakeCommand() ?
    # def runLocalSnakemakeCommand() ?
    def _get_engine_version_str(
        self, matWfEng: "MaterializedWorkflowEngine"
    ) -> "WorkflowEngineVersionStr":
        assert (
            matWfEng.instance == self
        ), "The workflow engine instance does not match!!!!"

        if self.engine_mode == EngineMode.Local:
            return self.__get_engine_version_str_local(matWfEng)

        raise WorkflowEngineException(
            "Unsupported engine mode {} for {} engine".format(
                self.engine_mode, self.ENGINE_NAME
            )
        )

    def __get_engine_version_str_local(
        self, matWfEng: "MaterializedWorkflowEngine"
    ) -> "WorkflowEngineVersionStr":
        # SnakemakeWorkflowEngine directory is needed
        snakemake_install_dir = matWfEng.engine_path
        
        # Execute snakemake --version
        with tempfile.NamedTemporaryFile() as snakemake_version_stderr:
            # Writing straight to the file
            with subprocess.Popen(
                [f"{snakemake_install_dir}/bin/snakemake", "--version"],
                stdout=subprocess.PIPE,
                stderr=snakemake_version_stderr,
                cwd=snakemake_install_dir,
            ) as vP:
                engine_ver: "str" = ""
                if vP.stdout is not None:
                    engine_ver = vP.stdout.read().decode("utf-8", errors="continue")
                    self.logger.debug(f"{snakemake_install_dir} version => {engine_ver}")

                retval = vP.wait()

            # Proper error handling
            if retval != 0:
                # Reading the output and error for the report
                with open(snakemake_version_stderr.name, "r") as c_stF:
                    snakemake_version_stderr_v = c_stF.read()

                errstr = "Could not get version running snakemake --version from {}. Retval {}\n======\nSTDOUT\n======\n{}\n======\nSTDERR\n======\n{}".format(
                    snakemake_install_dir, retval, engine_ver, snakemake_version_stderr_v
                )
                raise WorkflowEngineException(errstr)

            pref_ver = os.path.join(snakemake_install_dir, "bin") + "/"
            if engine_ver.startswith(pref_ver):
                engine_ver = engine_ver[len(pref_ver) :]

            return cast("WorkflowEngineVersionStr", engine_ver.strip())

    def materializeWorkflow(
        self, matWorkflowEngine: "MaterializedWorkflowEngine", offline: "bool" = False
    ) -> "Tuple[MaterializedWorkflowEngine, Sequence[ContainerTaggedName]]":
        """
        Method to ensure the workflow has been materialized. It returns the
        localWorkflow directory, as well as the list of containers
        """
        ### TBC ....

    
    def simpleContainerFileName(self, imageUrl: "URIType") -> "RelPath":
        """
        https://github.com/snakemake/snakemake/blob/main/snakemake/deployment/singularity.py  ??
        """
        ### TBC ....

    def launchWorkflow(
        self,
        matWfEng: "MaterializedWorkflowEngine",
        matInputs: "Sequence[MaterializedInput]",
        matEnvironment: "Sequence[MaterializedInput]",
        outputs: "Sequence[ExpectedOutput]",
    ) -> "StagedExecution":
        # TODO: implement usage of materialized environment variables
        if len(matInputs) == 0:  # Is list of materialized inputs empty?
            raise WorkflowEngineException("FATAL ERROR: Execution with no inputs")

        localWf = matWfEng.workflow

        ### TBC ....
