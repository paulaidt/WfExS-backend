#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: Apache-2.0
# Copyright 2020-2023 Barcelona Supercomputing Center (BSC), Spain
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

import os
import re
import sys

from typing import (
    cast,
    NamedTuple,
    TYPE_CHECKING,
)
if TYPE_CHECKING:
    from typing import (
        Any,
        Iterator,
        Mapping,
        MutableMapping,
        MutableSequence,
        Optional,
        Sequence,
        Tuple,
        TypeVar,
        Union,
    )

from ..engine import (
    WorkflowEngine, 
    WorkflowEngineException, 
)

import logging

def get_min_version(dtstr: "str") -> "Sequence[str]":
    """
    Return the min_version required from a Snakefile.
    If more than one version specified in the file, the 
    higher version is returned. 
    """
    pattern = r"min_version\(['\"](\d+\.\d+\.\d+)['\"]\)"
    match_vers = re.findall(pattern, dtstr)
    if len(match_vers)>=1:
        return max(match_vers)
    else:
        return None


class smkInclude(NamedTuple):
    path: "str"

def extract_smk_includes(candidate: "str") -> "Sequence[smkInclude]":
    """
    Input: path of candidateSmk file (Snakefile) where to read the includes from. 
    Return list of paths of included rules.
    Includes are relative to the directory of the Snakefile in which they occur. 
    """
    #JMF comments:
    # - recursive call for includes, 
    # - should return complete path? include snakefile path with relative paths of includes.
    # - return named tupple (see HintedWorkflowComponent)
    # - eval ensure not leaving main root dir
    include_calls = []
    with open(candidate, mode='r', encoding='utf-8') as file:
        for line in file:
            logging.error(f"line: {line}.")
            match = re.match(r'^\s*include\s*:\s*(.*)', line)
            if match:
                include_calls.append(match.group(1).strip())
    logging.error(f"include calls: {include_calls}.")
    includes_abs_paths = []
    for call in include_calls:
        if call.startswith("os.path"):
            call_path = os.path.normpath(call) 
        else:
            call_path = call.strip('"')
        # Get abs path for includes calls
        call_abs_path= cast(
                    "AbsPath", os.path.join(candidate, call_path)
                )               
        try: 
            with open (call_abs_path, mode='r', encoding='utf-8') as call_file_H:
                os.path.isfile(call_file_H) #check if file exist
                compile(call_file_H) # check if its python

        except Exception as e:
            errstr = f"Failed to find {call_abs_path} included file in {candidate}."
            self.logger.exception(errstr)
            raise WorkflowEngineException(errstr) from e

        includes_abs_paths.append(call_abs_path)

    if includes_abs_paths: 
        return [
            smkInclude(
                path=path,
            )
            # , extract_smk_includes(path)
            for path in includes_abs_paths
        ] 
    
    else: 
        return []

class smkWorkflow(NamedTuple):
    name: "Optional[str]"

def extract_smk_workflow(path_or_uri: "str") -> "smkWorkflow":
    """
    ...
    """

class smkConfig(NamedTuple):
    name: "Optional[str]"

def extract_smk_config(path_or_uri: "str") -> "smkConfig":
    """
    ...
    """