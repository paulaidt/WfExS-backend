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
