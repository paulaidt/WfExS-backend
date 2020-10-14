#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2020-2021 Barcelona Supercomputing Center (BSC), Spain
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

import sys
import os
import argparse
import yaml
import tempfile
import atexit
import shutil

# We have preference for the C based loader and dumper, but the code
# should fallback to default implementations when C ones are not present
try:
	from yaml import CLoader as YAMLLoader, CDumper as YAMLDumper
except ImportError:
	from yaml import Loader as YAMLLoader, Dumper as YAMLDumper

from wes_backend.workflow import WF

if __name__ == "__main__":
	ap = argparse.ArgumentParser(description="WES backend")
	ap.add_argument('-C','--config',dest="configFilename",help="Local installation configuration file")
	ap.add_argument('--cache-dir',dest="cacheDir",help="Caching directory")
	ap.add_argument('-W','--workflow-config',dest="workflowConfigFilename",required=True,help="Configuration file, describing workflow, inputs and needed credentials")
	args = ap.parse_args()
	
	# First, try loading the configuration file
	if args.configFilename:
		with open(args.configFilename,"r",encoding="utf-8") as cf:
			local_config = yaml.load(cf,Loader=YAMLLoader)
	else:
		local_config = {}
	
	if args.cacheDir:
		local_config['cache-directory'] = args.cacheDir
	
	# In any case, assuring the cache directory does exist
	cacheDir = local_config.get('cacheDir')
	if cacheDir:
		os.makedirs(cacheDir, exist_ok=True)	
	else:
		cacheDir = tempfile.mkdtemp(prefix='wes',suffix='backend')
		local_config['cacheDir'] = cacheDir
		# Assuring this temporal directory is removed at the end
		atexit.register(shutil.rmtree,cacheDir)
	
	with open(args.workflowConfigFilename,"r",encoding="utf-8") as wcf:
		workflow_config = yaml.load(wcf,Loader=YAMLLoader)
	
	wfInstance= WF.fromDescription(workflow_config,local_config)
	
	wfInstance.fetchWorkflow()
	wfInstance.setupEngine()
	wfInstance.fetchInputs()
	wfInstance.executeWorkflow()