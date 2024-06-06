#!/usr/bin/env python
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: Apache-2.0
# Copyright 2020-2024 Barcelona Supercomputing Center (BSC), Spain
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

import json
import os
from typing import (
    cast,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from typing import (
        Any,
        Mapping,
        Optional,
        Sequence,
        Tuple,
        Union,
    )

    from typing_extensions import (
        Final,
    )

    from ..common import (
        AbsPath,
        AnyPath,
        ContainerTaggedName,
        Fingerprint,
        RelPath,
        URIType,
    )

    from . import (
        ContainerFileNamingMethod,
        ContainerLocalConfig,
        ContainerOperatingSystem,
        ProcessorArchitecture,
    )

    from .abstract_docker_container import (
        DockerManifestMetadata,
    )

from ..common import (
    ContainerType,
    DEFAULT_PODMAN_CMD,
    META_JSON_POSTFIX,
)
from . import (
    Container,
    ContainerEngineException,
    ContainerFactoryException,
    DOCKER_URI_PREFIX,
)
from .abstract_docker_container import (
    AbstractDockerContainerFactory,
    DOCKER_PROTO,
)
from ..utils.contents import (
    link_or_copy,
    real_unlink_if_exists,
)
from ..utils.digests import ComputeDigestFromFile


class PodmanContainerFactory(AbstractDockerContainerFactory):
    TRIMMABLE_MANIFEST_KEYS: "Final[Sequence[str]]" = [
        "Digest",
        "RepoDigests",
        "Size",
        "VirtualSize",
    ]

    @classmethod
    def trimmable_manifest_keys(cls) -> "Sequence[str]":
        return cls.TRIMMABLE_MANIFEST_KEYS

    def __init__(
        self,
        simpleFileNameMethod: "ContainerFileNamingMethod",
        containersCacheDir: "Optional[AnyPath]" = None,
        stagedContainersDir: "Optional[AnyPath]" = None,
        tools_config: "Optional[ContainerLocalConfig]" = None,
        engine_name: "str" = "unset",
        tempDir: "Optional[AnyPath]" = None,
    ):
        super().__init__(
            simpleFileNameMethod=simpleFileNameMethod,
            containersCacheDir=containersCacheDir,
            stagedContainersDir=stagedContainersDir,
            tools_config=tools_config,
            engine_name=engine_name,
            tempDir=tempDir,
        )
        self.runtime_cmd = self.tools_config.get("podmanCommand", DEFAULT_PODMAN_CMD)

        self._environment.update(
            {
                "XDG_DATA_HOME": os.path.join(self.stagedContainersDir, ".podman"),
            }
        )

        # Now, detect whether userns could work
        userns_supported = False
        if self.supportsFeature("host_userns"):
            userns_supported = True
            self._features.add("userns")

        self.logger.debug(f"Podman supports userns: {userns_supported}")

    @classmethod
    def ContainerType(cls) -> "ContainerType":
        return ContainerType.Podman

    @classmethod
    def variant_name(self) -> "str":
        return "podman"

    @property
    def architecture(self) -> "Tuple[ContainerOperatingSystem, ProcessorArchitecture]":
        v_retval, payload, v_stderr = self._version()

        if v_retval != 0:
            errstr = """Could not get podman version. Retval {}
======
STDOUT
======
{}

======
STDERR
======
{}""".format(
                v_retval, payload, v_stderr
            )
            raise ContainerEngineException(errstr)

        try:
            version_json = json.loads(payload)
            osstr, arch = version_json["Client"]["OsArch"].split("/")

            # Trying to be coherent with Python
            if arch == "amd64":
                arch = "x86_64"

            return cast("ContainerOperatingSystem", osstr), cast(
                "ProcessorArchitecture", arch
            )
        except Exception as e:
            raise ContainerEngineException(
                "Ill-formed answer from podman version"
            ) from e

    def _genPodmanTag(
        self,
        tag: "ContainerTaggedName",
    ) -> "Tuple[URIType, str, str]":
        # It is an absolute URL, we are removing the docker://
        tag_name = tag.origTaggedName
        if tag_name.startswith(DOCKER_PROTO):
            dockerTag = tag_name[len(DOCKER_PROTO) :]
        else:
            dockerTag = tag_name

        # Should we enrich the tag with the registry?
        if isinstance(tag.registries, dict) and (
            (ContainerType.Docker in tag.registries)
            or (ContainerType.Podman in tag.registries)
        ):
            if ContainerType.Podman in tag.registries:
                registry = tag.registries[ContainerType.Podman]
            else:
                registry = tag.registries[ContainerType.Docker]

            # Bare case
            if "/" not in dockerTag:
                dockerTag = f"{registry}/library/{dockerTag}"
            elif dockerTag.find("/") == dockerTag.rfind("/"):
                dockerTag = f"{registry}/{dockerTag}"
            # Last case, it already has a registry declared

        # This is needed ....
        if isinstance(tag, Container) and tag.signature is not None:
            shapos = dockerTag.rfind("@sha256:")
            if shapos != -1:
                # The sha256 tag takes precedence over the recorded signature
                dockerPullTag = dockerTag
            else:
                colonpos = dockerTag.rfind(":")
                slashpos = dockerTag.rfind("/")
                if colonpos > slashpos:
                    dockerPullTag = dockerTag[:colonpos]
                else:
                    dockerPullTag = dockerTag
                dockerPullTag += "@sha256:" + tag.signature
        else:
            dockerPullTag = dockerTag

        podmanPullTag = DOCKER_PROTO + dockerPullTag

        return cast("URIType", dockerTag), dockerPullTag, podmanPullTag

    def materializeSingleContainer(
        self,
        tag: "ContainerTaggedName",
        containers_dir: "Optional[AnyPath]" = None,
        offline: "bool" = False,
        force: "bool" = False,
    ) -> "Optional[Container]":
        """
        It is assured the containers are materialized
        """

        matEnv = dict(os.environ)
        matEnv.update(self.environment)

        # It is an absolute URL, we are removing the docker://
        tag_name = tag.origTaggedName
        dockerTag, dockerPullTag, podmanPullTag = self._genPodmanTag(tag)

        self.logger.info(f"downloading podman container: {tag_name} => {podmanPullTag}")

        fetch_metadata = True
        trusted_copy = False
        localContainerPath: "Optional[AbsPath]" = None
        localContainerPathMeta: "Optional[AbsPath]" = None
        imageSignature: "Optional[Fingerprint]" = None
        image_id: "Optional[Fingerprint]" = None
        manifestsImageSignature: "Optional[Fingerprint]" = None
        manifests = None
        manifest = None
        if not force:
            (
                trusted_copy,
                localContainerPath,
                localContainerPathMeta,
                imageSignature,
            ) = self.cc_handler.query(tag)

            if trusted_copy:
                try:
                    with open(localContainerPathMeta, mode="r", encoding="utf-8") as mH:
                        signaturesAndManifest = cast(
                            "DockerManifestMetadata", json.load(mH)
                        )
                        image_id = signaturesAndManifest["image_id"]
                        imageSignature_in_metadata = signaturesAndManifest[
                            "image_signature"
                        ]
                        manifestsImageSignature = signaturesAndManifest[
                            "manifests_signature"
                        ]
                        manifests = signaturesAndManifest["manifests"]

                        # Check the status of the gathered manifests
                        trusted_copy = (
                            manifestsImageSignature
                            == self._gen_trimmed_manifests_signature(manifests)
                        )

                        if trusted_copy:
                            trusted_copy = imageSignature == imageSignature_in_metadata
                            fetch_metadata = not trusted_copy
                except Exception as e:
                    self.logger.exception(
                        f"Problems extracting docker metadata at {localContainerPathMeta}"
                    )
                    trusted_copy = False

        # And now, the final judgement!
        if not trusted_copy:
            if offline:
                raise ContainerFactoryException(
                    f"Banned remove podman containers in offline mode from {tag_name}"
                )

            if (
                localContainerPathMeta is not None
                and localContainerPath is not None
                and (
                    os.path.exists(localContainerPathMeta)
                    or os.path.exists(localContainerPath)
                )
            ):
                self.logger.warning(
                    f"Unable to trust Podman container {dockerTag} => {podmanPullTag} . Discarding cached contents"
                )

            # Blindly remove
            _, _, _ = self._rmi(dockerTag, matEnv)

            # And now, let's materialize the new world
            d_retval, d_out_v, d_err_v = self._pull(podmanPullTag, matEnv)

            if d_retval == 0 and dockerTag != dockerPullTag:
                # Second try
                d_retval, d_out_v, d_err_v = self._tag(dockerPullTag, dockerTag, matEnv)

            if d_retval == 0:
                # Second try
                d_retval, d_out_v, d_err_v = self._inspect(dockerTag, matEnv)

            if d_retval != 0:
                errstr = """Could not materialize podman image {}. Retval {}
======
STDOUT
======
{}

======
STDERR
======
{}""".format(
                    podmanPullTag, d_retval, d_out_v, d_err_v
                )
                raise ContainerEngineException(errstr)

            # Parsing the output from podman inspect
            try:
                manifests = cast("Sequence[Mapping[str, Any]]", json.loads(d_out_v))
                manifest = manifests[0]
                image_id = cast("Fingerprint", manifest["Id"])
                manifestsImageSignature = self._gen_trimmed_manifests_signature(
                    manifests
                )
            except Exception as e:
                raise ContainerFactoryException(
                    f"FATAL ERROR: Podman finished properly but it did not properly materialize {tag_name}: {e}"
                )

            self.logger.info(
                f"saving podman container (for reproducibility matters): {tag_name}"
            )

            # Let's materialize the container image for preservation
            tmpContainerPath = self.cc_handler._genTmpContainerPath()

            # Now, save the image as such
            d_retval, d_err_ev = self._save(dockerTag, tmpContainerPath, matEnv)
            self.logger.debug("podman save retval: {}".format(d_retval))
            self.logger.debug("podman save stderr: {}".format(d_err_v))

            if d_retval != 0:
                errstr = """Could not save podman image {}. Retval {}
======
STDERR
======
{}""".format(
                    dockerTag, d_retval, d_err_v
                )

                # Removing partial dumps
                if os.path.exists(tmpContainerPath):
                    try:
                        os.unlink(tmpContainerPath)
                    except:
                        pass
                raise ContainerEngineException(errstr)

            # This is needed for the metadata
            imageSignature = self.cc_handler._computeFingerprint(
                cast("AnyPath", tmpContainerPath)
            )

            tmpContainerPathMeta = tmpContainerPath + META_JSON_POSTFIX

            # Last, save the metadata itself for further usage
            with open(tmpContainerPathMeta, mode="w", encoding="utf-8") as tcpM:
                manifest_metadata: "DockerManifestMetadata" = {
                    "image_id": image_id,
                    "image_signature": imageSignature,
                    "manifests_signature": manifestsImageSignature,
                    "manifests": manifests,
                }
                json.dump(manifest_metadata, tcpM)

            # And update the cache
            self.cc_handler.update(
                tag,
                image_path=tmpContainerPath,
                image_metadata_path=cast("AbsPath", tmpContainerPathMeta),
                do_move=True,
            )

        if containers_dir is None:
            containers_dir = self.stagedContainersDir

        # Do not allow overwriting in offline mode
        transferred_image = self.cc_handler.transfer(
            tag, stagedContainersDir=containers_dir, force=force and not offline
        )
        assert transferred_image is not None, f"Unexpected cache miss for {tag}"
        containerPath, containerPathMeta = transferred_image

        assert manifestsImageSignature is not None
        assert manifests is not None
        if manifest is None:
            manifest = manifests[0]

        # Now the image is not loaded here, but later in deploySingleContainer
        # Then, compute the fingerprint based on remote repo's information
        fingerprint = None
        if len(manifest["RepoDigests"]) > 0:
            fingerprint = manifest["RepoDigests"][0]

        # Learning about the intended processor architecture and variant
        architecture = manifest.get("Architecture")
        # As of version 4.5.0, podman does not report the architecture variant
        if architecture is not None:
            variant = manifest.get("Variant")
            if variant is not None:
                architecture += "/" + variant
        # And add to the list of containers
        return Container(
            origTaggedName=tag_name,
            taggedName=dockerTag,
            signature=image_id,
            fingerprint=fingerprint,
            architecture=architecture,
            operatingSystem=manifest.get("Os"),
            type=self.containerType,
            localPath=containerPath,
            registries=tag.registries,
            metadataLocalPath=containerPathMeta,
            source_type=tag.type,
            image_signature=imageSignature,
        )

    def deploySingleContainer(
        self,
        container: "ContainerTaggedName",
        containers_dir: "Optional[AnyPath]" = None,
        force: "bool" = False,
    ) -> "Tuple[Container, bool]":
        # Should we load the image?
        matEnv = dict(os.environ)
        matEnv.update(self.environment)
        tag_name = container.origTaggedName

        # These are the paths to the copy of the saved container
        if containers_dir is None:
            containers_dir = self.stagedContainersDir
        containerPath, containerPathMeta = self.cc_handler.genStagedContainersDirPaths(
            container, containers_dir
        )

        imageSignature: "Optional[Fingerprint]" = None
        manifestsImageSignature: "Optional[Fingerprint]" = None
        manifests = None
        manifest = None
        if not os.path.isfile(containerPath):
            errmsg = f"Podman saved image {os.path.basename(containerPath)} is not in the staged working dir for {tag_name}"
            self.logger.warning(errmsg)
            raise ContainerFactoryException(errmsg)

        if not os.path.isfile(containerPathMeta):
            errmsg = f"FATAL ERROR: Podman saved image metadata {os.path.basename(containerPathMeta)} is not in the staged working dir for {tag_name}"
            self.logger.error(errmsg)
            raise ContainerFactoryException(errmsg)

        try:
            with open(containerPathMeta, mode="r", encoding="utf-8") as mH:
                signaturesAndManifest = cast("DockerManifestMetadata", json.load(mH))
                imageSignature_in_metadata = signaturesAndManifest["image_signature"]
                manifestsImageSignature = signaturesAndManifest["manifests_signature"]
                manifests = signaturesAndManifest["manifests"]

                if isinstance(container, Container):
                    # Reuse the input container instance
                    rebuilt_container = container
                    dockerTag = rebuilt_container.taggedName
                else:
                    manifest = manifests[0]

                    dockerTag, dockerPullTag, podmanPullTag = self._genPodmanTag(
                        container
                    )

                    image_id = signaturesAndManifest["image_id"]

                    # Then, compute the fingerprint based on remote repo's information
                    fingerprint = None
                    if len(manifest["RepoDigests"]) > 0:
                        fingerprint = manifest["RepoDigests"][0]

                    # Learning about the intended processor architecture and variant
                    architecture = manifest.get("Architecture")
                    # As of version 4.5.0, podman does not report the architecture variant
                    if architecture is not None:
                        variant = manifest.get("Variant")
                        if variant is not None:
                            architecture += "/" + variant

                    rebuilt_container = Container(
                        origTaggedName=container.origTaggedName,
                        taggedName=dockerTag,
                        signature=image_id,
                        fingerprint=fingerprint,
                        architecture=architecture,
                        operatingSystem=manifest.get("Os"),
                        type=self.containerType,
                        localPath=containerPath,
                        registries=container.registries,
                        metadataLocalPath=containerPathMeta,
                        source_type=container.type,
                        image_signature=imageSignature_in_metadata,
                    )
        except Exception as e:
            errmsg = f"Problems extracting podman metadata at {containerPathMeta}"
            self.logger.exception(errmsg)
            raise ContainerFactoryException(errmsg)

        imageSignature = self.cc_handler._computeFingerprint(containerPath)

        if imageSignature != imageSignature_in_metadata:
            errmsg = f"Image signature recorded in {os.path.basename(containerPathMeta)} does not match image signature of {os.path.basename(containerPath)}"
            self.logger.exception(errmsg)
            raise ContainerFactoryException(errmsg)

        d_retval, d_out_v, d_err_v = self._inspect(dockerTag, matEnv)
        #        d_retval, d_out_v, d_err_v = self._images(matEnv)

        if d_retval not in (0, 125):
            errstr = """Could not inspect podman image {}. Retval {}
======
STDOUT
======
{}

======
STDERR
======
{}""".format(
                dockerTag, d_retval, d_out_v, d_err_v
            )
            raise ContainerEngineException(errstr)

        # Parsing the output from podman inspect
        try:
            ins_manifests = json.loads(d_out_v)
        except Exception as e:
            errmsg = f"FATAL ERROR: Podman inspect finished properly but it did not properly answered for {tag_name}"
            self.logger.exception(errmsg)
            raise ContainerFactoryException(errmsg) from e

        # Let's load then
        do_redeploy = manifestsImageSignature != self._gen_trimmed_manifests_signature(
            ins_manifests
        )
        if do_redeploy:
            self.logger.debug(f"Redeploying {dockerTag}")
            # Should we load the image?
            d_retval, d_out_v, d_err_v = self._load(containerPath, dockerTag, matEnv)

            if d_retval != 0:
                errstr = """Could not load podman image {}. Retval {}
======
STDOUT
======
{}

======
STDERR
======
{}""".format(
                    dockerTag, d_retval, d_out_v, d_err_v
                )
                self.logger.error(errstr)
                raise ContainerEngineException(errstr)

        return rebuilt_container, do_redeploy
