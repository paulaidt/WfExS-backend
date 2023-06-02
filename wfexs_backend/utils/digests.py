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

import base64
import functools
import hashlib
import json
import os
from typing import (
    cast,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from typing import (
        Any,
        Callable,
        IO,
        Mapping,
        MutableSequence,
        Optional,
        Sequence,
        Tuple,
        Union,
    )

    from typing_extensions import (
        Protocol,
        TypeAlias,
    )

    from ..common import (
        AbstractGeneratedContent,
        AnyPath,
        Fingerprint,
    )

    FingerprintMethod: TypeAlias = Callable[[str, bytes], Fingerprint]
    RawFingerprintMethod: TypeAlias = Callable[[str, bytes], bytes]

    class Hexable(Protocol):
        def hex(self) -> "str":
            ...


from ..common import (
    scantree,
    GeneratedContent,
)


# Next methods have been borrowed from FlowMaps
DEFAULT_DIGEST_ALGORITHM = "sha256"
DEFAULT_DIGEST_BUFFER_SIZE = 65536


def stringifyDigest(digestAlgorithm: "str", digest: "bytes") -> "Fingerprint":
    return cast(
        "Fingerprint",
        "{0}={1}".format(
            digestAlgorithm, str(base64.standard_b64encode(digest), "iso-8859-1")
        ),
    )


def unstringifyDigest(digestion: "Fingerprint") -> "Tuple[bytes, str]":
    algo, b64digest = digestion.split("=", 1)
    return base64.b64decode(b64digest), algo


def hexDigest(digestAlgorithm: "str", digest: "Hexable") -> "Fingerprint":
    return cast("Fingerprint", digest.hex())


def stringifyFilenameDigest(digestAlgorithm: "str", digest: "bytes") -> "Fingerprint":
    return cast(
        "Fingerprint",
        "{0}~{1}".format(
            digestAlgorithm, str(base64.urlsafe_b64encode(digest), "iso-8859-1")
        ),
    )


def nullProcessDigest(digestAlgorithm: "str", digest: "bytes") -> "bytes":
    return digest


from rfc6920.methods import generate_nih_from_digest  # type: ignore[import]

# As of https://datatracker.ietf.org/doc/html/rfc6920#page-17
# rewrite the names of the algorithms
VALID_NI_ALGOS: "Mapping[str, str]" = {
    "sha256": "sha-256",
    "sha256-128": "sha-256-128",
    "sha256_128": "sha-256-128",
    "sha256-120": "sha-256-120",
    "sha256_120": "sha-256-120",
    "sha256-96": "sha-256-96",
    "sha256_96": "sha-256-96",
    "sha256-64": "sha-256-64",
    "sha256_64": "sha-256-64",
    "sha256-32": "sha-256-32",
    "sha256_32": "sha-256-32",
}


def nihDigester(digestAlgorithm: "str", digest: "bytes") -> "Fingerprint":
    # Added fallback, in case it cannot translate the algorithm
    digestAlgorithm = VALID_NI_ALGOS.get(digestAlgorithm, digestAlgorithm)

    return cast("Fingerprint", generate_nih_from_digest(digest, algo=digestAlgorithm))


def ComputeDigestFromObject(
    obj: "Any",
    digestAlgorithm: "str" = DEFAULT_DIGEST_ALGORITHM,
    repMethod: "FingerprintMethod" = stringifyDigest,
) -> "Fingerprint":
    """
    Accessory method used to compute the digest of an input file-like object
    """
    h = hashlib.new(digestAlgorithm)
    h.update(json.dumps(obj, sort_keys=True).encode("utf-8"))

    return repMethod(digestAlgorithm, h.digest())


def ComputeDigestFromFileLike(
    filelike: "IO[bytes]",
    digestAlgorithm: "str" = DEFAULT_DIGEST_ALGORITHM,
    bufferSize: "int" = DEFAULT_DIGEST_BUFFER_SIZE,
    repMethod: "Union[FingerprintMethod, RawFingerprintMethod]" = stringifyDigest,
) -> "Union[Fingerprint, bytes]":
    """
    Accessory method used to compute the digest of an input file-like object
    """
    h = hashlib.new(digestAlgorithm)
    buf = filelike.read(bufferSize)
    while len(buf) > 0:
        h.update(buf)
        buf = filelike.read(bufferSize)

    return repMethod(digestAlgorithm, h.digest())


@functools.lru_cache(maxsize=32)
def ComputeDigestFromFile(
    filename: "AnyPath",
    digestAlgorithm: "str" = DEFAULT_DIGEST_ALGORITHM,
    bufferSize: "int" = DEFAULT_DIGEST_BUFFER_SIZE,
    repMethod: "Union[FingerprintMethod, RawFingerprintMethod]" = stringifyDigest,
) -> "Optional[Union[Fingerprint, bytes]]":
    """
    Accessory method used to compute the digest of an input file
    """

    # "Fast" compute: no report, no digest
    if repMethod is None:
        return None

    with open(filename, mode="rb") as f:
        return ComputeDigestFromFileLike(f, digestAlgorithm, bufferSize, repMethod)


def ComputeDigestFromDirectory(
    dirname: "AnyPath",
    digestAlgorithm: "str" = DEFAULT_DIGEST_ALGORITHM,
    bufferSize: "int" = DEFAULT_DIGEST_BUFFER_SIZE,
    repMethod: "FingerprintMethod" = stringifyDigest,
) -> "Fingerprint":
    """
    Accessory method used to compute the digest of an input directory,
    based on the names and digest of the files in the directory
    """
    cEntries: "MutableSequence[Tuple[bytes, bytes]]" = []
    # First, gather and compute all the files
    for entry in scantree(dirname):
        if entry.is_file():
            cEntries.append(
                (
                    os.path.relpath(entry.path, dirname).encode("utf-8"),
                    cast(
                        "bytes",
                        ComputeDigestFromFile(entry.path, repMethod=nullProcessDigest),
                    ),
                )
            )

    # Second, sort by the relative path, bytes encoded in utf-8
    cEntries = sorted(cEntries, key=lambda e: e[0])

    # Third, digest compute
    h = hashlib.new(digestAlgorithm)
    for cRelPathB, cDigest in cEntries:
        h.update(cRelPathB)
        h.update(cDigest)

    return repMethod(digestAlgorithm, h.digest())


def ComputeDigestFromGeneratedContentList(
    dirname: "AnyPath",
    theValues: "Sequence[AbstractGeneratedContent]",
    digestAlgorithm: "str" = DEFAULT_DIGEST_ALGORITHM,
    bufferSize: "int" = DEFAULT_DIGEST_BUFFER_SIZE,
    repMethod: "FingerprintMethod" = stringifyDigest,
) -> "Fingerprint":
    """
    Accessory method used to compute the digest of an input directory,
    based on the names and digest of the files in the directory
    """
    cEntries: "MutableSequence[Tuple[bytes, bytes]]" = []
    # First, gather and compute all the files
    for theValue in theValues:
        if isinstance(theValue, GeneratedContent):
            cEntries.append(
                (
                    os.path.relpath(theValue.local, dirname).encode("utf-8"),
                    cast(
                        "bytes",
                        ComputeDigestFromFile(
                            theValue.local, repMethod=nullProcessDigest
                        ),
                    ),
                )
            )

    # Second, sort by the relative path, bytes encoded in utf-8
    cEntries = sorted(cEntries, key=lambda e: e[0])

    # Third, digest compute
    h = hashlib.new(digestAlgorithm)
    for cRelPathB, cDigest in cEntries:
        h.update(cRelPathB)
        h.update(cDigest)

    return repMethod(digestAlgorithm, h.digest())
