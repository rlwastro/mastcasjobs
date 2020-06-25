#!/usr/bin/env python

try:
    from setuptools import setup, Extension
    setup, Extension
except ImportError:
    from distutils.core import setup
    from distutils.extension import Extension
    setup, Extension

import os
import sys

if sys.argv[-1] == "publish":
    os.system("python setup.py sdist upload")
    sys.exit()


desc = open("README.rst").read()
with open("requirements.txt") as f:
    required = f.readlines()

setup(
    name="mastcasjobs",
    version="0.0.3",
    author="Richard L. White",
    author_email="rlw@stsci.edu",
    url="https://github.com/rlwastro/mastcasjobs",
    py_modules=["mastcasjobs", ],
    install_requires=required,
    license="MIT",
    description="An interface to MAST CasJobs.",
    long_description=desc,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
)
