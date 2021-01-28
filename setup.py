# File: setup.py
# Date: 9-Mar-2018
#
# Update:
#   3-Jul-2018  jdw update CLI entry points and dependencies
#  21-Aug-2018  jdw version adjustments
#  22-Aug-2018  jdw adjust for namespace packaging
#  27-Aug-2018  jdw change directory containing console scripts
#  26-Oct-2018  jdw update dependencies
#  11-Mar-2019  jdw add rcsb.utils.ec and taxonomy dependencies
#
import re

from setuptools import find_packages
from setuptools import setup

packages = []
thisPackage = "rcsb.db"

with open("rcsb/db/cli/__init__.py", "r") as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError("Cannot find version information")

setup(
    name=thisPackage,
    version=version,
    description="RCSB Python Database Access and Loading Utility Classes",
    long_description="See:  README.md",
    author="John Westbrook",
    author_email="john.westbrook@rcsb.org",
    url="https://github.com/rcsb/py-rcsb_db",
    #
    license="Apache 2.0",
    classifiers=(
        "Development Status :: 3 - Alpha",
        # 'Development Status :: 5 - Production/Stable',
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
    ),
    entry_points={
        "console_scripts": [
            "exdb_repo_load_cli=rcsb.db.cli.RepoLoadExec:main",
            "repo_scan_cli=rcsb.db.cli.RepoScanExec:main",
            "schema_update_cli=rcsb.db.cli.SchemaUpdateExec:main",
            "etl_exec_cli=rcsb.db.cli.ETLExec:main",
        ]
    },
    #
    install_requires=[
        "future",
        "six",
        "python-dateutil",
        "pytz",
        "jsondiff >= 1.2.0",
        "jsonschema >= 2.6.0",
        "strict-rfc3339",
        "mysqlclient >= 1.3.12",
        "pymongo >= 3.7.0",
        "mmcif >= 0.57",
        'scandir; python_version < "3.0"',
        # 'configparser; python_version < "3.0"',
        "rcsb.utils.io >= 0.97",
        "rcsb.utils.config >= 0.35",
        "rcsb.utils.multiproc >= 0.17",
        "rcsb.utils.validation >= 0.22",
        "rcsb.utils.chemref >= 0.65",
        "rcsb.utils.citation >= 0.15",
        "rcsb.utils.ec >= 0.21",
        "rcsb.utils.taxonomy >= 0.31",
        "rcsb.utils.seq >= 0.43",
        "rcsb.utils.struct >= 0.26",
        "SQLAlchemy; sys_platform == 'linux'",
        "psycopg2-binary",
        "crate",
    ],
    packages=find_packages(exclude=["rcsb.db.tests", "rcsb.db.tests-*", "tests.*"]),
    package_data={
        # If any package contains *.md or *.rst ...  files, include them:
        "": ["*.md", "*.rst", "*.txt", "*.cfg"]
    },
    #
    # These basic tests require no database services -
    test_suite="rcsb.db.tests",
    tests_require=["tox", "jsonschema", "strict-rfc3339"],
    #
    # Not configured ...
    extras_require={"dev": ["check-manifest"], "test": ["coverage"]},
    # Added for
    command_options={"build_sphinx": {"project": ("setup.py", thisPackage), "version": ("setup.py", version), "release": ("setup.py", version)}},
    # This setting for namespace package support -
    zip_safe=False,
)
