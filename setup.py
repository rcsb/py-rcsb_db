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
from pathlib import Path

from setuptools import find_packages, setup


version = (re.compile(r"""^__version__ *= *['"]([^'"]+)['"]$""", re.MULTILINE)
    .search(Path("rcsb/db/cli/__init__.py").read_text("utf-8")).group(1))
packages = find_packages(exclude=["rcsb.db.tests", "rcsb.db.tests-*", "tests.*"])
requirements = Path("requirements.txt").read_text("utf-8").splitlines()

setup(
    name="rcsb.db",
    version=version,
    description="RCSB Python Database Access and Loading Utility Classes",
    long_description_content_type="text/markdown",
    long_description=Path("README.md").read_text(encoding="utf-8"),
    author="John Westbrook",
    author_email="john.westbrook@rcsb.org",
    url="https://github.com/rcsb/py-rcsb_db",
    license="Apache 2.0",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only",
    ],
    entry_points={
        "console_scripts": [
            "exdb_repo_load_cli=rcsb.db.cli.RepoLoadExec:main",
            "repo_scan_cli=rcsb.db.cli.RepoScanExec:main",
            "schema_update_cli=rcsb.db.cli.SchemaUpdateExec:main",
            "etl_exec_cli=rcsb.db.cli.ETLExec:main",
        ]
    },
    install_requires=requirements,
    packages=packages,
    # These basic tests require no database services -
    test_suite="rcsb.db.tests",
    tests_require=["tox", "jsonschema", "rcsb.utils.chemref>=0.91", "jsondiff>=1.2.0"],
    # This setting for namespace package support -
    zip_safe=False,
)
