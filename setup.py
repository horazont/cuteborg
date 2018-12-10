#!/usr/bin/env python3
import os.path
import runpy
import sys

import setuptools
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, "README.rst"), encoding="utf-8") as f:
    long_description = f.read()

install_requires = [
    'borgbackup',
    'pytz',
]

try:
    # this is *super* ugly, but for some reason pip/pkg_resources doesn’t find
    # PyQt5 installed by apt (but it does find borgbackup. super weird.)
    import PyQt5  # NOQA
except ImportError:
    install_requires.append("PyQt5")

setup(
    name="cuteborg",
    version="0.1.3",
    description="Third-party scheduler and UI frontend for Borgbackup",
    long_description=long_description,
    url="https://github.com/horazont/cuteborg",
    author="Jonas Wielicki",
    author_email="jonas@wielicki.name",
    license="GPLv3+",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: End Users/Desktop",
        "Operating System :: POSIX",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
    ],
    keywords="borgbackup",
    install_requires=install_requires,
    packages=find_packages(exclude=["tests*"]),
    entry_points={
        "console_scripts": [
            "cuteborg-scheduler=cuteborg.scheduler.cli:main",
            "cuteborg=cuteborg.cli:main",
            "qtborg-trayicon=qtborg.trayicon.cli:main",
        ]
    },
    include_package_data=True,
)
