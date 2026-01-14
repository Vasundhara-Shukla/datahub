import os
import pathlib

import setuptools

package_metadata: dict = {}
with open("./src/datahub_soda_plugin/_version.py") as fp:
    exec(fp.read(), package_metadata)


def get_long_description():
    root = os.path.dirname(__file__)
    return pathlib.Path(os.path.join(root, "README.md")).read_text()


rest_common = {"requests", "requests_file"}

_version: str = package_metadata["__version__"]
_self_pin = (
    f"=={_version}"
    if not (_version.endswith(("dev0", "dev1")) or "docker" in _version)
    else ""
)

base_requirements = {
    # Actual dependencies.
    # Soda Core/SQL is the main dependency
    "soda-core>=3.0.0, <4.0.0",
    "pydantic>=2.1.0",
    *rest_common,
    f"acryl-datahub[datahub-rest]{_self_pin}",
}

mypy_stubs = {
    "types-dataclasses",
    "types-setuptools",
    "types-six",
    "types-python-dateutil",
    "types-requests",
    "types-toml",
    "types-PyYAML",
    "types-freezegun",
    "types-cachetools",
    "types-click==0.1.12",
    "types-tabulate",
    "types-pytz",
}

base_dev_requirements = {
    *base_requirements,
    *mypy_stubs,
    "coverage>=5.1",
    "ruff==0.11.7",
    "mypy==1.17.1",
    "pytest>=6.2.2",
    "pytest-asyncio>=0.16.0",
    "pytest-cov>=2.8.1",
    "tox",
    "deepdiff!=8.0.0",
    "requests-mock",
    "freezegun",
    "jsonpickle",
    "build",
    "twine",
    "packaging",
}

dev_requirements = {
    *base_dev_requirements,
}

integration_test_requirements = {
    *dev_requirements,
    f"acryl-datahub[testing-utils]{_self_pin}",
    "pytest-docker>=1.1.0",
}

entry_points = {
    "console_scripts": [
        "datahub-soda=datahub_soda_plugin.cli:main",
    ],
}


setuptools.setup(
    # Package metadata.
    name=package_metadata["__package_name__"],
    version=package_metadata["__version__"],
    url="https://docs.datahub.com/",
    project_urls={
        "Documentation": "https://docs.datahub.com/docs/",
        "Source": "https://github.com/datahub-project/datahub",
        "Changelog": "https://github.com/datahub-project/datahub/releases",
    },
    license="Apache-2.0",
    description="DataHub Soda plugin to capture data quality scans and send to DataHub",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "Operating System :: Unix",
        "Operating System :: POSIX :: Linux",
        "Environment :: Console",
        "Environment :: MacOS X",
        "Topic :: Software Development",
    ],
    # Package info.
    zip_safe=False,
    python_requires=">=3.9",
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="./src"),
    entry_points=entry_points,
    # Dependencies.
    install_requires=list(base_requirements),
    extras_require={
        "ignore": [],  # This is a dummy extra to allow for trailing commas in the list.
        "dev": list(dev_requirements),
        "integration-tests": list(integration_test_requirements),
    },
)
