"""Python setup.py for simple_baserow_api package"""
import io
import os
from setuptools import find_packages, setup


def read(*paths, **kwargs):
    """Read the contents of a text file safely.
    >>> read("simple_baserow_api", "VERSION")
    '0.1.0'
    >>> read("README.md")
    ...
    """

    content = ""
    with io.open(
        os.path.join(os.path.dirname(__file__), *paths),
        encoding=kwargs.get("encoding", "utf8"),
    ) as open_file:
        content = open_file.read().strip()
    return content


def read_requirements(path):
    return [
        line.strip()
        for line in read(path).split("\n")
        if not line.startswith(('"', "#", "-", "git+"))
    ]


setup(
    name="simple_baserow_api",
    version=read("simple_baserow_api", "VERSION"),
    description="Awesome simple_baserow_api created by KuechlerO",
    url="https://github.com/KuechlerO/simple_baserow_api/",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="KuechlerO",
    packages=find_packages(exclude=["tests", ".github"]),
    install_requires=read_requirements("requirements.txt"),
    entry_points={
        "console_scripts": ["simple_baserow_api = simple_baserow_api.__main__:main"]
    },
    extras_require={"test": read_requirements("requirements-test.txt")},
)
