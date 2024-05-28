from setuptools import setup, find_packages
import re


def parse_requirement(req):

    if req.startswith("git+https"):
        match = re.match(r"git\+https://github.com/([^/]+)/([^/]+).git", req)
        if match:
            package_name = match.group(2).replace("-", "_")
            return f"{package_name} @ {req}"
    return req


with open("requirements.txt") as f:
    requirements = f.read().splitlines()

requirements = [parse_requirement(req) for req in requirements]

setup(
    name="radiointerferometry",
    version="0.1.0",
    packages=find_packages(),
    install_requires=requirements,
)
