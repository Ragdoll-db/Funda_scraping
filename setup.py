from setuptools import find_packages, setup

setup(
    name="SP500",
    packages=find_packages(exclude=["SP500_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "selenium"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
