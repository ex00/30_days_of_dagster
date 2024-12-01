from setuptools import find_packages, setup

setup(
    name="dagster_advent_of_code",
    packages=find_packages(exclude=["dagster_advent_of_code_tests"]),
    install_requires=[
        "dagster",
        "pandas"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
