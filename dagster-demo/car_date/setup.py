from setuptools import find_packages, setup

setup(
    name="car_date",
    packages=find_packages(exclude=["car_date_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
