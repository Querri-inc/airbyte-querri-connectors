from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk>=1.0.0",
    "requests>=2.28.0",
]

TEST_REQUIREMENTS = [
    "pytest>=7.0",
    "pytest-mock>=3.10",
    "requests-mock>=1.10",
]

setup(
    name="destination_querri",
    version="0.1.0",
    description="Airbyte destination connector for the Querri Data API",
    author="Querri Inc.",
    python_requires=">=3.10",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    extras_require={"dev": TEST_REQUIREMENTS},
    package_data={"destination_querri": ["spec.json"]},
    entry_points={
        "console_scripts": ["destination-querri=main:main"],
    },
)
