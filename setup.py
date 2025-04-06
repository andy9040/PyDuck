from setuptools import setup, find_packages

setup(
    name="pyduck",
    version="0.1",
    packages=find_packages(),
    install_requires=["duckdb", "pandas"],
)
