from setuptools import setup, find_packages


def readme():
    with open("README.rst") as f:
        return f.read().strip()


setup(
    name="mmmeta",
    version="0.3.2",
    description="A simple toolkit for managing local state against remote metadata.",
    long_description=readme(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Topic :: Utilities",
    ],
    url="https://github.com/simonwoerpel/mmmeta",
    author="Simon Wörpel",
    author_email="simon.woerpel@medienrevolte.de",
    license="MIT",
    packages=find_packages(exclude=["mmmeta.tests"]),
    package_dir={"mmmeta": "mmmeta"},
    entry_points={"console_scripts": ["mmmeta=mmmeta.cli:cli"]},
    install_requires=[
        "click",
        "dataset",
        "banal",
        "pyyaml",
        "python-dateutil",
        "awesome-slugify",
    ],
    zip_safe=False,
)
