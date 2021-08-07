from setuptools import setup
import setuptools
with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='simplesql-uraniumkid30',
    version='0.0.1',
    author='Christopher Okoro',
    author_email='christopherokoro007@gmail.com',
    description='a pip-installable package example',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/bgse/hellostackoverflow',
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database :: Database Engines/Servers",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3',
)
