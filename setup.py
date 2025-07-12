from setuptools import setup, find_packages

setup(
    name="hydragram",  # Your custom package name
    version="2.0",  # Update this for each PyPI release
    author="Endtrz",
    author_email="endtrz@gmail.com",
    description="An enhanced Pyrogram-like filter and handler system using Pyrogram.",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Endtrz/hasnainkk",  # Your GitHub repo
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pyrogram>=2.0.106",
        "tgcrypto>=1.2.5"  # Optional: but recommended for speed
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
