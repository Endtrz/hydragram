from setuptools import setup, find_packages

setup(
    name="hydragram",
    version="1.0.0",
    author="Endtrz",
    author_email="endtrz@gmail.com",
    description="An enhanced Pyrogram-like filter and handler system using Kurigram.",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Endtrz/hasnainkk",  # Change to your repo URL if any
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "kurigram>=2.2.0", 
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
