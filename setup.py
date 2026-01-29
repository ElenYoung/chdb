from setuptools import setup, find_packages

setup(
    name="quantchdb",          
    version="0.2.0",            
    author="Young",
    author_email="yang13515360252@163.com",
    description="A Well-Encapsulated ClickHouse Database APIs Lib",
    long_description=open("README.md", encoding="utf-8").read(),  
    long_description_content_type="text/markdown",
    url="https://github.com/ElenYoung/chdb",
    package_dir={'': 'src'}, 
    packages=find_packages(where='src'),   
    install_requires=[   
        "numpy>=2",       
        "clickhouse-driver>=0.2.9",
        "pandas>=2",
        "pytz>=2025.2",
        "coloredlogs>=15.0.1",
        "python-dotenv>=1.1.1",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",  
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',  
)