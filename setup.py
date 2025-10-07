from setuptools import setup, find_packages

setup(
    name="chdb",          
    version="0.1.0",            
    author="Young",
    author_email="13515360252@163.com",
    description="A Well-Encapsulated ClickHouse Database APIs Lib",
    long_description=open("README.md").read(),  
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/my_package",
    packages=find_packages(),   
    install_requires=[          
        "requests>=2.25.1"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",  
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.11',  
)