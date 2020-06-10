# @Author: chunyang.xu
# @Email:  398745129@qq.com
# @Date:   2020-06-09 16:46:54
# @Last Modified time: 2020-06-10 15:19:20
# @github: https://github.com/longfengpili

#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import setuptools

VERSION = '0.0.8'

with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

with open('./requirements.txt', 'r', encoding='utf-8') as f:
    requires = f.readlines()


setuptools.setup(
    name="pydbapi", # Replace with your own username
    version=VERSION,
    author="longfengpili",
    author_email="398745129@qq.com",
    description="A simple database API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/longfengpili/pydbapi",
    packages=setuptools.find_packages(),
    install_requires=requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    keywords = ("dbapi", "sqlite3", "redshift"), 
    python_requires='>=3.6',
)



# python setup.py sdist bdist_wheel
# python -m twine upload dist/*0.0.8* --repository pydbapi
