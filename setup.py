# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-06-02 15:27:41
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-21 11:59:07
# @github: https://github.com/longfengpili


import os
import setuptools
from pathlib import Path
from setuptools.command.install import install

VERSION = '0.0.138'
PROJECT_NAME = 'pydbapi'

with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

with open('requirements.txt', 'r', encoding='utf-8') as f:
    requires = f.readlines()


class CustomInstallCommand(install):
    """自定义安装命令以自动在 IPython 启动目录中添加启动脚本"""
    
    def run(self):
        # 首先执行标准的安装过程
        install.run(self)
        
        # IPython 启动目录及脚本
        ipython_startup_dir = Path.home() / ".ipython" / "profile_default" / "startup"
        startup_script_path = ipython_startup_dir / "00-pydbapi-startup.py"
        
        # 确保启动目录存在
        if not os.path.exists(ipython_startup_dir):
            os.makedirs(ipython_startup_dir)
        
        # 写入启动脚本
        with open(startup_script_path, 'w') as f:
            f.write("get_ipython().run_line_magic('load_ext', 'pydbapi')\n")


setuptools.setup(
    name=PROJECT_NAME,  # Replace with your own username
    version=VERSION,
    author="longfengpili",
    author_email="398745129@qq.com",
    description="A simple database API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=f"https://pypi.org/project/{PROJECT_NAME}/",
    packages=setuptools.find_packages(exclude=["tests.*", "tests"]),
    install_requires=requires,
    entry_points={
        'ipython.extensions': [
            'pydbapi = pydbapi.api.pydbapimagic:load_ipython_extension',
        ],
        'console_scripts': [
            'pydbapimagic = pydbapi.api.pydbapimagic:register_magic',
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    keywords=["dbapi", "sqlite3", "redshift", 'snowflake', 'doris', 'trino'],
    python_requires=">=3.9",
    project_urls={
        'Documentation': f'https://github.com/longfengpili/{PROJECT_NAME}/blob/master/README.md',
        'Source': f'https://github.com/longfengpili/{PROJECT_NAME}',
    },
    cmdclass={
        'install': CustomInstallCommand,
    },
)
