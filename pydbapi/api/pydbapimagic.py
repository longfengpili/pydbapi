# -*- coding: utf-8 -*-
# @Author: longfengpili
# @Date:   2023-11-02 13:36:08
# @Last Modified by:   longfengpili
# @Last Modified time: 2024-11-13 10:27:55
# @github: https://github.com/longfengpili

from pathlib import Path
import pandas as pd

from IPython.core.error import UsageError
from IPython.core.interactiveshell import InteractiveShell
from IPython.core.magic import (  # type: ignore
    Magics,
    line_cell_magic,
    line_magic,
    cell_magic,
    magics_class,
)
from IPython.core.magic_arguments import (  # type: ignore
    argument,
    magic_arguments,
    parse_argstring,
)

from traitlets import Int, Bool, Dict, Instance, Unicode, default, observe  # noqa
from traitlets.config.loader import Config

from pydbapi.api import SqliteDB, MysqlDB, RedshiftDB, TrinoDB


# 注册magic命令
def load_ipython_extension(ipython):
    ipython.register_magics(PydbapiMagics)


def register_magic():
    # IPython 启动目录及脚本
    ipython_startup_dir = Path.home() / ".ipython" / "profile_default" / "startup"
    startup_script_path = ipython_startup_dir / "00-pydbapi-startup.py"
    
    # 确保启动目录存在
    if not ipython_startup_dir.exists():
        ipython_startup_dir.mkdir(parents=True)
    
    # 写入启动脚本
    with open(startup_script_path, 'w') as f:
        f.write("get_ipython().run_line_magic('load_ext', 'pydbapi')\n")


@magics_class
class PydbapiMagics(Magics):
    dbtype = Unicode('trino',
                     help=('current use dbtype, default: trino ~\n\n'
                           'It supports dbtype:\n\n'
                           '- sqlite\n\n'
                           '- mysql\n\n'
                           '- doris\n\n'
                           '- redshift\n\n'
                           '- trino\n\n'
                           )
                     ).tag(config=True)

    host = Unicode(allow_none=False, 
                   help=('Database host')
                   ).tag(config=True)

    port = Int(allow_none=False, 
               help=('Database port')
               ).tag(config=True)

    @default('port')
    def _default_port(self):
        dbtype = self.dbtype
        if dbtype == 'mysql':
            port = 3306
        elif dbtype == 'trino':
            port = 8443
        else:
            port = None
        return port

    user = Unicode(allow_none=False, 
                   help=('Database username')
                   ).tag(config=True)

    password = Unicode(allow_none=False, 
                       help=('Database password')
                       ).tag(config=True)

    database = Unicode(allow_none=False, 
                       help=('Database database')
                       ).tag(config=True)

    catalog = Unicode('hive', 
                      help=('Database catalog, you will set when you use trino')
                      ).tag(config=True)

    auto_rule = Bool(True, 
                     help=('If you want to use DDL, you should set False')
                     ).tag(config=True)

    def __init__(self, shell: InteractiveShell = None):
        super(PydbapiMagics, self).__init__(shell)

    @property
    def dbapi(self):
        if not self.dbtype:
            self.dbtype = input('please input your dbtype:')
            if self.dbtype not in ('sqlite', 'mysql', 'doris', 'redshift', 'trino'):
                raise TypeError(f"not supported {self.dbtype}")
        if not self.host:
            self.host = input('please input your host:')
        if not self.port:
            port = input('please input your port:')
            self.port = int(port)
        if not self.user:
            self.user = input('please input your user:')
        if not self.password:
            self.password = input('please input your password:')
        if not self.database:
            self.database = input('please input your database:')

        dbtype = self.dbtype
        if dbtype == 'sqlite':
            dbapi = SqliteDB(self.database)
        elif dbtype == 'mysql':
            dbapi = MysqlDB(self.host, self.user, self.password, self.database, self.port, safe_rule=self.auto_rule)
        elif dbtype == 'doris':
            dbapi = MysqlDB(self.host, self.user, self.password, self.database, self.port, safe_rule=self.auto_rule, isdoris=True)
        elif dbtype == 'redshift':
            dbapi = RedshiftDB(self.host, self.user, self.password, self.database, self.port, safe_rule=self.auto_rule)
        elif dbtype == 'trino':
            dbapi = TrinoDB(self.host, self.user, self.password, self.database, self.catalog, self.port, safe_rule=self.auto_rule)
        else:
            pass

        return dbapi

    @magic_arguments()
    @argument('--verbose', '-v', default=0, type=int, help="Whether to show verbose")
    @argument('--dataname', '-d', default='pyresult', type=str, help="Whether to convert to DataFrame")
    @argument('--number', '-n', default=None, type=int, help="how many data to show")
    @line_cell_magic
    def pydbapi(self, line, cell=None):
        '''[summary]
        
        [pytdbapi in jupyter]
        
        Args:
            line ([str]): [line]
            cell ([str]): [cell] (default: `None`)
        '''
        if not cell:
            return

        args = parse_argstring(self.pydbapi, line)
        number, verbose, dataname = args.number, args.verbose, args.dataname
        
        cursor, action, results = self.dbapi.execute(sql=cell, count=number, verbose=verbose)
        data = results.to_dataframe()
        self.shell.user_ns[dataname] = data

        return data

    @line_magic
    def dbconfig(self, line):
        """Used for displaying and modifying xinghuo configurations.

        Exemplar usage:

        - %dbconfig
          print all the configurable parameters and its current value

        - %dbconfig <parameter_name>
          print the current value of the parameter

        - %dbconfig <parameter_name>=<value>
          set the value of the parameter
        """
        line = line.strip().split('#')[0].strip()
        class_configs = self.class_own_traits()

        if not line or line.startswith('#'):
            doc = self.class_get_help()
            print(doc)
            return
        elif line.lower() in class_configs.keys():
            return getattr(self, line.lower())
        elif '=' in line and line.split('=')[0].strip().lower() in class_configs.keys():
            param, value = line.strip().split('=')
            line = param.lower() + '=' + value
            cfg = Config()
            exec(f'cfg.{self.__class__.__name__}.{line}', self.shell.user_ns, locals())
            self.update_config(cfg)
        elif line in ('-h', '--help'):
            print(
                    "It supports the following usage:\n"
                    "- %dbconfig\n  print all the configurable parameters and its current value\n"
                    "- %dbconfig <parameter_name>\n  print the current value of the parameter\n"
                    "- %dbconfig <parameter_name>=<value>\n  set the value of the parameter"
                )
        else:
            raise UsageError(
                    f"Invalid usage of the dbconfig command: {line}.\n"
                    f"It only supports the following params: {class_configs.keys()}"
                )
