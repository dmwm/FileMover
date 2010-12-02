#!/usr/bin/env python

import sys
import os
from unittest import TextTestRunner, TestLoader
from glob import glob
from os.path import splitext, basename, join as pjoin, walk
from distutils.core import setup
from distutils.cmd import Command
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError
from distutils.errors import DistutilsPlatformError, DistutilsExecError
from distutils.core import Extension
from distutils.command.install import INSTALL_SCHEMES

sys.path.append(os.path.join(os.getcwd(), 'src/python'))
from fm import version as fm_version

required_python_version = '2.6'

if sys.platform == 'win32' and sys.version_info > (2, 6):
   # 2.6's distutils.msvc9compiler can raise an IOError when failing to
   # find the compiler
   build_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError,
                 IOError)
else:
   build_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)

class TestCommand(Command):
    """
    Class to handle unit tests
    """
    user_options = [ ]

    def initialize_options(self):
        """Init method"""
        self._dir = os.getcwd()

    def finalize_options(self):
        """Finalize method"""
        pass

    def run(self):
        """
        Finds all the tests modules in test/, and runs them.
        """
#        exclude = [pjoin(self._dir, 'test', 'cern_sso_auth_t.py')]
        exclude = []
        testfiles = []
        for t in glob(pjoin(self._dir, 'test', '*_t.py')):
            if  not t.endswith('__init__.py') and t not in exclude:
                testfiles.append('.'.join(
                    ['test', splitext(basename(t))[0]])
                )
        testfiles.sort()
        tests = TestLoader().loadTestsFromNames(testfiles)
        t = TextTestRunner(verbosity = 2)
        t.run(tests)

class CleanCommand(Command):
    """
    Class which clean-up all pyc files
    """
    user_options = [ ]

    def initialize_options(self):
        """Init method"""
        self._clean_me = [ ]
        for root, dirs, files in os.walk('.'):
            for f in files:
                if f.endswith('.pyc') or f.endswith('~'):
                    self._clean_me.append(pjoin(root, f))

    def finalize_options(self):
        """Finalize method"""
        pass

    def run(self):
        """Run method"""
        for clean_me in self._clean_me:
            try:
                os.unlink(clean_me)
            except:
                pass

def dirwalk(relativedir):
    """
    Walk a directory tree and look-up for __init__.py files.
    If found yield those dirs. Code based on
    http://code.activestate.com/recipes/105873-walk-a-directory-tree-using-a-generator/
    """
    dir = os.path.join(os.getcwd(), relativedir)
    for fname in os.listdir(dir):
        fullpath = os.path.join(dir, fname)
        if  os.path.isdir(fullpath) and not os.path.islink(fullpath):
            for subdir in dirwalk(fullpath):  # recurse into subdir
                yield subdir
        else:
            initdir, initfile = os.path.split(fullpath)
            if  initfile == '__init__.py':
                yield initdir

def find_packages(relativedir):
    packages = [] 
    for dir in dirwalk(relativedir):
        package = dir.replace(os.getcwd() + '/', '')
        package = package.replace(relativedir + '/', '')
        package = package.replace('/', '.')
        packages.append(package)
    return packages

def datafiles(dir):
    """Return list of data files in provided relative dir"""
    files = []
    for dirname, dirnames, filenames in os.walk(dir):
        for subdirname in dirnames:
            files.append(os.path.join(dirname, subdirname))
        for filename in filenames:
            if  filename[-1] == '~':
                continue
            files.append(os.path.join(dirname, filename))
    return files
#    return [os.path.join(dir, f) for f in os.listdir(dir)]
    
version      = fm_version
name         = "FileMover"
description  = "CMS FileMover data service"
readme       ="""FileMover service was designed to allow users 
to request/transfer data via web interface to local disk. 
"""
author       = ["Valentin Kuznetsov", "Brian Bockelman"],
author_email = ["vkuznet@gmail.com", "bbockelm@math.unl.edu"],
scriptfiles  = filter(os.path.isfile, ['etc/fm_wmcoreconfig.py'])
url          = "https://twiki.cern.ch/twiki/bin/viewauth/CMS/FileMover",
keywords     = ["FileMover", "transfer"]
package_dir  = {'fm': 'src/python/fm'}
packages     = find_packages('src/python')
data_files   = [
                ('fm/etc', ['etc/fm_wmcoreconfig.py']),
                ('fm/test', datafiles('test')),
                ('fm/web/js', datafiles('src/js')),
                ('fm/web/css', datafiles('src/css')),
                ('fm/web/images', datafiles('src/images')),
                ('fm/web/templates', datafiles('src/templates')),
               ]
license      = "CMS experiment software"
classifiers  = [
    "Development Status :: 3 - Production/Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: CMS/CERN Software License",
    "Operating System :: MacOS :: MacOS X",
    "Operating System :: Microsoft :: Windows",
    "Operating System :: POSIX",
    "Programming Language :: Python",
    "Topic :: Database"
]

def main():
    if sys.version < required_python_version:
        s = "I'm sorry, but %s %s requires Python %s or later."
        print s % (name, version, required_python_version)
        sys.exit(1)

    # set default location for "data_files" to
    # platform specific "site-packages" location
    for scheme in INSTALL_SCHEMES.values():
        scheme['data'] = scheme['purelib']

    dist = setup(
        name                 = name,
        version              = version,
        description          = description,
        long_description     = readme,
        keywords             = keywords,
        packages             = packages,
        package_dir          = package_dir,
        data_files           = data_files,
        scripts              = datafiles('bin'),
        requires             = ['python (>=2.6)', 'sphinx (>=1.0.4)',
                                'Cheetah (>=2.4)', 'cherrypy (>=3.1.2)'],
        classifiers          = classifiers,
        cmdclass             = {'test': TestCommand, 'clean': CleanCommand},
        author               = author,
        author_email         = author_email,
        url                  = url,
        license              = license,
    )

if __name__ == "__main__":
    main()

