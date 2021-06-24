from setuptools import find_packages, setup

VERSION = '0.9.0'  # sensible default, but get from the kcri.qaap module
with open("kcri/qaap/__init__.py", 'r') as f:
    for l in f:
        if l.startswith('__version__'):
             VERSION = l.split('=')[1].strip().strip('"')

NAME = 'kcriqaap'
DESCRIPTION = 'KCRI Quality Analysis and Assembly Pipeline'
URL = 'https://github.com/zwets/kcri-qaap'
EMAIL = 'zwets@kcri.ac.tz'
AUTHOR = 'Marco van Zwetselaar'
PLATFORMS = [ 'Linux' ]
REQUIRES_PYTHON = '>=3.8.0'
REQUIRED = ['picoline' ]
EXTRAS = { }

about = {'__version__': VERSION}

setup(
    name = NAME,
    version = VERSION,
    description = DESCRIPTION,
    long_description = DESCRIPTION,
    platforms = PLATFORMS,
    author = AUTHOR,
    author_email = EMAIL,
    python_requires = REQUIRES_PYTHON,
    url = URL,
    packages = find_packages(exclude=["tests"]),
    entry_points={ 'console_scripts': [ 'QAAP = kcri.bap.QAAP:main' ] },
    install_requires = REQUIRED,
    extras_require = EXTRAS,
    include_package_data = True,
    #test_suite="tests",
    license = 'GNU General Public License v3',
    classifiers = ['License :: OSI Approved :: GPL v3'],
    zip_safe = False
    )

