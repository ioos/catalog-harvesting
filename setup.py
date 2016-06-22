from setuptools import find_packages, setup
from ioos_waf_harvester import __version__

def readme():
    with open('README.rst') as f:
        return f.read()

reqs = [line.strip() for line in open('requirements.txt')]

setup(
    name             = "ioos-waf-harvester",
    version          = __version__,
    description      = 'Tools for synchronizing WAFs',
    packages         = find_packages(exclude=['tests', 'test.*']),
    long_description = readme(),
    author           = 'Luke Campbell',
    author_email     = 'luke.campbell@rpsgroup.com',
    url              = '',
    install_requires = reqs,
    test_requires    = ['pytest'],
    classifiers      = [
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Topic :: Documentation :: Sphinx',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: GIS'
    ]
)
