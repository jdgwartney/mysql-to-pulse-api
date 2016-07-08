from distutils.core import setup

setup(
    name='tspmysql',
    version='0.0.1',
    url="http://github.io/boundary/mysql-to-pulse-api",
    author='David Gwartney',
    author_email='david_gwartney@bmc.com',
    packages=['tspmysql', ],
    scripts=[
    ],
#    package_data={'tspmysql': ['templates/*']},
    license='LICENSE',
    entry_points={
        'console_scripts': [
            'tsp-mysql = tspmysql.cli:main',
         ],
    },
    description='TrueSight Pulse MySQL ETL',
    long_description=open('README.txt').read(),
    install_requires=[
       'python-dateutil >= 2.5.2',
       'tspapi >= 0.3.5',
       'pyowm >= 2.3.0',
       'petl >= 1.1.0',
    ],
)
