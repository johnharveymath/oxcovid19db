from distutils.core import setup

setup(
    name='OxCOVID19',
    version='0.1.0',
    author='John Harvey',
    author_email='john.harvey.math@gmail.com',
    packages=['oxcovid19db'],
    scripts=['bin/example.py'],
    url='http://pypi.python.org/pypi/OxCOVID19/',
    license='LICENSE.txt',
    description='For interacting with the OxCOVID19 Database.',
    long_description=open('README.txt').read(),
    install_requires=[
        "pandas",
        "psycopg2"
    ],
)
