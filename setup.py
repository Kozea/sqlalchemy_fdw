from setuptools import find_packages, setup

setup(
    name='sqlalchemy_fdw',
    version='0.3.0',
    author='Kozea',
    license='BSD',
    install_requires=['sqlalchemy>=1.3', 'psycopg2-binary'],
    entry_points={
        'sqlalchemy.dialects': ['pgfdw = sqlalchemy_fdw.dialect:dialect']
    },
    packages=find_packages(),
)
