from setuptools import setup, find_packages
setup(
 name='sqlalchemy_fdw',
 version='0.0.1',
 author='Kozea',
 license='BSD',
 install_requires=['sqlalchemy>=0.9', 'psycopg2'],
 entry_points={
     'sqlalchemy.dialects': ['pgfdw = sqlalchemy_fdw.dialect:dialect']
 },
 packages=find_packages(),
)
