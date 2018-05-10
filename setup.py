from setuptools import setup, find_packages

setup(name='streamplay',
      version='0.1',
      description='streamplay - play chain streamer',
      author='Sandeep',
      author_email='sandeep@ucen.io',
      license='MIT',
      packages=find_packages(exclude=['examples']),
      install_requires=['chainsync', 'redis'],
      zip_safe=False)
