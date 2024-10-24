from setuptools import setup, find_packages

setup(
   name='nps_velo_import',
   version='0.2.0',
   description='nps import helper',
   packages=find_packages(),  #same as name
   license='GPLv3',
   install_requires=['pywin32'], #external packages as dependencies
   entry_points={
       "console_scripts":[
           "vi = netezza_import:main"
       ]
   }
)

# python -m build
# twine upload dist/*