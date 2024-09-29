from setuptools import setup, find_packages

setup(
   name='netezza_import',
   version='0.1.0',
   description='Netezza import helper',
   packages=find_packages(),  #same as name
   license='GPLv3',
   install_requires=['pywin32'], #external packages as dependencies
   entry_points={
       "console_scripts":[
           "nz_csv_pipe = netezza_import:main"
       ]
   }
)

# python -m build
# twine upload dist/*