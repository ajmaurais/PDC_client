from setuptools import setup, find_packages

setup(name='PDC_client',
      version=0.2,
      author='Aaron Maurais',
      url='https://github.com/ajmaurais/PDC_client',
      packages=find_packages(),
      package_dir={'PDC_client': 'PDC_client'},
      python_requires='>=3.8',
      install_requires=['requests>=2.27.1'],
      entry_points={'console_scripts': ['PDC_client=PDC_client:main']}
)
