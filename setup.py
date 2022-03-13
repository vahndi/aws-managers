from distutils.core import setup

from setuptools import find_packages

setup(
    name='aws-managers',
    packages=find_packages(),
    package_data={
        'aws_managers': ['./templates/athena/ddl/*.jinja2',
                         './templates/athena/dml/*.jinja2']
    },
    include_package_data=False,
    version='0.025',
    license='MIT',
    description='Wrappers around boto3 and sagemaker',
    author='Vahndi Minah',
    url='https://github.com/vahndi/aws-managers',
    keywords=['boto3', 'sagemaker'],
    install_requires=[
        'awswrangler',
        'botocore',
        'boto3',
        'jinja2',
        'numpy',
        'pandas',
        'sagemaker',
        'tqdm'
    ],
    classifiers=[
      'Development Status :: 3 - Alpha',
      'Intended Audience :: Developers',
      'Topic :: Software Development :: Build Tools',
      'License :: OSI Approved :: MIT License',
      'Programming Language :: Python :: 3',
    ]
)
