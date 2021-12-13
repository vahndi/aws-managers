from distutils.core import setup

from setuptools import find_packages

setup(
    name='aws-managers',
    packages=find_packages(),
    version='0.002',
    license='MIT',
    description='Wrappers around boto3 and sagemaker',
    author='Vahndi Minah',
    url='https://github.com/vahndi/aws-managers',
    keywords=['boto3', 'sagemaker'],
    install_requires=[
        'botocore',
        'boto3',
        'jinja2',
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
