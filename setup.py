from setuptools import setup, find_packages

import botoflow

requires = ['botocore>=1.1.10',
            'six>=1.2.0',
            'dill>=0.2',
            'retrying>=1.3.3']

args = dict(
    name='botoflow',
    version=botoflow.__version__,
    author='Amazon.com, Darjus Loktevic',
    author_email='darjus@gmail.com',
    description="Botoflow is an asynchronous framework for Amazon SWF that helps you "
                "build SWF applications using Python",
    long_description=open('README.rst').read(),
    url='https://github.com/boto/botoflow',
    packages=find_packages(exclude=['test*']),
    scripts=[],
    cmdclass={},
    install_requires=requires,
    license="Apache License 2.0",
    classifiers=(
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ),
    )

setup(**args)

