from setuptools import setup, find_packages


requires = ['botocore>=0.8.2',
            'requests>=1.2.0']

args = dict(
    name='awsflow',
    version='1.0-preview',
    author='Amazon.com',
    description="The The AWS Flow Framework is a programming framework that "
    "works together with Amazon Simple Workflow Service (Amazon SWF) to "
    "help developers build asynchronous and distributed applications",
    packages=find_packages('lib'),
    scripts=[],
    cmdclass={},
    install_requires=requires,
    license=open("LICENSE.txt").read(),
    package_dir={'' : 'lib'},
    )


# setup sphinx command
try:
    from sphinx.setup_command import BuildDoc
    args['cmdclass']['build_sphinx'] = BuildDoc
except ImportError:
    pass

setup(**args)
