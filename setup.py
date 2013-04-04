# For more details on how to operate this file, check
# https://w.amazon.com/index.php/Python/Brazil
import re
from setuptools import setup, find_packages, Command
from glob import glob

# declare your scripts:
# scripts in bin/ with a shebang containing python will be
# recognized automatically
scripts = []
for fname in glob('bin/*'):
    with open(fname, 'r') as fh:
        if re.search(r'^#!.*python', fh.readline()):
            scripts.append(fname)

# if you have any python modules (.py files instead of dir/__init__.py
# packages) list them here:
py_modules = []

# add custom commands here:
cmdclass = {}



# you should not need to modify anything below, unless you need to build
# C extensions

args = dict(
    name='Python-awsflow',
    version='1.0',
    # declare your packages
    packages=find_packages('lib'),
    # declare your scripts
    scripts=scripts,
    # declare custom commads
    cmdclass=cmdclass,
    package_dir={'' : 'lib'},
    options={
        # make sure the right shebang is set for the scripts
        'build_scripts': {
            'executable': '/apollo/sbin/envroot "$ENVROOT/bin/python"',
            },
        },
    )

# setup flake8 command
try:
    import flake8.run
    import flake8.pep8
    class Flake8Command(Command):
        description = "Python style guide checker"
        user_options = []

        def initialize_options(self):
            pass

        def finalize_options(self):
            pass

        def run(self):
            flake8.run.pep8style = flake8.pep8.StyleGuide(config_file=True)
            flake8.run._initpep8()

            checked = set()

            warnings = 0
            for path in flake8.run._get_python_files(['lib', 'bin']):
                if path not in checked:
                    warnings += flake8.run.check_file(path)
                checked.add(path)

            raise SystemExit(warnings > 0)

    args['cmdclass']['flake8'] = Flake8Command

except ImportError:
    pass


# setup sphinx command
try:
    from sphinx.setup_command import BuildDoc
    args['cmdclass']['build_sphinx'] = BuildDoc
except ImportError:
    pass

if py_modules:
    args['py_modules'] = py_modules

setup(**args)
