from setuptools import setup, find_packages


install_requires = [
    "argparse",
     "pyspark"
]

tests_require = [
    "pytest"
]

dev_requires = tests_require + [

    "flake8",
    "black",
    "sphinx",
    "pylint",
]

with open('README.md') as f:
    readme = f.read()


setup(
    name='route_pipeline',
    version='0.2.7',
    description='Sample pipeline(s) handling batch and streaming route data',
    long_description=readme,
    author='David van der Vliet',
    author_email='d.vander.vliet@live.nl',
    url='https://github.com/Deefvandervliet/routes.git',
    python_requires=">=3.8",
    install_requires=install_requires,
    extras_require={
        "test": tests_require,
        "dev": dev_requires,
    },
    test_suite="pytest",
    classifiers=[
        # Optional
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',
        # Indicate who your project is intended for
        'Intended Audience :: Data Engineers',
        'Topic :: route data',
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate you support Python 3. These classifiers are *not*
        # checked by 'pip install'. See instead 'python_requires' below.
        'Programming Language :: Python :: 3.8',
    ],

    py_modules=["jobs/routes"],
    entry_points={
        "console_scripts": ["run_route_job = jobs.routes:main"],
    },
    packages=find_packages(exclude=('tests', 'docs'))
)
