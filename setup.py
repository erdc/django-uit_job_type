from setuptools import setup, find_packages


# -- Python Dependencies -- #
dependencies = [
    'pyuit',
]

setup(
    name='uit_plus_job',
    version='0.0.0',
    description='',
    long_description='',
    keywords='',
    author='',
    author_email='',
    url='',
    license='',
    packages=find_packages(exclude=['uit_plus_job/tests']),
    include_package_data=True,
    install_requires=dependencies,
)
