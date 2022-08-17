import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='kft-dp',
    version='0.0.1',
    author='',
    author_email='',
    description='Data pipeline for KFT',
    long_description='',
    long_description_content_type="text/markdown",
    url='https://github.com/Muls/toolbox',
    project_urls = {
        "Bug Tracker": "https://github.com/Muls/toolbox/issues"
    },
    license='MIT',
    packages=['kft-dp'],
    install_requires=['s3fs','boto3','pandas'],
)