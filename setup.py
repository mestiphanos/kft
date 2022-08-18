import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='kft_dp',
    version='0.0.1',
    author='',
    author_email='',
    description='Data pipeline for KFT',
    long_description='',
    long_description_content_type="text/markdown"
    url="http://gitlab.kifiya.et/yabi/kft-data-lakehouse",
    project_urls = {
        "Bug Tracker": "http://gitlab.kifiya.et/yabi/kft-data-lakehouse/issues"
    },
    license='MIT',
    packages=['kft_dp'],
    install_requires=['s3fs','boto3','pandas','fsspec'],
)