from setuptools import setup, find_packages

setup(
    name='agti',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'numpy',
        'pandas',
        'sqlalchemy',
        'cryptography',
        'xrpl-py',
        'requests',
        'toml',
        'nest_asyncio','brotli','sec-cik-mapper','psycopg2-binary','quandl','schedule','openai','lxml',
        'gspread_dataframe','gspread','oauth2client',
        'selenium','selenium-wire>=5.1.0<6','boto3','blinker==1.7',
        'ua_generator',
    ],
    author='Alex Good',
    author_email='alex@agti.net',
    description='Post Fiat Finance focused node',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/postfiatorg/pftpyclient',  # Replace with your actual GitHub repo URL
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.11',
)
