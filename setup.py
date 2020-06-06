from setuptools import setup, find_packages

setup(name='etl_cli',
      version='1.0.0',
      description='etl_cli',
      long_description='ETL command line utility',
      classifiers=[
        "Programming Language :: Python",
        "Framework :: Pyramid",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        ],
      author='vahan',
      author_email='aivosha@gmail.com',
      url='',
      keywords='web pyramid pylons',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      test_suite="etl_cli",
      entry_points="""\
      [paste.app_factory]
      [console_scripts]
        etl.etl = etl_cli.etl:run
      """,
      )
