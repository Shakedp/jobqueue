from distutils.core import setup

setup(name='jobqueue',
      version='1.0',
      description='Simple HTTP server and client for job queue',
      entry_points={
          'console_scripts': ["jobqueue=jobqueue.server:main"]
      },
      install_requires=[
          "requests",
          "flask"
      ]
      )
