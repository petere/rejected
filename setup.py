from setuptools import setup
from rejected import __version__

desc = 'Rejected is a Python RabbitMQ Consumer Framework and Controller Daemon'
long_description = """Rejected is a RabbitMQ consumer framwork and controller
daemon that allows you to focus on the development of the code that handles
the messages and not the code that facilitates the communication with
RabbitMQ."""


setup(name='rejected',
      version=__version__,
      description=desc,
      long_description=long_description,
      classifiers=['Development Status :: 4 - Beta',
                   'Intended Audience :: Developers',
                   'License :: OSI Approved :: BSD License'],
      keywords='amqp rabbitmq',
      author='Gavin M. Roy',
      author_email='gmr@meetme.com',
      url='http://github.com/gmr/rejected',
      license='BSD',
      packages=['rejected', 'rejected.mixins'],
      install_requires=['clihelper',
                        'pika>=0.9.13',
                        'pyyaml',
                        'simplejson',
                        'tornado==2.4.1'],
      extras_require={'HTML':  ['beautifulsoup4'],
                      'msgpack': ['msgpack'],
                      'redis': ['redis'],
                      'postgres': ['pgsql_wraper'],
                      'XML':  ['beautifulsoup4']},
      tests_require=['mock', 'nose', 'unittest2'],
      entry_points=dict(console_scripts=['rejected=rejected.controller:main']),
      zip_safe=True)
