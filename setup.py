from distutils.core import setup

setup(name='asynqp_simulation',
      version='0.1.0',
      author='Georg Wächter',
      author_email='georgwaechter@gmail.com',
      py_modules=['asynqp_simulation'],
      scripts=[],
      url='http://github.com/georgwaechter/asynqp_simulation',
      license='LICENSE',
      description='Simulation of amqp broker for easier testing of application',
      install_requires=[])