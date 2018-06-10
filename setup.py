from setuptools import setup, find_packages

import catcher_modules


def get_requirements() -> list:
    with open('requirements.txt', 'r') as f:
        return f.readlines()


setup(name=catcher_modules.APPNAME,
      version=catcher_modules.APPVSN,
      description='Additional modules for catcher.',
      author=catcher_modules.APPAUTHOR,
      author_email='valerii.tikhonov@gmail.com',
      url='https://github.com/comtihon/catcher_modules',
      packages=find_packages(),
      install_requires=get_requirements(),
      include_package_data=True,
      classifiers=[
          'Intended Audience :: Developers',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3.3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Topic :: Software Development :: Testing'
      ]
      )
