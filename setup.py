from setuptools import setup


def readme():
    with open('README.md') as f:
        return f.read()

setup(name='mkgfd',
      version='0.1',
      description='Multimodal Knowledge Graph Functional Dependencies',
      author='WX Wilcke',
      author_email='w.x.wilcke@vu.nl',
      license='GPL3',
      install_requires=[
          "rdflib == 4.2.1"
      ],
      packages=['mkgfd'],
      include_package_data=True)
