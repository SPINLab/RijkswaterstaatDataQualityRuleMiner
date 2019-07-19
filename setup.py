from setuptools import setup


def readme():
    with open('README.md') as f:
        return f.read()

setup(name='mkgfd',
      version='0.1',
      description='Multimodal Knowledge Graph Functional Dependencies',
      author='WX Wilcke',
      author_email='w.x.wilcke@vu.nl',
      url='https://gitlab.com/wxwilcke/mkgfd',
      license='GPL3',
      install_requires=[
          "rdflib == 4.2.1",
          "numpy",
          "scipy",
          "scikit-learn"
      ],
      packages=['mkgfd'],
      include_package_data=True)
