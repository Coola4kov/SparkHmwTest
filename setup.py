import setuptools
import os

setuptools.setup(
    name="SparkHmw9Kulakov",
    author="Mikhail Kulakov1",
    author_email="Mikhail_Kulakov1@epam.com",
    description="Re-usable functions to track "
                "changes using 'Actual Date' appraoch",
    tests_require=['pytest'],
    url="https://github.com/Coola4kov/SparkHmwTest",
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    packages=setuptools.find_packages(),
    install_requires=[],
    python_requires='>=3.7',
)
