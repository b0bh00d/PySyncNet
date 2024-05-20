from setuptools import setup, Extension

setup(
    name="snmd5",
    version="1.0",
    description="MD5 calculation in C for speed",
    ext_modules=[Extension("snmd5", sources=["md5.c"])],
)
