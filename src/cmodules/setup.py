from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize
import numpy

# To compile: python setup.py build_ext --inplace

ext_modules = [
    Extension(
        "funcx",
        ["auxfunc.pyx"],
        extra_compile_args=['-fopenmp'],
        extra_link_args=['-fopenmp'],
        include_dirs=[numpy.get_include()],
    )
]

setup(
    name='funcx',
    ext_modules=cythonize(ext_modules),
    include_dirs=[numpy.get_include()]
)
