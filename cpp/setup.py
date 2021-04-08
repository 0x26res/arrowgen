from distutils.core import setup

import numpy
import pyarrow
from Cython.Distutils import build_ext
from setuptools import Extension

pyarrow.create_library_symlinks()

pyarrow.get_libraries()

extensions = [
    Extension(
        "simple",
        ["simple.pyx"] + ["cmake-build-debug/simple.pb.cc", "cmake-build-debug/simple.arrow.cc"],
        include_dirs=[numpy.get_include(), "./cmake-build-debug", "./"],

        extra_objects=[

            '/usr/lib/x86_64-linux-gnu/libprotobuf-lite.so',
            '/usr/lib/x86_64-linux-gnu/libprotobuf.so',
            #'/usr/lib/x86_64-linux-gnu/libarrow.so',
            '/home/arthur/source/arrowgen/venv/lib/python3.8/site-packages/pyarrow/libarrow.so',
            '/home/arthur/source/arrowgen/venv/lib/python3.8/site-packages/pyarrow/libarrow_python.so',
        ],
        #libraries=pyarrow.get_libraries() + [ 'protobuf', 'protobuf-lite'],
        #library_dirs= ['/usr/lib/x86_64-linux-gnu/'] + pyarrow.get_library_dirs() ,
        language="c++"),

]

setup(
    name="simple",
    cmdclass={"build_ext": build_ext},
    ext_modules=extensions
)
