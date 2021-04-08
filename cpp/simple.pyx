# distutils: language = c++
# distutils: sources = cmake-build-debug/simple.pb.cc
# distutils: sources = cmake-build-debug/simple.arrow.cc

from pyarrow.includes.libarrow cimport *

from pyarrow.lib cimport (check_status, pyarrow_wrap_table)



cdef extern from "cmake-build-debug/simple.arrow.h" namespace "messages" nogil:
    cdef cppclass CSearchRequestAppender "messages::SearchRequestAppender":
        CSearchRequestAppender() except +
        CStatus append(const char*, size_t )
        shared_ptr[CTable] build()



cdef class PySearchRequestAppender:
    cdef CSearchRequestAppender* thisptr
    def __cinit__(self):
        self.thisptr = new CSearchRequestAppender()
    def __dealloc__(self):
        del self.thisptr
    def append(self, data: bytes):
        check_status(self.thisptr.append(data, len(data)))
    cdef build(self):
        return pyarrow_wrap_table(self.thisptr.build())

