# TLDR;

`arrowgen` is a python tool to generate C++ code which converts protobuf messages to Apache Arrow Table (and vice versa).

This is a work in progress

# How to use
 

 
# Process


1. Generate the python code from the `proto` files
2. Load the python code and uses the `DESCRIPTOR` to generate the code

 


# TODO

- [ ] Add support for nested data
- [ ] Stop poluting the source when testing
- [ ] Lint/format generated code
- [ ] Discover libraries dynamically when compiling in test
- [ ] Add documentation (type mapping)
- [ ] Add support for maps
- [ ] Improve docs
- [ ] Add CMake for tests
- [ ] Use TypeTraits
- [ ] Work column by column?