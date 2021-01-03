# TLDR;

`arrowgen` is a python tool to generate C++ code which converts Google Protocol Buffers messages to Apache Arrow Table (and vice versa).


# Usage

## Prepare the environment
```bash
git clone https://github.com/0x26res/arrowgen.git
cd arrowgen
pip install -r requirements.txt
```
## Run the Generator
```bash
python arrowgen/__main__.py <proto file>
```
This will output the location of the header and source file that have been generated

## Generated Code

The generated code consist of two classes per message:
- XXXAppender: Build table incrementally by adding messages to `ArrayBuilder`
- XXXReader: Read messages from table, keeping track of the current position

# Process

## General Process

1. Generate the python code from the `proto` files
2. Load the python code and uses the `DESCRIPTOR` to generate the code for the appender and reader
3. Lint the code with `clang-format`
 

## Test Process

1. Run the general process for sample data messages
2. Generate json test data
3. Compile a test example with the generated code
4. Test the code: load test json data as proto messages, convert to table and back
5. Test again with several record batches this time 

# TODO

- [ ] Handle missing values
- [ ] Add support for repeated nested data
- [ ] Stop poluting the source when testing
- [ ] Discover libraries dynamically when compiling in test
- [ ] Add CMake for tests ?
- [ ] Add documentation (type mapping)
- [ ] Add support for maps
- [ ] Improve docs
- [ ] Use TypeTraits
- [ ] Work column by column?
- [ ] Add classifiers
- [ ] Rename generated file to arrow (instead of appender)
- [ ] try to publish to pypi for real
- [ ] Make schema & field vector a static member
- [ ] use GetColumnByName
- [ ] add struct array reader
- [ ] consistent case in functions and members
- [ ] rationalize wrappers and code generation, better naming for list
- [ ] go through code TODOs
- [ ] Add proper C++ unit test