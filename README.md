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

# Implementation notes

## OneOf

--For one of we use a dictionary column to store the name of the one of element (WIP)-- Use `pyarrow.union`


## Type Mapping

WIP

# TODO

- [ ] Add documentation (type mapping)
- [ ] Add support for maps
- [ ] Add support for one of
- [ ] Improve docs (add simple example)
- [ ] Use TypeTraits for tests
- [ ] Work column by column?
- [ ] Add classifiers
- [ ] try to publish to pypi for real
- [ ] add struct array reader
- [ ] consistent case in functions and members
- [ ] rationalize wrappers and code generation, better naming for list
- [ ] go through code TODOs
- [ ] Be consistent when using shared ptr
- [ ] Try to get rid of the Struct reader
- [ ] add messages with various level of nesting
- [ ] add support for imports from other files
- [ ] stop using CPP_TYPE, use TYPE instead