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
- XXXAppender: Build table incrementally by appending messages.
- XXXReader: Read messages from table, keeping track of the current position

# Process

## Code Generation Process

Arrowgen works in 3 phases 
1. Generate the python code from the `proto` files
2. Load the python code and uses the `DESCRIPTOR` to generate the code for the appender and reader
3. Lint the code with `clang-format`
 

## Test Process

1. Run the code generation process for sample data messages
2. Generate json test data
3. Compile a test example with the generated code
4. Test the code: load test json data as proto messages, convert to table and back
5. Test again with several record batches this time 

# Implementation notes

## Type Mapping

|Protoubf|C++|Arrow|Array               |Builder             |Note                    |
|--------|---|-----|--------------------|--------------------|------------------------|
|double  |double|float64|DoubleArray         |DoubleBuilder       |                        |
|float   |float|float32|FloatArray          |FloatBuilder        |                        |
|int64   |int64_t|int64|Int64Array          |Int64Builder        |                        |
|uint64  |uint64_t|uint64|UInt64Array         |UInt64Builder       |                        |
|int32   |int32_t|int32|Int32Array          |Int32Buidler        |                        |
|fixed64 |uint64_t|uint64|UInt64Array         |UInt64Builder       |                        |
|fixed32 |uint32_t|uint32|Int32Array          |Int32Builder        |                        |
|bool    |bool|boolean|BooleanArray        |BooleanBuilder      |                        |
|string  |std::string|utf8 |StringArray         |StringBuilder       |                        |
|group   |   |     |                    |                    |Deprecated              |
|message |class (generated code)|StructType|StructArray         |StructBuilder       |                        |
|bytes   |std::string|binary|BinaryArray         |BinaryBuilder       |                        |
|uint32  |uint32_t|uint32|UInt32Array         |UInt32Builder       |                        |
|enum    |enum (generated code)|dictionary|Int32Builder        |Int32Builder        |Use the enum int value  |
|sfixed32|int32_t|int32|Int32Array          |Int32Builder        |                        |
|sfixed64|int64_t|int64|Int64Array          |Int64Builder        |                        |
|sint32  |int32_t|int32|Int32Array          |Int32Builder        |                        |
|sint64  |int64_t|int64|Int64Array          |Int64Builder        |                        |
|any     |   |     |                    |                    |WIP                     |
|oneof   |   |dense union|                    |                    |Using one array per type|
|maps    |   |     |                    |                    |WIP                     |
|repeated|   |ListType|ListArray           |ListBuilder         |                        |

# TODO

- [ ] Add documentation (type mapping)
- [ ] Add support for maps
- [x] Add support for one of
- [ ] Improve docs (add simple example)
- [ ] Use TypeTraits for tests
- [ ] Work column by column?
- [ ] Add classifiers
- [ ] try to publish to pypi for real
- [x] add struct array reader
- [ ] consistent case in functions and members
- [ ] rationalize wrappers and code generation, better naming for list
- [ ] go through code TODOs
- [ ] Be consistent when using shared ptr
- [ ] Try to get rid of the Struct reader
- [ ] add messages with various level of nesting
- [ ] add support for imports from other files
- [x] stop using CPP_TYPE, use TYPE instead
- [ ] generate cython wrapper
- [ ] better data generation for one of
- [ ] use union as schema for one of  
- [ ] add test for repeated one of
- [ ] Merge struct reader with reader
- [x] check chunk can be misaligned
- [ ] merge type mapping (array, arraybuilder, type etc)
- [ ] generate type mapping automatically
- [ ] Consider using union array / union array builder
- [x] add python code to convert from arrow to protobuf 
- [ ] add python code to convert from protobuf to pandas
- [ ] add python code to convert from pandas to protobuf