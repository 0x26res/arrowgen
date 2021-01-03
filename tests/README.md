# Tests

Tests use a sample proto file to:
* Generate C++ and python code for the messages
* Generate the arrow appender and reader code
* Compile the reader code and run test on it

## Tips

```bash
protoc --proto_path=./ ./messages/simple.proto  --cpp_out=./ --python_out=./
```

```
g++ -I./ simple_tester.cc  messages/simple.appender.cc messages/simple.pb.cc   /usr/lib/x86_64-linux-gnu/libarrow.so.200 /usr/lib/x86_64-linux-gnu/libprotobuf.so
```

List all libraries:
```
dpkg --get-selections | grep -v deinstall | less
```
Find library components:
```
dpkg -L libarrow200:amd64 | less
```

