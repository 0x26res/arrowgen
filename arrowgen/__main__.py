import argparse

from arrowgen.generator import generate_for_file


def main():
    parser = argparse.ArgumentParser(
        description="Generate code to convert Google Protocol Buffers to Arrow Table"
    )
    parser.add_argument("proto_file", type=str, help="Input .proto file")
    args = parser.parse_args()
    header, source = generate_for_file(args.proto_file)
    print(header, source)


if __name__ == "__main__":
    main()
