import sys

from arrowgen.generator import generate_for_file


def main():
    generate_for_file(sys.argv[1])


if __name__ == "__main__":
    main()
