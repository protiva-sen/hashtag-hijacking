import argparse

parser = argparse.ArgumentParser(description="Download data for the project.")
parser.add_argument('--input', type=str, required=True, help='Input file path')
parser.add_argument('--output', type=str, required=True, help='Output file path')

if __name__ == "__main__":
    args = parser.parse_args()
    print(f"Input file: {args.input}")
    print(f"Output file: {args.output}")