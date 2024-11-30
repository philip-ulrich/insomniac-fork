import argparse
from cigam import Magic



def main(args):
    print(Magic(args.p).get_type())


if __name__ == '__main__':
    __VERSION__ = '0.0.3'

    parser = argparse.ArgumentParser(prog='cigam', description=None)
    parser.add_argument('p', help='path')

    parser.add_argument('-V', '--version', action='version',
                        version=__VERSION__)

    args = parser.parse_args()
    main(args)
