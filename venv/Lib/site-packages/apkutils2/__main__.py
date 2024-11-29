import argparse
import binascii

from apkutils2 import __VERSION__, APK


def main(args):
    apk = APK(args.p)

    if args.m:
        import json
        if apk.get_manifest():
            print(json.dumps(apk.get_manifest(), indent=1))
        elif apk.get_org_manifest():
            print(apk.get_org_manifest())

    elif args.s:
        for item in apk.get_strings():
            print(binascii.unhexlify(item).decode(errors='ignore'))

    elif args.f:
        for item in apk.get_files():
            print(item)

    elif args.c:
        for item in apk.get_certs():
            print(item)


if __name__ == '__main__':

    _parser = argparse.ArgumentParser(prog='apkutils2', description=None)
    _parser.add_argument('p', help='path')
    _parser.add_argument('-m', action='store_true',
                         help='Show manifest', required=False)
    _parser.add_argument('-s', action='store_true',
                         help='Show strings', required=False)
    _parser.add_argument('-f', action='store_true',
                         help='Show files', required=False)
    _parser.add_argument('-c', action='store_true',
                         help='Show certs', required=False)
    _parser.add_argument('-V', '--version', action='version',
                         version=__VERSION__)

    _args = _parser.parse_args()
    main(_args)
