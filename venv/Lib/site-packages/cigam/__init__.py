import binascii


class Match(object):

    def __init__(self, data):
        self.data = data

    def is_apk(self):
        zip_magic = b'504b0304'
        if zip_magic not in binascii.hexlify(self.data[:4]):
            return None

        # AndroidManifest.xml
        manifest_bytes = b'416e64726f69644d616e69666573742e786d6c'
        contains_manifest = manifest_bytes in binascii.hexlify(self.data)

        # classes
        classes_bytes = b'636c6173736573'
        contains_classes = classes_bytes in binascii.hexlify(self.data)

        if contains_manifest and contains_classes:
            return ('.apk', 'apk', '', 'Android application package')
        elif contains_classes:
            return ('.apk', 'apk', '', 'Android application packag, without AndroidManifest.xml')
        elif contains_manifest:
            return ('.apk', 'apk', '', 'Android application packag, without classes.dex')
        else:
            return ('.zip', 'zip', 'application/zip', 'ZIP archive')

    def is_axml(self):
        magic = binascii.hexlify(self.data[:4])
        if b'03000800' == magic:
            return ('.xml', 'axml', '', 'Android binary XML')
        elif b'00000800' == magic:
            return ('.xml', 'axml', '', 'Android binary XML with modified')
        return None

    def is_arsc(self):
        if b'02000c00' == binascii.hexlify(self.data[:4]):
            return ('.arsc', 'arsc', '', 'Android resources')
        return None

    def is_elf(self):
        if b'7f454c46' == binascii.hexlify(self.data[:4]):
            return ('.so', 'elf', '', 'Executable and Linkable Format')
        return None

    def is_dex(self):
        if b'6465780a' == binascii.hexlify(self.data[:4]):
            return ('.dex', 'dex', '', 'Android Dalvik Executable format')
        return None

    def is_sqlite(self):
        if b'53514c69746520666f726d617420' == binascii.hexlify(self.data[:14]):
            return ('.db', 'sqlite', '', 'SQlite format')
        return None

    def is_odex(self):
        if b'6465790a' == binascii.hexlify(self.data[:4]):
            return ('.odex', 'odex', '', 'Android Optimized Dalvik EXecutable')
        return None

    def is_tar(self):
        if b'66697874' == binascii.hexlify(self.data[:4]):
            return ('.tar', 'tar', 'application/x-tar', 'Tape Archive (TAR)')
        return None

    def is_jpg(self):
        if b'ffd8ff' != binascii.hexlify(self.data[:3]):
            return None
        bytes = binascii.hexlify(self.data[6:10])
        if b'45786966' == bytes:
            return ('.jpg', 'jpg', 'image/jpeg, Exif', 'JPEG images')
        if b'4a464946' == bytes:
            return ('.jpg', 'jpg', 'image/jpeg, JFIF', 'JPEG images')
        return ('.jpg', 'jpg', 'image/jpeg', 'JPEG images')

    def is_png(self):
        if b'89504e47' == binascii.hexlify(self.data[:4]):
            return ('.png', 'png', 'image/png', '')
        return None

    def is_ico(self):
        if b'00000100' == binascii.hexlify(self.data[:4]):
            return ('.ico', 'icon', 'image/ico', 'MS Windows icon resource')
        return None

    def is_gif(self):
        if b'47494638' == binascii.hexlify(self.data[:4]):
            return ('.gif', 'gif', 'image/gif', 'Graphics Interchange Format (GIF)')
        return None

    def is_ogg(self):
        if b'4f676753' == binascii.hexlify(self.data[:4]):
            return ('.ogg', 'ogg', 'audio/ogg', 'OGG audio')
        return None

    def is_p7b(self):
        if b'3082' == binascii.hexlify(self.data[:2]):
            return ('.p7b', 'p7b', 'cert/p7b', 'PKCS7 File')
        return None

class Magic(object):

    def __init__(self, param):
        if isinstance(param, bytes):
            self.data = param
        else:
            with open(param, mode='rb') as f:
                self.data = f.read()

        self.extension = 'Unknown'
        self.type = 'Unknown'
        self.mime = 'Unknown'
        self.desc = 'Unknown'

        if self.istext(self.data):
            if '<?xml' in self.data[:5].decode(errors='ignore'):
                self.extension = '.xml'
                self.type = 'xml'
                self.mime = 'xml/plain'
                self.desc = 'XML document text'
                return

            self.extension = '.txt'
            self.type = 'txt'
            self.mime = 'text/plain'
            self.desc = 'textual file'
            return

        m = Match(self.data)
        for mtd in dir(m):
            if mtd.startswith('is_'):
                result = getattr(m, mtd)()
                if result:
                    self.extension, self.type, self.mime, self.desc = result
                    break

        if self.mime == 'Unknown':
            self.extension = '.data'
            self.type = 'data'

    def istext(self, data):
        try:
            data.decode()
            return True
        except UnicodeDecodeError:
            try:
                data.decode('gbk')
                return True
            except UnicodeDecodeError:
                pass
        return False

    def get_extension(self):
        return self.extension

    def get_type(self):
        return self.type

    def get_mime(self):
        return self.mime

    def get_desc(self):
        return self.desc
