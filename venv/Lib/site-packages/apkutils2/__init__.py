# coding: utf-8
import binascii
import re
import xml
import zipfile

import xmltodict
from apkutils2 import apkfile
from apkutils2.axml.arscparser import ARSCParser
from apkutils2.axml.axmlparser import AXML
from apkutils2.dex.dexparser import DexFile
from apkutils2.manifest import Manifest
from cigam import Magic

__VERSION__ = '0.5.3'


class APK:
    def __init__(self, apk_path):
        self.apk_path = apk_path
        self.dex_files = None
        self.children = None
        self.org_manifest = None
        self.strings = None
        self.org_strings = None
        self.opcodes = None
        self.certs = []
        self.arsc = None
        self.strings_refx = None
        self.app_icon = None
        self._manifest = None

    def get_app_icon(self):
        if self.app_icon:
            return self.app_icon
        self._init_app_icon()
        return self.app_icon

    def _init_app_icon(self):
        files = self.get_files()
        result = re.search(r':icon="@(.*?)"', self.get_org_manifest())
        ids = '0x' + result.groups()[0].lower()
        try:
            with apkfile.ZipFile(self.apk_path, 'r') as z:
                data = z.read('resources.arsc')
                self.arscobj = ARSCParser(data)
                self.package = self.arscobj.get_packages_names()[0]
                datas = xmltodict.parse(
                    self.arscobj.get_public_resources(self.package))
                for item in datas['resources']['public']:
                    if ids != item['@id']:
                        continue
                    for f in files:
                        name = f['name']
                        if item['@type'] in name and item['@name'] in name:
                            self.app_icon = name
        except Exception as ex:
            raise ex

    def _init_strings_refx(self):
        if not self.dex_files:
            self._init_dex_files()

        self.strings_refx = {}
        for dex_file in self.dex_files:
            for dexClass in dex_file.classes:
                try:
                    dexClass.parseData()
                except IndexError:
                    continue

                for method in dexClass.data.methods:
                    if not method.code:
                        continue

                    for bc in method.code.bytecode:
                        # 1A const-string
                        # 1B const-string-jumbo
                        if bc.opcode not in {26, 27}:
                            continue

                        clsname = method.id.cname.decode()
                        mtdname = method.id.name.decode()
                        dexstr = dex_file.string(bc.args[1])
                        if clsname in self.strings_refx:
                            if mtdname in self.strings_refx[clsname]:
                                self.strings_refx[clsname][mtdname].add(dexstr)
                            else:
                                self.strings_refx[clsname][mtdname] = set()
                                self.strings_refx[clsname][mtdname].add(dexstr)
                        else:
                            self.strings_refx[clsname] = {}
                            self.strings_refx[clsname][mtdname] = set()
                            self.strings_refx[clsname][mtdname].add(dexstr)

    def get_strings_refx(self):
        """获取字符串索引，即字符串被那些类、方法使用了。

        :return: 字符串索引
        :rtype: [dict]
        """
        if self.strings_refx is None:
            self._init_strings_refx()
        return self.strings_refx

    def get_dex_files(self):
        if not self.dex_files:
            self._init_dex_files()
        return self.dex_files

    def _init_dex_files(self):
        self.dex_files = []
        try:
            with apkfile.ZipFile(self.apk_path, 'r') as z:
                for name in z.namelist():
                    data = z.read(name)
                    if name.startswith('classes') and name.endswith('.dex') \
                            and Magic(data).get_type() == 'dex':
                        dex_file = DexFile(data)
                        self.dex_files.append(dex_file)
        except Exception as ex:
            raise ex

    def get_strings(self):
        if not self.strings:
            self._init_strings()
        return self.strings

    def get_org_strings(self):
        if not self.org_strings:
            self._init_strings()
        return self.org_strings

    def _init_strings(self):
        if not self.dex_files:
            self._init_dex_files()

        str_set = set()
        org_str_set = set()
        for dex_file in self.dex_files:
            for i in range(dex_file.string_ids.size):
                ostr = dex_file.string(i)
                org_str_set.add(ostr)
                str_set.add(binascii.hexlify(ostr).decode())

        self.strings = list(str_set)
        self.org_strings = list(org_str_set)

    def get_files(self):
        if not self.children:
            self._init_children()
        return self.children

    def _init_children(self):
        self.children = []
        try:
            with apkfile.ZipFile(self.apk_path, mode="r") as zf:
                for name in zf.namelist():
                    try:
                        data = zf.read(name)
                        mine = Magic(data).get_type()
                        info = zf.getinfo(name)
                    except Exception as ex:
                        print(name, ex)
                        continue
                    item = {}
                    item["name"] = name
                    item["type"] = mine
                    item["time"] = "%d%02d%02d%02d%02d%02d" % info.date_time
                    crc = str(hex(info.CRC)).upper()[2:]
                    crc = '0' * (8 - len(crc)) + crc
                    item["crc"] = crc
                    # item["sha1"] = ""
                    self.children.append(item)
        except Exception as e:
            raise e

    def get_org_manifest(self):
        if not self.org_manifest:
            self._init_manifest()
        return self.org_manifest

    @property
    def resources(self):
        with zipfile.ZipFile(self.apk_path, mode="r") as zf:
            data = zf.read('resources.arsc')
            return ARSCParser(data)

    def _init_org_manifest(self):
        ANDROID_MANIFEST = "AndroidManifest.xml"
        try:
            with apkfile.ZipFile(self.apk_path, mode="r") as zf:
                if ANDROID_MANIFEST in zf.namelist():
                    data = zf.read(ANDROID_MANIFEST)
                    try:
                        axml = AXML(data)
                        if axml.is_valid:
                            self.org_manifest = axml.get_xml()
                    except Exception as e:
                        raise e
        except Exception as e:
            raise e

    def get_manifest(self):
        if not self._manifest:
            self._init_manifest()
        return self._manifest

    @property
    def manifest(self):
        return Manifest(self.get_org_manifest())

    def _init_manifest(self):
        if not self.org_manifest:
            self._init_org_manifest()

        if self.org_manifest:
            try:
                self._manifest = xmltodict.parse(self.org_manifest,
                                                 False)['manifest']
            except xml.parsers.expat.ExpatError as e:
                pass
            except Exception as e:
                raise e

    def _init_arsc(self):
        ARSC_NAME = 'resources.arsc'
        try:
            with apkfile.ZipFile(self.apk_path, mode="r") as zf:
                if ARSC_NAME in zf.namelist():
                    data = zf.read(ARSC_NAME)
                    self.arsc = ARSCParser(data)
        except Exception as e:
            raise e

    def get_arsc(self):
        if not self.arsc:
            self._init_arsc()

        return self.arsc

    def get_certs(self):
        if not self.certs:
            self._init_certs()
        return self.certs

    def _init_certs(self):
        try:
            with apkfile.ZipFile(self.apk_path, mode="r") as zf:
                for name in zf.namelist():
                    if 'META-INF' in name:
                        data = zf.read(name)
                        mine = Magic(data).get_type()
                        if mine != 'txt':
                            from apkutils2.cert import Certificate
                            cert = Certificate(data)
                            self.certs = cert.get()
        except Exception as e:
            raise e

    def get_opcodes(self):
        if not self.dex_files:
            self._init_opcodes()
        return self.opcodes

    def _init_opcodes(self):
        if not self.dex_files:
            self._init_dex_files()

        self.opcodes = []
        for dex_file in self.dex_files:
            for dexClass in dex_file.classes:
                try:
                    dexClass.parseData()
                except IndexError:
                    continue

                for method in dexClass.data.methods:
                    opcodes = ""
                    if method.code:
                        for bc in method.code.bytecode:
                            opcode = str(hex(bc.opcode)).upper()[2:]
                            if len(opcode) == 2:
                                opcodes = opcodes + opcode
                            else:
                                opcodes = opcodes + "0" + opcode

                    proto = self.get_proto_string(method.id.return_type,
                                                  method.id.param_types)

                    item = {}
                    item['super_class'] = dexClass.super.decode()
                    item['class_name'] = method.id.cname.decode()
                    item['method_name'] = method.id.name.decode()
                    item['method_desc'] = method.id.desc.decode()
                    item['proto'] = proto
                    item['opcodes'] = opcodes
                    self.opcodes.append(item)

    @staticmethod
    def get_proto_string(return_type, param_types):
        proto = return_type.decode()
        if len(proto) > 1:
            proto = 'L'

        for item in param_types:
            param_type = item.decode()
            proto += 'L' if len(param_type) > 1 else param_type

        return proto
