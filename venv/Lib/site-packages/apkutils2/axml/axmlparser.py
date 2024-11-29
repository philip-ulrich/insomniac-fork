from struct import pack, unpack
from xml.dom import minidom
from xml.sax.saxutils import escape

from apkutils2.axml import public
from apkutils2.axml.chunk import BuffHandle, StringPoolChunk

# AXML FORMAT #
# Translated from
# http://code.google.com/p/android4me/source/browse/src/android/content/res/AXmlResourceParser.java

UTF8_FLAG = 0x00000100

CHUNK_STRINGPOOL_TYPE = 0x001C0001
CHUNK_NULL_TYPE = 0x00000000

ATTRIBUTE_IX_NAMESPACE_URI = 0
ATTRIBUTE_IX_NAME = 1
ATTRIBUTE_IX_VALUE_STRING = 2
ATTRIBUTE_IX_VALUE_TYPE = 3
ATTRIBUTE_IX_VALUE_DATA = 4
ATTRIBUTE_LENGHT = 5

CHUNK_AXML_FILE = {0x00080003, 0x00080009}
MAGIC_NUMBER = 0x00080003
MAGIC_NUMBER_MIN = 0x00080000
MAGIC_NUMBER_MAX = 0x00080009
CHUNK_RESOURCEIDS = 0x00080180
CHUNK_XML_FIRST = 0x00100100
CHUNK_XML_START_NAMESPACE = 0x00100100
CHUNK_XML_END_NAMESPACE = 0x00100101
CHUNK_XML_START_TAG = 0x00100102
CHUNK_XML_END_TAG = 0x00100103
CHUNK_XML_TEXT = 0x00100104
CHUNK_XML_LAST = 0x00100104

START_DOCUMENT = 0
END_DOCUMENT = 1
START_TAG = 2
END_TAG = 3
TEXT = 4


class AXMLParser(object):

    def __init__(self, raw_buff):
        self.reset()
        self.file_size = 0

        self.valid_axml = True
        self.buff = BuffHandle(raw_buff)

        magic_number = unpack('<L', self.buff.read(4))[0]

        if magic_number == MAGIC_NUMBER:
            self.file_size = unpack('<L', self.buff.read(4))[0]
            self.sb = StringPoolChunk(self.buff)

            self.m_resourceIDs = []
            self.m_prefixuri = {}
            self.m_uriprefix = {}
            self.m_prefixuriL = []

            self.visited_ns = []
        elif magic_number >= MAGIC_NUMBER_MIN and magic_number <= MAGIC_NUMBER_MAX:
            self.file_size = unpack('<L', self.buff.read(4))[0]
            self.sb = StringPoolChunk(self.buff)

            self.m_resourceIDs = []
            self.m_prefixuri = {}
            self.m_uriprefix = {}
            self.m_prefixuriL = []

            self.visited_ns = []
        else:
            self.valid_axml = False
            raise Exception("It's a invalid xml file.")

    def is_valid(self):
        return self.valid_axml

    def reset(self):
        self.m_event = -1
        self.m_lineNumber = -1
        self.m_name = -1
        self.m_namespaceUri = -1
        self.m_attributes = []
        self.m_idAttribute = -1
        self.m_classAttribute = -1
        self.m_styleAttribute = -1

    def __next__(self):
        self.do_next()
        return self.m_event

    def do_next(self):
        if self.m_event == END_DOCUMENT:
            return

        event = self.m_event

        self.reset()
        while True:
            chunkType = -1

            # Fake END_DOCUMENT event.
            if event == END_TAG:
                pass

            # START_DOCUMENT
            if event == START_DOCUMENT:
                chunkType = CHUNK_XML_START_TAG
            else:
                if self.buff.end():
                    self.m_event = END_DOCUMENT
                    break
                # --- FIXME 这里不一定是4个
                # 这里出里问题，导致死循环
                data4 = self.buff.read(4)
                if data4:
                    chunkType = unpack('<L', data4)[0]
                else:
                    pass

            if chunkType == CHUNK_RESOURCEIDS:
                chunkSize = unpack('<L', self.buff.read(4))[0]
                # FIXME
                if chunkSize < 8 or chunkSize % 4 != 0:
                    break

                for i in range(0, int(chunkSize / 4) - 2):
                    self.m_resourceIDs.append(
                        unpack('<L', self.buff.read(4))[0])

                continue

            # FIXME
            if chunkType < CHUNK_XML_FIRST or chunkType > CHUNK_XML_LAST:
                break

            # Fake START_DOCUMENT event.
            if chunkType == CHUNK_XML_START_TAG and event == -1:
                self.m_event = START_DOCUMENT
                break

            self.buff.read(4)  # /*chunkSize*/
            lineNumber = unpack('<L', self.buff.read(4))[0]
            self.buff.read(4)  # 0xFFFFFFFF

            if chunkType == CHUNK_XML_START_NAMESPACE or chunkType == CHUNK_XML_END_NAMESPACE:
                if chunkType == CHUNK_XML_START_NAMESPACE:
                    prefix = unpack('<L', self.buff.read(4))[0]
                    uri = unpack('<L', self.buff.read(4))[0]

                    self.m_prefixuri[prefix] = uri
                    self.m_uriprefix[uri] = prefix
                    self.m_prefixuriL.append((prefix, uri))
                    self.ns = uri
                else:
                    self.ns = -1
                    self.buff.read(4)
                    self.buff.read(4)
                    (prefix, uri) = self.m_prefixuriL.pop()

                continue

            self.m_lineNumber = lineNumber

            if chunkType == CHUNK_XML_START_TAG:
                self.m_namespaceUri = unpack('<L', self.buff.read(4))[0]
                self.m_name = unpack('<L', self.buff.read(4))[0]

                # FIXME
                self.buff.read(4)  # flags

                attributeCount = unpack('<L', self.buff.read(4))[0]
                self.m_idAttribute = (attributeCount >> 16) - 1
                attributeCount = attributeCount & 0xFFFF
                self.m_classAttribute = unpack('<L', self.buff.read(4))[0]
                self.m_styleAttribute = (self.m_classAttribute >> 16) - 1

                self.m_classAttribute = (self.m_classAttribute & 0xFFFF) - 1

                for i in range(0, attributeCount * ATTRIBUTE_LENGHT):
                    self.m_attributes.append(
                        unpack('<L', self.buff.read(4))[0])

                for i in range(ATTRIBUTE_IX_VALUE_TYPE, len(self.m_attributes),
                               ATTRIBUTE_LENGHT):
                    self.m_attributes[i] = self.m_attributes[i] >> 24

                self.m_event = START_TAG
                break

            if chunkType == CHUNK_XML_END_TAG:
                self.m_namespaceUri = unpack('<L', self.buff.read(4))[0]
                self.m_name = unpack('<L', self.buff.read(4))[0]
                self.m_event = END_TAG
                break

            if chunkType == CHUNK_XML_TEXT:
                self.m_name = unpack('<L', self.buff.read(4))[0]

                # FIXME
                self.buff.read(4)
                self.buff.read(4)

                self.m_event = TEXT
                break

    def get_prefix_by_uri(self, uri):
        try:
            return self.m_uriprefix[uri]
        except KeyError:
            return -1

    def get_prefix(self):
        try:
            return self.sb.getString(self.m_uriprefix[self.m_namespaceUri])
        except KeyError:
            return ''

    def get_name(self):
        if self.m_name == -1 or (self.m_event != START_TAG and
                                 self.m_event != END_TAG):
            return ''

        return self.sb.getString(self.m_name)

    def get_text(self):
        if self.m_name == -1 or self.m_event != TEXT:
            return ''

        return self.sb.getString(self.m_name)

    def get_namespace_prefix(self, pos):
        prefix = self.m_prefixuriL[pos][0]
        return self.sb.getString(prefix)

    def get_namespace_uri(self, pos):
        uri = self.m_prefixuriL[pos][1]
        return self.sb.getString(uri)

    def get_xmlns(self):
        buff = ""
        for i in self.m_uriprefix:
            if i not in self.visited_ns:
                buff += "xmlns:%s=\"%s\"\n" % (
                    self.sb.getString(self.m_uriprefix[i]),
                    self.sb.getString(self.m_prefixuri[self.m_uriprefix[i]]))
                self.visited_ns.append(i)
        return buff

    def get_attribute_offset(self, index):
        # FIXME
        if self.m_event != START_TAG:
            print("Current event is not START_TAG.")

        offset = index * 5
        # FIXME
        if offset >= len(self.m_attributes):
            print("Invalid attribute index")

        return offset

    def get_attribute_count(self):
        if self.m_event != START_TAG:
            return -1

        return len(self.m_attributes) / ATTRIBUTE_LENGHT

    def get_attribute_prefix(self, index):
        offset = self.get_attribute_offset(index)
        uri = self.m_attributes[offset + ATTRIBUTE_IX_NAMESPACE_URI]

        prefix = self.get_prefix_by_uri(uri)

        if prefix == -1:
            return ""

        return self.sb.getString(prefix)

    def get_attribute_name(self, index):
        offset = self.get_attribute_offset(index)
        name = self.m_attributes[offset + ATTRIBUTE_IX_NAME]

        if name == -1:
            return ""

        res = self.sb.getString(name)
        if not res:
            attr = self.m_resourceIDs[name]
            if attr in public.SYSTEM_RESOURCES['attributes']['inverse']:
                res = 'android:' + public.SYSTEM_RESOURCES['attributes']['inverse'][
                    attr
                ]

        return res

    def get_attribute_valueType(self, index):
        offset = self.get_attribute_offset(index)
        return self.m_attributes[offset + ATTRIBUTE_IX_VALUE_TYPE]

    def get_attribute_value_data(self, index):
        offset = self.get_attribute_offset(index)
        return self.m_attributes[offset + ATTRIBUTE_IX_VALUE_DATA]

    def get_attribute_value(self, index):
        offset = self.get_attribute_offset(index)
        valueType = self.m_attributes[offset + ATTRIBUTE_IX_VALUE_TYPE]
        if valueType == TYPE_STRING:
            valueString = self.m_attributes[offset + ATTRIBUTE_IX_VALUE_STRING]
            return self.sb.getString(valueString)
        # WIP
        return ""


START_DOCUMENT = 0
END_DOCUMENT = 1
START_TAG = 2
END_TAG = 3
TEXT = 4

TYPE_ATTRIBUTE = 2
TYPE_DIMENSION = 5
TYPE_FIRST_COLOR_INT = 28
TYPE_FIRST_INT = 16
TYPE_FLOAT = 4
TYPE_FRACTION = 6
TYPE_INT_BOOLEAN = 18
TYPE_INT_COLOR_ARGB4 = 30
TYPE_INT_COLOR_ARGB8 = 28
TYPE_INT_COLOR_RGB4 = 31
TYPE_INT_COLOR_RGB8 = 29
TYPE_INT_DEC = 16
TYPE_INT_HEX = 17
TYPE_LAST_COLOR_INT = 31
TYPE_LAST_INT = 31
TYPE_NULL = 0
TYPE_REFERENCE = 1
TYPE_STRING = 3

RADIX_MULTS = [0.00390625, 3.051758E-005, 1.192093E-007, 4.656613E-010]
DIMENSION_UNITS = ["px", "dip", "sp", "pt", "in", "mm", "", ""]
FRACTION_UNITS = ["%", "%p", "", "", "", "", "", ""]

COMPLEX_UNIT_MASK = 15


class AXML:

    def __init__(self, raw_buff):
        self.parser = AXMLParser(raw_buff)
        self.xmlns = False
        # 存放解析后的XML
        self.buff = ''

        self.is_valid = True
        if self.parser.is_valid():
            self.parse()
        else:
            self.is_valid = False

    def parse(self):
        tag = "notag"
        while True:
            _type = next(self.parser)
            # 异常类型直接退出
            if _type == -1:
                break

            if "</manifest>" in self.buff:
                break

            if _type == START_DOCUMENT:
                self.buff += '''<?xml version="1.0" encoding="utf-8"?>\n'''
            elif _type == START_TAG:
                prefix = self.get_prefix(
                    self.parser.get_prefix()) + self.parser.get_name()

                if len(prefix) == 0:
                    tag = "notag"

                self.buff += '<' + prefix + '\n'
                self.buff += self.parser.get_xmlns()

                # tag = prefix
                for i in range(0, int(self.parser.get_attribute_count())):

                    self.buff += "%s%s=\"%s\"\n" % (
                        self.get_prefix(self.parser.get_attribute_prefix(i)),
                        self.parser.get_attribute_name(i),
                        self._escape(self.get_attribute_value(i)))

                self.buff += '>\n'

            elif _type == END_TAG:
                prefix = self.get_prefix(
                    self.parser.get_prefix()) + self.parser.get_name()
                if len(prefix) == 0:
                    prefix = "notag"
                self.buff += "</%s>\n" % (prefix)

            elif _type == TEXT:
                self.buff += "%s\n" % self.parser.get_text()

            elif _type == END_DOCUMENT:
                break

    # pleed patch
    def _escape(self, s):
        s = s.replace("&", "&amp;")
        s = s.replace('"', "&quot;")
        s = s.replace("'", "&apos;")
        s = s.replace("<", "&lt;")
        s = s.replace(">", "&gt;")
        return escape(s)

    def get_buff(self):
        return self.buff

    def get_xml(self):
        return self.buff.replace('\0', '')

    def _format_xml(self):
        tmp = minidom.parseString(self.get_buff()).toprettyxml()
        A = str(tmp).replace('\t', '').replace('\n', '')
        return minidom.parseString(A).toprettyxml()

    def get_xml_obj(self):
        return minidom.parseString(self.get_buff())

    def get_prefix(self, prefix):
        """
        处理没有前缀的情况

        有一部分异常的节点，需要特殊处理。
        """
        if prefix is None or len(prefix) == 0:
            return ''

        return prefix + ':'

    def get_attribute_value(self, index):
        _type = self.parser.get_attribute_valueType(index)
        _data = self.parser.get_attribute_value_data(index)

        if _type == TYPE_STRING:
            return self.parser.get_attribute_value(index)

        elif _type == TYPE_ATTRIBUTE:
            return "?%s%08X" % (self.get_package(_data), _data)

        elif _type == TYPE_REFERENCE:
            return "@%s%08X" % (self.get_package(_data), _data)

        elif _type == TYPE_FLOAT:
            return "%f" % unpack("=f", pack("=L", _data))[0]

        elif _type == TYPE_INT_HEX:
            return "0x%08X" % _data

        elif _type == TYPE_INT_BOOLEAN:
            if _data == 0:
                return "false"
            return "true"

        elif _type == TYPE_DIMENSION:
            return "%f%s" % (self.complexToFloat(_data), DIMENSION_UNITS[_data & COMPLEX_UNIT_MASK])

        elif _type == TYPE_FRACTION:
            return "%f%s" % (self.complexToFloat(_data), FRACTION_UNITS[_data & COMPLEX_UNIT_MASK])

        elif _type >= TYPE_FIRST_COLOR_INT and _type <= TYPE_LAST_COLOR_INT:
            return "#%08X" % _data

        elif _type >= TYPE_FIRST_INT and _type <= TYPE_LAST_INT:
            return "%d" % int(_data)

        return "<0x%X, type 0x%02X>" % (_data, _type)

    def complexToFloat(self, xcomplex):
        return (float)(xcomplex & 0xFFFFFF00) * RADIX_MULTS[(xcomplex >> 4) & 3]

    def get_package(self, id):
        if id >> 24 == 1:
            return "android:"
        return ""

    def get_content(self):
        return self.content
