import zipfile
import io
import binascii
from elftools.elf.elffile import ELFFile
from elftools.common.exceptions import ELFError
from elftools.common.py3compat import byte2int

from cigam import Magic


class ELF():

    def __init__(self, file_path):
        if Magic(file_path).get_type() != 'elf':
            return

        self.elf_data = open(file_path, 'rb')
        self.elf_file = ELFFile(self.elf_data)

    def close(self):
        self.elf_data.close()

    def get_dynsym_datas(self, skip_import=True):
        dynsym_datas = []

        symbol_table = self.elf_file.get_section_by_name('.dynsym')
        for symbol in symbol_table.iter_symbols():
            if skip_import and symbol.entry.st_size == 0 or symbol.entry.st_info.type != 'STT_FUNC':
                continue

            self.elf_data.seek(0)
            symbol_addr = symbol.entry.st_value & 0xFFFE
            self.elf_data.seek(symbol_addr)
            symbol_hexs = ''

            size = symbol.entry.st_size
            if symbol.entry.st_size > 80:
                size = 80

            for x in self.elf_data.read(size):
                op = str(hex(x)).upper()[2:]
                if len(op) == 1:
                    op = '0' + op
                symbol_hexs = symbol_hexs + op

            dynsym_datas.append(
                (symbol.name, hex(symbol_addr), symbol_hexs))

        return dynsym_datas

    def get_rodata_strings(self):
        try:
            return display_string_dump(self.elf_file, '.rodata')
        except ELFError as ex:
            print('ELF error: %s\n' % ex)

    def display_string_dump(self, section_spec):
        """ Display a strings dump of a section. section_spec is either a
            section number or a name.
        """
        section = _section_from_spec(self.elf_file, section_spec)
        if section is None:
            print("Section '%s' does not exist in the file!" % section_spec)
            return None

        data = section.data()
        dataptr = 0

        strs = []
        while dataptr < len(data):
            while dataptr < len(data) and not 32 <= byte2int(data[dataptr]) <= 127:
                dataptr += 1

            if dataptr >= len(data):
                break

            endptr = dataptr
            while endptr < len(data) and byte2int(data[endptr]) != 0:
                endptr += 1

            strs.append(binascii.b2a_hex(
                data[dataptr:endptr]).decode().upper())
            dataptr = endptr

        return strs

    def _section_from_spec(self, spec):
        '''
            Retrieve a section given a "spec" (either number or name).
            Return None if no such section exists in the file.
        '''
        try:
            num = int(spec)
            if num < self.elf_file.num_sections():
                return self.elf_file.get_section(num)
            else:
                return None
        except ValueError:
            # Not a number. Must be a name then
            return self.elf_file.get_section_by_name(spec)


def get_elf_files(apk_path):
    files = list()
    if zipfile.is_zipfile(apk_path):
        try:
            with zipfile.ZipFile(apk_path, mode="r") as zf:
                for name in zf.namelist():
                    try:
                        data = zf.read(name)

                        mime = Magic(data).get_type()
                        if mime == 'elf':
                            elf_data = io.BytesIO(data)
                            elf_file = ELFFile(elf_data)
                            files.append((name, elf_data, elf_file))
                    except Exception as ex:
                        continue

        except Exception as ex:
            raise ex

    return files


def get_dynsym_datas(elf_data, elf_file, skip_import=True):
    """
    获取符号/方法的相关信息（符号名、符号数据）。
    """
    f = elf_data
    dynsym_datas = []

    symbol_table = elf_file.get_section_by_name('.dynsym')
    if symbol_table:
        for symbol in symbol_table.iter_symbols():
            if skip_import and symbol.entry.st_size == 0 or symbol.entry.st_info.type != 'STT_FUNC':
                continue

            f.seek(0)
            symbol_addr = symbol.entry.st_value & 0xFFFE
            f.seek(symbol_addr)
            symbol_hexs = ''

            size = symbol.entry.st_size
            if symbol.entry.st_size > 80:
                size = 80

            for x in f.read(size):
                op = str(hex(x)).upper()[2:]
                if len(op) == 1:
                    op = '0' + op
                symbol_hexs = symbol_hexs + op
            item = {}
            item["name"] = symbol.name
            item["data"] = symbol_hexs
            dynsym_datas.append(item)

    return dynsym_datas


def get_rodata_strings(elf_file):
    """
    获取字符串列表，以hex格式表示，避免字符编码问题。
    """
    try:
        return display_string_dump(elf_file, '.rodata')
    except ELFError as ex:
        import sys
        sys.stderr.write('ELF error: %s\n' % ex)
        sys.exit(1)


def display_string_dump(elf_file, section_spec):
    """ Display a strings dump of a section. section_spec is either a
        section number or a name.
    """
    section = _section_from_spec(elf_file, section_spec)
    if section is None:
        print("Section '%s' does not exist in the file!" % section_spec)
        return None

    data = section.data()
    dataptr = 0

    strs = []
    while dataptr < len(data):
        while (dataptr < len(data) and not (32 <= byte2int(data[dataptr]) <= 127)):
            dataptr += 1

        if dataptr >= len(data):
            break

        endptr = dataptr
        while endptr < len(data) and byte2int(data[endptr]) != 0:
            endptr += 1

        strs.append(binascii.b2a_hex(data[dataptr:endptr]).decode().upper())
        dataptr = endptr

    return strs


def _section_from_spec(elf_file, spec):
    '''
        Retrieve a section given a "spec" (either number or name).
        Return None if no such section exists in the file.
    '''
    if isinstance(spec, int):
        num = int(spec)
        if num < elf_file.num_sections():
            return elf_file.get_section(num)

    # Not a number. Must be a name then
    if isinstance(spec, str):
        try:
            return elf_file.get_section_by_name(spec)
        except AttributeError:
            return None
