import re

IDMS_RECORD_NAME_REGEX = re.compile(r'RECORD\s+NAME\.*\s+(?P<name>[a-zA-Z\d-]+)')
IDMS_ELEM_ITEM_REGEX = re.compile(
    r'^(?P<lvl>\d{2})\s+(?P<name>[a-zA-Z\d-]+)\s+[a-zA-Z\d-]+\s+\'?(?P<def_val>[a-zA-Z\d-]+)?\'?\s+(?P<type>[a-zA-Z\d\(\)]+)\s+\d+\s+\d+$')
IDMS_ITEM_REGEX = re.compile(
    r'^(?P<lvl>\d{2})\s+(?P<name>[a-zA-Z\d-]+)\s+[a-zA-Z\d-]+\'?(?:\s+)(?P<def_val>[a-zA-Z\d-]+)?\'?(?:\s+)(?P<type>[a-zA-Z\d\(\)]+)?\s+\d+\s+\d+$')
IDMS_NAME_SPLIT_REGEX = re.compile(r'-|:')
IDMS_STD_PIC_W_LEN_REGEX = re.compile(r'^(?P<type>[AX9])\((?P<len>\d+)\)$')
IDMS_SIGNED_INT_PIC_W_LEN_REGEX = re.compile(r'^S9\((?P<len>\d+)\)$')
IDMS_DECIMAL_PIC_W_LEN_REGEX = re.compile(r'^S?9\((?P<len_1>\d+)\)V9\((?P<len_2>\d+)\)$')
IDMS_DECIMAL_PIC_W_FIRST_LEN_REGEX = re.compile(r'^S?9\((?P<len_1>\d+)\)V(?P<len_2>9+)$')
IDMS_SET_HEADER_REGEX = re.compile(
    r'SET\.+\s+(?P<name>[a-zA-Z\d-]+)\s+MODE\s+(?P<mode>CHAIN|INDEX)'
)
IDMS_SET_OWNER_REGEX = re.compile(r'OWNER\.+\s+(?P<name>[a-zA-Z\d-]+)')
IDMS_SET_MEMBER_REGEX = re.compile(
    r'MEMBER\.+\s+(?P<table>[a-zA-Z\d-]+)\s+.+\n.+SORT\s+KEY\s+(?P<key>[a-zA-Z\d-]+)\s+(?P<order>ASC|DESC)')
IDMS_SET_MEMBER_KEY_REGEX = re.compile(f'(?P<key>[a-zA-Z\d-]+)\s+(?P<order>ASC|DESC)')
